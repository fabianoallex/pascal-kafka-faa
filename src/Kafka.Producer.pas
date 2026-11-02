unit Kafka.Producer;

{$I kafka.inc}

{ The producer: Send queues a record and returns at once; a sender thread
  groups the records into batches per partition and sends each batch to its
  partition's leader, retrying what can be retried; each record's outcome
  goes to a callback and to a future.

  The shape is Java's KafkaProducer (docs/design.md, section 4, "Threads"),
  without idempotence (phase K3). The parameters keep Java's names and
  defaults (KafkaProducerParams): acks=all, linger.ms=5, batch.size=16384,
  buffer.memory=32 MB, delivery.timeout.ms=120000, max.request.size=1 MB,
  max.in.flight.requests.per.connection=5, retries=MAX_INT bounded by the
  delivery timeout, and from the cluster's parameters max.block.ms,
  retry.backoff.ms and request.timeout.ms.

  Send. Picks the partition (the record's own; murmur2 of the key; the sticky
  partition of Kafka.Partitioner for a record without a key), appends the
  record to the partition's last batch (a new one when it would grow past
  BatchSize), and returns. It blocks only for metadata, while a topic isn't
  known yet, and while the records waiting take more than BufferMemory, both
  up to MaxBlockMs (Java's max.block.ms). As in Java, a failure there doesn't
  raise: the record's future fails (EKafkaTimeout; EKafkaBrokerError with
  MESSAGE_TOO_LARGE for a record larger than MaxRequestSize or BufferMemory,
  the code a broker would answer; EKafkaBrokerError with
  UNKNOWN_TOPIC_OR_PARTITION for a partition the topic doesn't have), and
  the callback runs on the caller's thread. Send raises only for what is the
  caller's mistake: no topic, a negative partition, a producer not open or
  closing.

  The sender thread. A batch is ready when it is full (or another is behind
  it), when it has waited LingerMs, or while a Flush or Close is waiting. A
  pass takes the first ready batch of each partition, groups them by leader
  into Produce requests of up to MaxRequestSize, and sends them with up to
  MaxInFlightPerBroker requests waiting per broker. A partition has at most
  one batch in flight (Java mutes it so when it can't use idempotence to
  keep order): a retried batch goes back to the head of its partition's
  queue, so records are written in the order they were sent, retries
  included. Records may be written twice (a request that timed out may have
  been written): at-least-once until the idempotent producer of phase K3.

  Errors. A retriable error (KafkaErrorIsRetriable: a leader that moved, not
  enough replicas, a connection that broke, a request that timed out) sends
  the batch again after RetryBackoffMs, as long as it was sent at most
  Retries times and its records are younger than DeliveryTimeoutMs; an
  error that calls for fresh metadata (KafkaErrorIsInvalidMetadata) also
  invalidates the cluster's, which the sender refreshes before going on. Any
  other error, or no more time, fails the batch's records. A batch waiting
  longer than DeliveryTimeoutMs fails with EKafkaTimeout (Java's
  "Expiring N record(s)"). A batch the broker refuses as too large
  (MESSAGE_TOO_LARGE) fails; Java splits it and tries again, which isn't
  done here yet. A request is given RequestTimeoutMs; then it is abandoned
  (the connection stays open: Kafka.Connection's header) and retried.

  Delivery. Each record's report goes first to the callback given to Send,
  then to its future, in the order of the partition's records. Callbacks run
  on the sender thread (or the caller's, for a failure in Send), as Java's
  do: a slow callback holds up every send, and a callback must not call
  Flush or Close (they raise when called from the sender thread) nor Send
  when the buffer may be full (Send would wait for memory only the sender
  frees). An exception a callback raises is swallowed. Futures share one
  monitor (a hub, kept alive by the futures themselves) rather than an
  event each: a million records in flight would otherwise be a million
  kernel objects on Windows.

  Flush and Close. Flush makes every batch ready at once and waits for every
  record sent before it, up to a timeout; once it returns True, their
  callbacks have run. Close refuses new records, flushes up to its timeout,
  then fails what is left (EKafka: "closed before delivery"), stops the
  sender and closes the cluster. Every record has DeliveryTimeoutMs at most,
  so a Close without a timeout ends too: in DeliveryTimeoutMs plus a request
  timeout at worst. The destructor calls Close with no timeout. Don't free
  the producer while another thread is inside Send.

  Delphi x FPC: no anonymous methods (callbacks are "of object", with a Tag
  for the caller's context), no TTask; the hub is a pascal-common-faa
  TPcMonitor. }

interface

uses
  SysUtils,
  Classes,
  Generics.Collections,
  PascalCommon.ThreadPool,
  Kafka.Types,
  Kafka.Protocol.Primitives,
  Kafka.Protocol.RecordBatch,
  Kafka.Compression,
  Kafka.Connection,
  Kafka.Cluster,
  Kafka.Partitioner;

const
  /// TKafkaProducerRecord.Partition: let the producer choose.
  KAFKA_ANY_PARTITION = -1;
  /// No timeout, for Flush, Close and the futures' Wait.
  KAFKA_WAIT_INFINITE = PC_WAIT_INFINITE;

  KAFKA_DEFAULT_LINGER_MS = 5;
  KAFKA_DEFAULT_BATCH_SIZE = 16384;
  KAFKA_DEFAULT_BUFFER_MEMORY = 32 * 1024 * 1024;
  KAFKA_DEFAULT_DELIVERY_TIMEOUT_MS = 120000;
  KAFKA_DEFAULT_MAX_REQUEST_SIZE = 1024 * 1024;
  KAFKA_DEFAULT_MAX_IN_FLIGHT = 5;

type
  /// Java's acks: all (-1, the default), 1, 0. The zero of the enumeration
  /// is the default on purpose.
  TKafkaAcks = (kaAll, kaLeader, kaNone);

  { Start from KafkaProducerParams: it sets Java's defaults. A field at 0
    takes its default too, except LingerMs and Retries, where 0 means 0. }
  TKafkaProducerParams = record
    /// The bootstrap, TLS, client id, timeouts (MaxBlockMs, RetryBackoffMs,
    /// the connections' RequestTimeoutMs).
    Cluster: TKafkaClusterParams;
    Acks: TKafkaAcks;
    /// How long a batch that isn't full waits for more records.
    LingerMs: Cardinal;
    /// A batch's size, in records' bytes before compression.
    BatchSize: Integer;
    /// The bytes of records waiting (queued or in flight) before Send blocks.
    BufferMemory: Int64;
    Compression: TKafkaCompression;
    /// A record's time from Send to its outcome, retries included. At least
    /// LingerMs plus the request timeout.
    DeliveryTimeoutMs: Cardinal;
    /// A Produce request's size; a record larger than this is refused.
    MaxRequestSize: Integer;
    /// Produce requests waiting for an answer, per broker.
    MaxInFlightPerBroker: Integer;
    /// How many times a batch is sent again after a retriable error.
    Retries: Integer;
  end;

  TKafkaProducerRecord = record
    Topic: string;
    /// KAFKA_ANY_PARTITION: from the key (murmur2), or sticky without one.
    Partition: Int32;
    Key: TBytes;
    /// A null key (no key: sticky partition), not an empty one.
    KeyIsNull: Boolean;
    Value: TBytes;
    /// A null value: a tombstone for a compacted topic.
    ValueIsNull: Boolean;
    Headers: TKafkaRecordHeaderArray;
    /// Unix milliseconds; KAFKA_NO_TIMESTAMP (-1): when Send is called.
    Timestamp: Int64;
  end;

  { A record's outcome. }
  TKafkaDeliveryReport = record
    Topic: string;
    /// -1 when the record failed before a partition was chosen.
    Partition: Int32;
    /// -1 with acks=0 (the broker doesn't say) and when it failed.
    Offset: Int64;
    /// The record's timestamp; the broker's when the topic uses
    /// LogAppendTime.
    Timestamp: Int64;
    /// What Send was given, for the callback.
    Tag: NativeInt;
    /// nil when delivered.
    ErrorClass: EKafkaClass;
    /// The Kafka error code of an EKafkaBrokerError; 0 otherwise.
    ErrorCode: Int16;
    ErrorMessage: string;
    function Succeeded: Boolean;
    /// Raises the failure (EKafkaBrokerError with its code) when there is
    /// one.
    procedure RaiseIfFailed;
  end;

  TKafkaDeliveryEvent = procedure(const AReport: TKafkaDeliveryReport) of object;

  IKafkaDeliveryFuture = interface
    ['{9D4B7E21-5A3C-4F86-B0E2-6C1D8A9F3B47}']
    /// True once the record has its outcome; waits up to ATimeoutMs
    /// (KAFKA_WAIT_INFINITE: for ever).
    function Wait(ATimeoutMs: Cardinal): Boolean;
    function IsDone: Boolean;
    /// The outcome; EKafka when not done yet.
    function Report: TKafkaDeliveryReport;
    /// Wait, then the report when delivered; raises the failure, or
    /// EKafkaTimeout when the wait ran out first.
    function Get(ATimeoutMs: Cardinal = KAFKA_WAIT_INFINITE): TKafkaDeliveryReport;
  end;

  TKafkaProducer = class
  private type
    TSenderThread = class(TThread)
    private
      FOwner: TKafkaProducer;
    protected
      procedure Execute; override;
    public
      constructor Create(AOwner: TKafkaProducer);
    end;
    TState = (psCreated, psOpen, psClosing, psClosed);
  private
    FParams: TKafkaProducerParams;
    FRequestTimeoutMs: Cardinal;
    FMaxBlockMs: Cardinal;
    FRetryBackoffMs: Cardinal;
    FCluster: TKafkaCluster;
    FMonitor: TPcMonitor;
    FHub: IInterface;
    FQueues: TObjectDictionary<string, TObject>;
    FQueueList: TList<TObject>;
    FInFlight: TList<TObject>;
    /// Batches the sender took from their queue and hasn't put in flight,
    /// back in the queue or completed yet, so Flush sees them.
    FTransit: TList<TObject>;
    FBuffered: Int64;
    FFlushing: Integer;
    FSticky: TKafkaStickyPartitioner;
    FWake: TKafkaEventSignal;
    FWakeSignal: IKafkaSignal;
    FSender: TSenderThread;
    FSenderThreadId: TThreadID;
    FState: TState;
    FStopping: Boolean;
    FDrainIndex: Integer;
    FNodeBackoff: TDictionary<Int32, UInt64>;
    FNextMetadataAt: UInt64;
    FRequestsSent: Integer;
    procedure CheckNotSenderThread(const AWhat: string);
    function TopicFor(const AName: string): TKafkaTopicInfo;
    function Append(ADelivery: TObject; const ARecord: TKafkaProducerRecord;
      const ATopic: TKafkaTopicInfo; APartition: Int32; ASize: Integer;
      ADeadline: UInt64): Boolean;
    function WaitAll(ATimeoutMs: Cardinal): Boolean;
    // The sender thread.
    procedure RunSender;
    procedure HandleCompletions(ANow: UInt64; var ANext: UInt64);
    procedure HandleResponse(AFlight: TObject);
    procedure MaybeRefreshMetadata(ANow: UInt64);
    procedure ExpireBatches(ANow: UInt64; var ANext: UInt64);
    procedure SendReady(ANow: UInt64; var ANext: UInt64);
    procedure SendToNode(ANodeId: Int32; ABatches: TList<TObject>);
    procedure RetryOrFail(ABatch: TObject; AClass: EKafkaClass; ACode: Int16;
      const AMessage: string; ARetriable: Boolean);
    procedure Requeue(ABatch: TObject; ACountAttempt: Boolean);
    procedure CompleteBatch(ABatch: TObject; ABaseOffset, ALogAppendTime: Int64);
    procedure FailBatch(ABatch: TObject; AClass: EKafkaClass; ACode: Int16;
      const AMessage: string);
    procedure ReleaseBatch(ABatch: TObject);
    procedure AbortAll;
  public
    /// Checks the parameters (EArgumentException); no I/O.
    constructor Create(const AParams: TKafkaProducerParams);
    /// Close with no timeout, then frees.
    destructor Destroy; override;
    /// The cluster's bootstrap (EKafkaConnection when no address answers),
    /// then the sender thread. Once only.
    procedure Open;
    /// Flushes for up to ATimeoutMs, fails what is left, stops the sender
    /// and closes the cluster. Idempotent.
    procedure Close(ATimeoutMs: Cardinal = KAFKA_WAIT_INFINITE);

    /// Queues ARecord (the unit's header); the future of its outcome.
    function Send(const ARecord: TKafkaProducerRecord): IKafkaDeliveryFuture; overload;
    /// The same, with a callback that receives the outcome and ATag.
    function Send(const ARecord: TKafkaProducerRecord; AOnDelivery: TKafkaDeliveryEvent;
      ATag: NativeInt = 0): IKafkaDeliveryFuture; overload;
    /// Sends every waiting batch now and waits up to ATimeoutMs for every
    /// record sent before the call; True when all have their outcome.
    function Flush(ATimeoutMs: Cardinal = KAFKA_WAIT_INFINITE): Boolean;

    /// The topic's partitions, waiting up to MaxBlockMs for its metadata.
    function PartitionsFor(const ATopic: string): TKafkaPartitionInfoArray;
    /// Records sent and not done yet.
    function PendingCount: Integer;
    /// Produce requests written since Open (for tests and diagnostics).
    function RequestsSent: Integer;
    property Cluster: TKafkaCluster read FCluster;
    property Params: TKafkaProducerParams read FParams;
  end;

/// Java's defaults, with ABootstrapServers.
function KafkaProducerParams(const ABootstrapServers: string): TKafkaProducerParams;

/// A record with a null key (sticky partition) and AValue.
function KafkaProducerRecord(const ATopic: string; const AValue: TBytes): TKafkaProducerRecord; overload;
/// A record with AKey (never null, even when empty) and AValue.
function KafkaProducerRecord(const ATopic: string; const AKey, AValue: TBytes): TKafkaProducerRecord; overload;
/// A record with AKey and a null value (a tombstone).
function KafkaTombstone(const ATopic: string; const AKey: TBytes): TKafkaProducerRecord;

implementation

uses
  PascalCommon.Threading,
  PascalCommon.Tracing,
  Kafka.Errors,
  Kafka.Messages.Apis,
  Kafka.Messages.Produce;

const
  // What a record adds to a batch beyond its key, value and header bytes:
  // length, attributes, timestamp and offset deltas, the key's, value's and
  // header count's lengths, all varints at their largest.
  RECORD_OVERHEAD = 5 + 1 + 10 + 5 + 5 + 5 + 5;
  HEADER_OVERHEAD = 5 + 5;
  // A Produce request around one batch: the request header, acks, timeout,
  // the topic and partition arrays (topic name or id aside).
  REQUEST_OVERHEAD = 128;

type
  IDeliveryHub = interface
    ['{2F8A6C14-9B3E-4D71-A5C0-7E2B1D9F4A68}']
    function Monitor: TPcMonitor;
  end;

  TDeliveryHub = class(TInterfacedObject, IDeliveryHub)
  private
    FMonitor: TPcMonitor;
  public
    constructor Create;
    destructor Destroy; override;
    function Monitor: TPcMonitor;
  end;

  TDelivery = class(TInterfacedObject, IKafkaDeliveryFuture)
  private
    FHub: IDeliveryHub;
    FOnDelivery: TKafkaDeliveryEvent;
    FReport: TKafkaDeliveryReport;
    FDone: Boolean;
  public
    constructor Create(const AHub: IDeliveryHub; AOnDelivery: TKafkaDeliveryEvent; ATag: NativeInt);
    /// Runs the callback, then wakes the future's waiters.
    procedure Complete(const AReport: TKafkaDeliveryReport);
    function Wait(ATimeoutMs: Cardinal): Boolean;
    function IsDone: Boolean;
    function Report: TKafkaDeliveryReport;
    function Get(ATimeoutMs: Cardinal): TKafkaDeliveryReport;
  end;

  TEntry = record
    Delivery: TDelivery;
    /// Keeps the delivery alive while the batch has it.
    Hold: IKafkaDeliveryFuture;
  end;

  TProducerBatch = class
  public
    Topic: string;
    TopicId: TKafkaUuid;
    Partition: Int32;
    Batch: TKafkaRecordBatch;
    Entries: array of TEntry;
    Count: Integer;
    Bytes: Integer;
    CreatedAt: UInt64;
    RetryAt: UInt64;
    Attempts: Integer;
    Encoded: TBytes;
    /// Taken by the sender once: no record is appended after that.
    Closed: Boolean;
    procedure Add(ADelivery: TDelivery);
  end;

  TPartitionQueue = class
  public
    Topic: string;
    Partition: Int32;
    Batches: TList<TProducerBatch>;
    /// A batch of this partition is in flight.
    Muted: Boolean;
    constructor Create(const ATopic: string; APartition: Int32);
    destructor Destroy; override;
  end;

  TInFlight = class
  public
    NodeId: Int32;
    Broker: IKafkaBrokerConnection;
    Future: IKafkaResponseFuture;
    Version: Int16;
    Batches: array of TProducerBatch;
    SentAt: UInt64;
  end;

function KafkaProducerParams(const ABootstrapServers: string): TKafkaProducerParams;
begin
  Result := Default(TKafkaProducerParams);
  Result.Cluster := KafkaClusterParams(ABootstrapServers);
  Result.Acks := kaAll;
  Result.LingerMs := KAFKA_DEFAULT_LINGER_MS;
  Result.BatchSize := KAFKA_DEFAULT_BATCH_SIZE;
  Result.BufferMemory := KAFKA_DEFAULT_BUFFER_MEMORY;
  Result.Compression := kcNone;
  Result.DeliveryTimeoutMs := KAFKA_DEFAULT_DELIVERY_TIMEOUT_MS;
  Result.MaxRequestSize := KAFKA_DEFAULT_MAX_REQUEST_SIZE;
  Result.MaxInFlightPerBroker := KAFKA_DEFAULT_MAX_IN_FLIGHT;
  Result.Retries := High(Integer);
end;

function KafkaProducerRecord(const ATopic: string; const AValue: TBytes): TKafkaProducerRecord;
begin
  Result := Default(TKafkaProducerRecord);
  Result.Topic := ATopic;
  Result.Partition := KAFKA_ANY_PARTITION;
  Result.KeyIsNull := True;
  Result.Value := AValue;
  Result.Timestamp := KAFKA_NO_TIMESTAMP;
end;

function KafkaProducerRecord(const ATopic: string; const AKey, AValue: TBytes): TKafkaProducerRecord;
begin
  Result := KafkaProducerRecord(ATopic, AValue);
  Result.Key := AKey;
  Result.KeyIsNull := False;
end;

function KafkaTombstone(const ATopic: string; const AKey: TBytes): TKafkaProducerRecord;
begin
  Result := KafkaProducerRecord(ATopic, AKey, nil);
  Result.ValueIsNull := True;
end;

function NowUnixMs: Int64;
begin
  Result := PcUnixNanoOfLocal(Now) div 1000000;
end;

function AcksValue(AAcks: TKafkaAcks): Int16;
begin
  case AAcks of
    kaLeader: Result := 1;
    kaNone: Result := 0;
  else
    Result := -1;
  end;
end;

function RecordSize(const ARecord: TKafkaProducerRecord): Integer;
var
  I: Integer;
begin
  Result := RECORD_OVERHEAD + Length(ARecord.Key) + Length(ARecord.Value);
  // A UTF-16 code unit is at most 3 bytes of UTF-8.
  for I := 0 to High(ARecord.Headers) do
    Inc(Result, HEADER_OVERHEAD + 3 * Length(ARecord.Headers[I].Key) +
      Length(ARecord.Headers[I].Value));
end;

function Remaining(ADeadline: UInt64): Cardinal;
var
  LNow: UInt64;
begin
  LNow := PcTickMs;
  if LNow >= ADeadline then
    Result := 0
  else if ADeadline - LNow >= KAFKA_WAIT_INFINITE then
    Result := KAFKA_WAIT_INFINITE - 1
  else
    Result := Cardinal(ADeadline - LNow);
end;

function DeadlineOf(ATimeoutMs: Cardinal): UInt64;
begin
  if ATimeoutMs = KAFKA_WAIT_INFINITE then
    Result := High(UInt64)
  else
    Result := PcTickMs + ATimeoutMs;
end;

function QueueKey(const ATopic: string; APartition: Int32): string;
begin
  Result := ATopic + #0 + IntToStr(APartition);
end;

procedure MinTo(var ANext: UInt64; AValue: UInt64);
begin
  if AValue < ANext then
    ANext := AValue;
end;

function FailedReport(const ATopic: string; APartition: Int32; AClass: EKafkaClass;
  ACode: Int16; const AMessage: string): TKafkaDeliveryReport;
begin
  Result := Default(TKafkaDeliveryReport);
  Result.Topic := ATopic;
  Result.Partition := APartition;
  Result.Offset := -1;
  Result.ErrorClass := AClass;
  Result.ErrorCode := ACode;
  Result.ErrorMessage := AMessage;
end;

{ TKafkaDeliveryReport }

function TKafkaDeliveryReport.Succeeded: Boolean;
begin
  Result := ErrorClass = nil;
end;

procedure TKafkaDeliveryReport.RaiseIfFailed;
begin
  if ErrorClass = nil then
    Exit;
  if ErrorClass.InheritsFrom(EKafkaBrokerError) then
    raise EKafkaBrokerError.Create(ErrorCode, ErrorMessage);
  raise ErrorClass.Create(ErrorMessage);
end;

{ TDeliveryHub }

constructor TDeliveryHub.Create;
begin
  inherited Create;
  FMonitor := TPcMonitor.Create;
end;

destructor TDeliveryHub.Destroy;
begin
  FMonitor.Free;
  inherited;
end;

function TDeliveryHub.Monitor: TPcMonitor;
begin
  Result := FMonitor;
end;

{ TDelivery }

constructor TDelivery.Create(const AHub: IDeliveryHub; AOnDelivery: TKafkaDeliveryEvent;
  ATag: NativeInt);
begin
  inherited Create;
  FHub := AHub;
  FOnDelivery := AOnDelivery;
  FReport.Tag := ATag;
  FReport.Partition := -1;
  FReport.Offset := -1;
end;

procedure TDelivery.Complete(const AReport: TKafkaDeliveryReport);
var
  LReport: TKafkaDeliveryReport;
begin
  LReport := AReport;
  LReport.Tag := FReport.Tag;
  if Assigned(FOnDelivery) then
    try
      FOnDelivery(LReport);
    except
      // A callback's exception has nowhere to go (Java logs it).
    end;
  FHub.Monitor.Enter;
  try
    FReport := LReport;
    FDone := True;
    FHub.Monitor.PulseAll;
  finally
    FHub.Monitor.Leave;
  end;
end;

function TDelivery.Wait(ATimeoutMs: Cardinal): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := DeadlineOf(ATimeoutMs);
  FHub.Monitor.Enter;
  try
    while not FDone do
    begin
      if PcTickMs >= LDeadline then
        Exit(False);
      FHub.Monitor.Wait(Remaining(LDeadline));
    end;
    Result := True;
  finally
    FHub.Monitor.Leave;
  end;
end;

function TDelivery.IsDone: Boolean;
begin
  FHub.Monitor.Enter;
  try
    Result := FDone;
  finally
    FHub.Monitor.Leave;
  end;
end;

function TDelivery.Report: TKafkaDeliveryReport;
begin
  FHub.Monitor.Enter;
  try
    if not FDone then
      raise EKafka.Create('The record has no outcome yet');
    Result := FReport;
  finally
    FHub.Monitor.Leave;
  end;
end;

function TDelivery.Get(ATimeoutMs: Cardinal): TKafkaDeliveryReport;
begin
  if not Wait(ATimeoutMs) then
    raise EKafkaTimeout.CreateFmt('The record has no outcome after %d ms', [ATimeoutMs]);
  Result := Report;
  Result.RaiseIfFailed;
end;

{ TProducerBatch }

procedure TProducerBatch.Add(ADelivery: TDelivery);
begin
  if Count = Length(Entries) then
  begin
    if Count = 0 then
      SetLength(Entries, 8)
    else
      SetLength(Entries, Count * 2);
  end;
  Entries[Count].Delivery := ADelivery;
  Entries[Count].Hold := ADelivery;
  Inc(Count);
end;

{ TPartitionQueue }

constructor TPartitionQueue.Create(const ATopic: string; APartition: Int32);
begin
  inherited Create;
  Topic := ATopic;
  Partition := APartition;
  Batches := TList<TProducerBatch>.Create;
end;

destructor TPartitionQueue.Destroy;
begin
  Batches.Free;
  inherited;
end;

{ TKafkaProducer.TSenderThread }

constructor TKafkaProducer.TSenderThread.Create(AOwner: TKafkaProducer);
begin
  inherited Create(True);
  FOwner := AOwner;
  FreeOnTerminate := False;
end;

procedure TKafkaProducer.TSenderThread.Execute;
begin
  FOwner.RunSender;
end;

{ TKafkaProducer }

constructor TKafkaProducer.Create(const AParams: TKafkaProducerParams);
begin
  inherited Create;
  FParams := AParams;
  if FParams.BatchSize <= 0 then
    FParams.BatchSize := KAFKA_DEFAULT_BATCH_SIZE;
  if FParams.BufferMemory <= 0 then
    FParams.BufferMemory := KAFKA_DEFAULT_BUFFER_MEMORY;
  if FParams.DeliveryTimeoutMs = 0 then
    FParams.DeliveryTimeoutMs := KAFKA_DEFAULT_DELIVERY_TIMEOUT_MS;
  if FParams.MaxRequestSize <= 0 then
    FParams.MaxRequestSize := KAFKA_DEFAULT_MAX_REQUEST_SIZE;
  if FParams.MaxInFlightPerBroker <= 0 then
    FParams.MaxInFlightPerBroker := KAFKA_DEFAULT_MAX_IN_FLIGHT;
  if FParams.Retries < 0 then
    raise EArgumentException.CreateFmt('Retries can''t be negative (%d)', [FParams.Retries]);
  FRequestTimeoutMs := FParams.Cluster.Connection.RequestTimeoutMs;
  if FRequestTimeoutMs = 0 then
    FRequestTimeoutMs := KAFKA_DEFAULT_REQUEST_TIMEOUT_MS;
  FMaxBlockMs := FParams.Cluster.MaxBlockMs;
  if FMaxBlockMs = 0 then
    FMaxBlockMs := KAFKA_DEFAULT_MAX_BLOCK_MS;
  FRetryBackoffMs := FParams.Cluster.RetryBackoffMs;
  if FRetryBackoffMs = 0 then
    FRetryBackoffMs := KAFKA_DEFAULT_RETRY_BACKOFF_MS;
  // Java refuses the same configuration (delivery.timeout.ms should be
  // equal to or larger than linger.ms + request.timeout.ms).
  if UInt64(FParams.DeliveryTimeoutMs) < UInt64(FParams.LingerMs) + FRequestTimeoutMs then
    raise EArgumentException.CreateFmt('DeliveryTimeoutMs (%d) must be at least LingerMs (%d) ' +
      'plus the request timeout (%d)', [FParams.DeliveryTimeoutMs, FParams.LingerMs,
      FRequestTimeoutMs]);
  if not KafkaCompressionSupported(FParams.Compression) then
    raise EArgumentException.Create('This compression codec isn''t supported yet');
  FCluster := TKafkaCluster.Create(FParams.Cluster);
  FMonitor := TPcMonitor.Create;
  FHub := TDeliveryHub.Create as IDeliveryHub;
  FQueues := TObjectDictionary<string, TObject>.Create([doOwnsValues]);
  FQueueList := TList<TObject>.Create;
  FInFlight := TList<TObject>.Create;
  FTransit := TList<TObject>.Create;
  FSticky := TKafkaStickyPartitioner.Create(FParams.BatchSize);
  FWake := TKafkaEventSignal.Create;
  FWakeSignal := FWake;
  FNodeBackoff := TDictionary<Int32, UInt64>.Create;
end;

destructor TKafkaProducer.Destroy;
begin
  if FMonitor <> nil then
    Close(KAFKA_WAIT_INFINITE);
  FNodeBackoff.Free;
  FWakeSignal := nil;
  FSticky.Free;
  FTransit.Free;
  FInFlight.Free;
  FQueueList.Free;
  FQueues.Free;
  FMonitor.Free;
  FCluster.Free;
  FHub := nil;
  inherited;
end;

procedure TKafkaProducer.CheckNotSenderThread(const AWhat: string);
begin
  if (FSender <> nil) and (TThread.CurrentThread.ThreadID = FSenderThreadId) then
    raise EKafka.CreateFmt('%s can''t be called from a delivery callback', [AWhat]);
end;

procedure TKafkaProducer.Open;
begin
  FMonitor.Enter;
  try
    if FState <> psCreated then
      raise EKafka.Create('The producer was opened already');
  finally
    FMonitor.Leave;
  end;
  FCluster.Open;
  FMonitor.Enter;
  try
    FState := psOpen;
  finally
    FMonitor.Leave;
  end;
  FSender := TSenderThread.Create(Self);
  FSenderThreadId := FSender.ThreadID;
  FSender.Start;
end;

procedure TKafkaProducer.Close(ATimeoutMs: Cardinal);
var
  LWasOpen: Boolean;
begin
  CheckNotSenderThread('Close');
  FMonitor.Enter;
  try
    if FState in [psClosing, psClosed] then
      Exit;
    LWasOpen := FState = psOpen;
    if LWasOpen then
      FState := psClosing
    else
      FState := psClosed;
    // Sends waiting for buffer memory give up.
    FMonitor.PulseAll;
  finally
    FMonitor.Leave;
  end;
  if LWasOpen then
  begin
    FWake.Signal;
    WaitAll(ATimeoutMs);
    FMonitor.Enter;
    try
      FStopping := True;
    finally
      FMonitor.Leave;
    end;
    FWake.Signal;
    FSender.WaitFor;
    FreeAndNil(FSender);
  end;
  FCluster.Close;
  FMonitor.Enter;
  try
    FState := psClosed;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaProducer.WaitAll(ATimeoutMs: Cardinal): Boolean;
var
  LWait: TList<IKafkaDeliveryFuture>;
  LDeadline: UInt64;
  I, K: Integer;
  Q: TPartitionQueue;
  B: TProducerBatch;
  F: TInFlight;
begin
  LDeadline := DeadlineOf(ATimeoutMs);
  LWait := TList<IKafkaDeliveryFuture>.Create;
  try
    FMonitor.Enter;
    try
      for I := 0 to FQueueList.Count - 1 do
      begin
        Q := TPartitionQueue(FQueueList[I]);
        for B in Q.Batches do
          for K := 0 to B.Count - 1 do
            LWait.Add(B.Entries[K].Hold);
      end;
      for I := 0 to FInFlight.Count - 1 do
      begin
        F := TInFlight(FInFlight[I]);
        for B in F.Batches do
          for K := 0 to B.Count - 1 do
            LWait.Add(B.Entries[K].Hold);
      end;
      for I := 0 to FTransit.Count - 1 do
      begin
        B := TProducerBatch(FTransit[I]);
        for K := 0 to B.Count - 1 do
          LWait.Add(B.Entries[K].Hold);
      end;
      Inc(FFlushing);
    finally
      FMonitor.Leave;
    end;
    try
      FWake.Signal;
      Result := True;
      for I := 0 to LWait.Count - 1 do
        if not LWait[I].Wait(Remaining(LDeadline)) then
          Exit(False);
    finally
      FMonitor.Enter;
      try
        Dec(FFlushing);
      finally
        FMonitor.Leave;
      end;
    end;
  finally
    LWait.Free;
  end;
end;

function TKafkaProducer.Flush(ATimeoutMs: Cardinal): Boolean;
begin
  CheckNotSenderThread('Flush');
  Result := WaitAll(ATimeoutMs);
end;

function TKafkaProducer.TopicFor(const AName: string): TKafkaTopicInfo;
var
  LMetadata: IKafkaClusterMetadata;
begin
  // The snapshot when it knows the topic (no lock, no I/O); the cluster's
  // wait otherwise. Stale metadata is the sender's to refresh.
  LMetadata := FCluster.Metadata;
  if (LMetadata <> nil) and LMetadata.FindTopic(AName, Result) and Result.IsReady then
    Exit;
  Result := FCluster.Topic(AName);
end;

function TKafkaProducer.Send(const ARecord: TKafkaProducerRecord): IKafkaDeliveryFuture;
begin
  Result := Send(ARecord, nil, 0);
end;

function TKafkaProducer.Send(const ARecord: TKafkaProducerRecord;
  AOnDelivery: TKafkaDeliveryEvent; ATag: NativeInt): IKafkaDeliveryFuture;
var
  D: TDelivery;
  LTopic: TKafkaTopicInfo;
  LPartition: Int32;
  LSize: Integer;
  LDeadline: UInt64;
  LRecord: TKafkaProducerRecord;
begin
  if ARecord.Topic = '' then
    raise EArgumentException.Create('A record needs a topic');
  if ARecord.Partition < KAFKA_ANY_PARTITION then
    raise EArgumentException.CreateFmt('Invalid partition %d', [ARecord.Partition]);
  FMonitor.Enter;
  try
    case FState of
      psCreated: raise EKafka.Create('The producer is not open (call Open)');
      psClosing, psClosed: raise EKafka.Create('The producer is closed');
    end;
  finally
    FMonitor.Leave;
  end;
  LDeadline := PcTickMs + FMaxBlockMs;
  D := TDelivery.Create(FHub as IDeliveryHub, AOnDelivery, ATag);
  Result := D;
  LRecord := ARecord;
  if LRecord.Timestamp < 0 then
    LRecord.Timestamp := NowUnixMs;
  LSize := RecordSize(LRecord);
  if (LSize + KAFKA_RECORD_BATCH_HEADER_SIZE + REQUEST_OVERHEAD + 3 * Length(LRecord.Topic) >
    FParams.MaxRequestSize) or (LSize > FParams.BufferMemory) then
  begin
    D.Complete(FailedReport(LRecord.Topic, LRecord.Partition, EKafkaBrokerError,
      KAFKA_ERROR_MESSAGE_TOO_LARGE, Format('The record is %d bytes serialized, larger than ' +
      'MaxRequestSize (%d) or BufferMemory (%d)', [LSize, FParams.MaxRequestSize,
      FParams.BufferMemory])));
    Exit;
  end;
  try
    LTopic := TopicFor(LRecord.Topic);
  except
    on E: EKafka do
    begin
      if E is EKafkaBrokerError then
        D.Complete(FailedReport(LRecord.Topic, LRecord.Partition, EKafkaBrokerError,
          EKafkaBrokerError(E).ErrorCode, E.Message))
      else
        D.Complete(FailedReport(LRecord.Topic, LRecord.Partition, EKafkaClass(E.ClassType),
          0, E.Message));
      Exit;
    end;
  end;
  LPartition := LRecord.Partition;
  if LPartition >= Length(LTopic.Partitions) then
  begin
    D.Complete(FailedReport(LRecord.Topic, LPartition, EKafkaBrokerError,
      KAFKA_ERROR_UNKNOWN_TOPIC_OR_PARTITION, Format('Topic %s has %d partitions, not %d',
      [LRecord.Topic, Length(LTopic.Partitions), LPartition + 1])));
    Exit;
  end;
  if (LPartition = KAFKA_ANY_PARTITION) and not LRecord.KeyIsNull then
    LPartition := KafkaPartitionForKey(LRecord.Key, Length(LTopic.Partitions));
  Append(D, LRecord, LTopic, LPartition, LSize, LDeadline);
end;

function TKafkaProducer.Append(ADelivery: TObject; const ARecord: TKafkaProducerRecord;
  const ATopic: TKafkaTopicInfo; APartition: Int32; ASize: Integer; ADeadline: UInt64): Boolean;
var
  D: TDelivery;
  Q: TPartitionQueue;
  B: TProducerBatch;
  LObj: TObject;
  LAvailable: TKafkaInt32Array;
  LRecord: TKafkaRecord;
  LWake: Boolean;
  LFailure: string;
  LFailureClass: EKafkaClass;
  I, N: Integer;
begin
  D := TDelivery(ADelivery);
  LWake := False;
  LFailure := '';
  LFailureClass := nil;
  FMonitor.Enter;
  try
    while (FState = psOpen) and (FBuffered + ASize > FParams.BufferMemory) do
    begin
      if PcTickMs >= ADeadline then
      begin
        LFailureClass := EKafkaTimeout;
        LFailure := Format('No buffer memory for the record in %d ms (BufferMemory %d)',
          [FMaxBlockMs, FParams.BufferMemory]);
        Break;
      end;
      FMonitor.Wait(Remaining(ADeadline));
    end;
    if (LFailureClass = nil) and (FState <> psOpen) then
    begin
      LFailureClass := EKafka;
      LFailure := 'The producer was closed before the record was queued';
    end;
    if LFailureClass = nil then
    begin
      if APartition = KAFKA_ANY_PARTITION then
      begin
        LAvailable := nil;
        SetLength(LAvailable, Length(ATopic.Partitions));
        N := 0;
        for I := 0 to High(ATopic.Partitions) do
          if ATopic.Partitions[I].LeaderId >= 0 then
          begin
            LAvailable[N] := I;
            Inc(N);
          end;
        SetLength(LAvailable, N);
        APartition := FSticky.Partition(ARecord.Topic, Length(ATopic.Partitions), LAvailable,
          ASize);
      end;
      if FQueues.TryGetValue(QueueKey(ARecord.Topic, APartition), LObj) then
        Q := TPartitionQueue(LObj)
      else
      begin
        Q := TPartitionQueue.Create(ARecord.Topic, APartition);
        FQueues.Add(QueueKey(ARecord.Topic, APartition), Q);
        FQueueList.Add(Q);
      end;
      B := nil;
      if Q.Batches.Count > 0 then
      begin
        B := Q.Batches[Q.Batches.Count - 1];
        if B.Closed or (B.Bytes + ASize > FParams.BatchSize) then
          B := nil;
      end;
      if B = nil then
      begin
        B := TProducerBatch.Create;
        B.Topic := ARecord.Topic;
        B.TopicId := ATopic.TopicId;
        B.Partition := APartition;
        B.Batch.Init;
        B.Batch.SetCompression(FParams.Compression);
        B.CreatedAt := PcTickMs;
        Q.Batches.Add(B);
        LWake := True;
      end;
      LRecord := Default(TKafkaRecord);
      LRecord.Key := ARecord.Key;
      LRecord.KeyIsNull := ARecord.KeyIsNull;
      LRecord.Value := ARecord.Value;
      LRecord.ValueIsNull := ARecord.ValueIsNull;
      LRecord.Headers := ARecord.Headers;
      LRecord.Timestamp := ARecord.Timestamp;
      B.Batch.Append(LRecord);
      D.FReport.Topic := ARecord.Topic;
      D.FReport.Partition := APartition;
      D.FReport.Timestamp := ARecord.Timestamp;
      B.Add(D);
      Inc(B.Bytes, ASize);
      Inc(FBuffered, ASize);
      if B.Bytes >= FParams.BatchSize then
        LWake := True;
    end;
  finally
    FMonitor.Leave;
  end;
  Result := LFailureClass = nil;
  if not Result then
  begin
    D.Complete(FailedReport(ARecord.Topic, APartition, LFailureClass, 0, LFailure));
    Exit;
  end;
  if LWake then
    FWake.Signal;
end;

function TKafkaProducer.PartitionsFor(const ATopic: string): TKafkaPartitionInfoArray;
begin
  Result := FCluster.Topic(ATopic).Partitions;
end;

function TKafkaProducer.PendingCount: Integer;
var
  I: Integer;
  B: TProducerBatch;
begin
  Result := 0;
  FMonitor.Enter;
  try
    for I := 0 to FQueueList.Count - 1 do
      for B in TPartitionQueue(FQueueList[I]).Batches do
        Inc(Result, B.Count);
    for I := 0 to FInFlight.Count - 1 do
      for B in TInFlight(FInFlight[I]).Batches do
        Inc(Result, B.Count);
    for I := 0 to FTransit.Count - 1 do
      Inc(Result, TProducerBatch(FTransit[I]).Count);
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaProducer.RequestsSent: Integer;
begin
  FMonitor.Enter;
  try
    Result := FRequestsSent;
  finally
    FMonitor.Leave;
  end;
end;

{ The sender thread }

procedure TKafkaProducer.RunSender;
var
  LNow, LNext: UInt64;
  LStop: Boolean;
begin
  while True do
  begin
    FMonitor.Enter;
    try
      LStop := FStopping;
    finally
      FMonitor.Leave;
    end;
    if LStop then
      Break;
    LNow := PcTickMs;
    // At most a second between passes, whatever the deadlines say.
    LNext := LNow + 1000;
    try
      HandleCompletions(LNow, LNext);
      MaybeRefreshMetadata(LNow);
      ExpireBatches(LNow, LNext);
      SendReady(LNow, LNext);
    except
      // Nothing above should raise; if something does, the next pass, after
      // a backoff, tries again rather than leaving the producer without a
      // sender.
      LNext := PcTickMs + FRetryBackoffMs;
    end;
    LNow := PcTickMs;
    if LNext > LNow then
      FWake.WaitFor(Cardinal(LNext - LNow));
  end;
  AbortAll;
end;

procedure TKafkaProducer.HandleCompletions(ANow: UInt64; var ANext: UInt64);
var
  LFlights: array of TInFlight;
  F: TInFlight;
  I: Integer;
begin
  LFlights := nil;
  FMonitor.Enter;
  try
    SetLength(LFlights, FInFlight.Count);
    for I := 0 to FInFlight.Count - 1 do
      LFlights[I] := TInFlight(FInFlight[I]);
  finally
    FMonitor.Leave;
  end;
  for I := 0 to High(LFlights) do
  begin
    F := LFlights[I];
    if not F.Future.IsDone then
    begin
      if ANow < F.SentAt + FRequestTimeoutMs then
      begin
        MinTo(ANext, F.SentAt + FRequestTimeoutMs);
        Continue;
      end;
      // Its future fails with EKafkaTimeout: retried below like any other.
      F.Broker.Connection.Abandon(F.Future);
    end;
    HandleResponse(F);
  end;
end;

procedure TKafkaProducer.HandleResponse(AFlight: TObject);
var
  F: TInFlight;
  LResponse: TKafkaProduceResponse;
  B: TProducerBatch;
  I, T, P: Integer;
  LFound: Boolean;
  LTopic: TKafkaProduceResponseTopicProduceResponse;
  LPart: TKafkaProduceResponsePartitionProduceResponse;
  LMessage: string;
  LMatch: Boolean;
begin
  F := TInFlight(AFlight);
  FMonitor.Enter;
  try
    FInFlight.Remove(F);
  finally
    FMonitor.Leave;
  end;
  try
    try
      LResponse.FromBytes(F.Future.Body, F.Version);
    except
      on E: EKafka do
      begin
        // No answer: the connection broke, the request timed out, or the
        // answer didn't decode. The first two are worth a retry.
        if E is EKafkaConnection then
          FCluster.Invalidate;
        for B in F.Batches do
          RetryOrFail(B, EKafkaClass(E.ClassType), 0, E.Message,
            (E is EKafkaConnection) or (E is EKafkaTimeout));
        Exit;
      end;
    end;
    for B in F.Batches do
    begin
      LFound := False;
      for T := 0 to High(LResponse.Responses) do
      begin
        LTopic := LResponse.Responses[T];
        if F.Version >= 13 then
          LMatch := LTopic.TopicId.Equals(B.TopicId)
        else
          LMatch := LTopic.Name = B.Topic;
        if not LMatch then
          Continue;
        for P := 0 to High(LTopic.PartitionResponses) do
        begin
          LPart := LTopic.PartitionResponses[P];
          if LPart.Index <> B.Partition then
            Continue;
          LFound := True;
          if LPart.ErrorCode = KAFKA_ERROR_NONE then
            CompleteBatch(B, LPart.BaseOffset, LPart.LogAppendTimeMs)
          else
          begin
            LMessage := Format('%s-%d: %s', [B.Topic, B.Partition,
              KafkaErrorText(LPart.ErrorCode)]);
            if not LPart.ErrorMessageIsNull and (LPart.ErrorMessage <> '') then
              LMessage := LMessage + ' (' + LPart.ErrorMessage + ')';
            for I := 0 to High(LPart.RecordErrors) do
              if not LPart.RecordErrors[I].BatchIndexErrorMessageIsNull then
                LMessage := LMessage + Format('; record %d: %s', [LPart.RecordErrors[I].BatchIndex,
                  LPart.RecordErrors[I].BatchIndexErrorMessage]);
            if KafkaErrorIsInvalidMetadata(LPart.ErrorCode) then
              FCluster.Invalidate;
            RetryOrFail(B, EKafkaBrokerError, LPart.ErrorCode, LMessage,
              KafkaErrorIsRetriable(LPart.ErrorCode));
          end;
          Break;
        end;
        if LFound then
          Break;
      end;
      if not LFound then
        FailBatch(B, EKafkaProtocol, 0, Format('The Produce response from broker %d has no ' +
          'answer for %s-%d', [F.NodeId, B.Topic, B.Partition]));
    end;
  finally
    F.Free;
  end;
end;

procedure TKafkaProducer.MaybeRefreshMetadata(ANow: UInt64);
begin
  if (ANow < FNextMetadataAt) or not FCluster.NeedsRefresh then
    Exit;
  FNextMetadataAt := ANow + FRetryBackoffMs;
  try
    FCluster.Refresh([]);
  except
    on EKafka do ; // tried again after the backoff
  end;
end;

procedure TKafkaProducer.ExpireBatches(ANow: UInt64; var ANext: UInt64);
var
  LExpired: TList<TProducerBatch>;
  I: Integer;
  Q: TPartitionQueue;
  B: TProducerBatch;
begin
  LExpired := TList<TProducerBatch>.Create;
  try
    FMonitor.Enter;
    try
      for I := 0 to FQueueList.Count - 1 do
      begin
        Q := TPartitionQueue(FQueueList[I]);
        // A muted partition's batches wait for the one in flight, whose
        // outcome comes first (in RequestTimeoutMs at most).
        if Q.Muted then
          Continue;
        while Q.Batches.Count > 0 do
        begin
          B := Q.Batches[0];
          if ANow < B.CreatedAt + FParams.DeliveryTimeoutMs then
          begin
            MinTo(ANext, B.CreatedAt + FParams.DeliveryTimeoutMs);
            Break;
          end;
          Q.Batches.Delete(0);
          LExpired.Add(B);
        end;
      end;
    finally
      FMonitor.Leave;
    end;
    for B in LExpired do
      FailBatch(B, EKafkaTimeout, 0, Format('Expiring %d record(s) for %s-%d: %d ms has ' +
        'passed since batch creation', [B.Count, B.Topic, B.Partition,
        Int64(ANow - B.CreatedAt)]));
  finally
    LExpired.Free;
  end;
end;

procedure TKafkaProducer.SendReady(ANow: UInt64; var ANext: UInt64);
var
  LByNode: TObjectDictionary<Int32, TList<TObject>>;
  LSizes: TDictionary<Int32, Integer>;
  LInFlight: TDictionary<Int32, Integer>;
  LMetadata: IKafkaClusterMetadata;
  LTopic: TKafkaTopicInfo;
  LList: TList<TObject>;
  LNeedMetadata: Boolean;
  LAll: Boolean;
  Q: TPartitionQueue;
  B: TProducerBatch;
  I, N, LNode, LSize, LCount: Integer;
  LUntil: UInt64;
  LPair: TPair<Int32, TList<TObject>>;
begin
  LByNode := TObjectDictionary<Int32, TList<TObject>>.Create([doOwnsValues]);
  LSizes := TDictionary<Int32, Integer>.Create;
  LInFlight := TDictionary<Int32, Integer>.Create;
  try
    LNeedMetadata := False;
    LMetadata := FCluster.Metadata;
    FMonitor.Enter;
    try
      LAll := (FFlushing > 0) or (FState <> psOpen);
      for I := 0 to FInFlight.Count - 1 do
      begin
        LNode := TInFlight(FInFlight[I]).NodeId;
        if LInFlight.TryGetValue(LNode, LCount) then
          LInFlight[LNode] := LCount + 1
        else
          LInFlight.Add(LNode, 1);
      end;
      N := FQueueList.Count;
      for I := 0 to N - 1 do
      begin
        Q := TPartitionQueue(FQueueList[(FDrainIndex + I) mod N]);
        if Q.Muted or (Q.Batches.Count = 0) then
          Continue;
        B := Q.Batches[0];
        if B.RetryAt > ANow then
        begin
          MinTo(ANext, B.RetryAt);
          Continue;
        end;
        if not (LAll or (Q.Batches.Count > 1) or (B.Bytes >= FParams.BatchSize) or
          (ANow >= B.CreatedAt + FParams.LingerMs)) then
        begin
          MinTo(ANext, B.CreatedAt + FParams.LingerMs);
          Continue;
        end;
        if (LMetadata = nil) or not LMetadata.FindTopic(Q.Topic, LTopic) or
          (Q.Partition >= Length(LTopic.Partitions)) or
          (LTopic.Partitions[Q.Partition].LeaderId < 0) then
        begin
          LNeedMetadata := True;
          MinTo(ANext, ANow + FRetryBackoffMs);
          Continue;
        end;
        LNode := LTopic.Partitions[Q.Partition].LeaderId;
        if FNodeBackoff.TryGetValue(LNode, LUntil) and (LUntil > ANow) then
        begin
          MinTo(ANext, LUntil);
          Continue;
        end;
        if not LInFlight.TryGetValue(LNode, LCount) then
          LCount := 0;
        if not LByNode.TryGetValue(LNode, LList) then
        begin
          // A new request for this node: within MaxInFlightPerBroker.
          if LCount >= FParams.MaxInFlightPerBroker then
            Continue; // woken when one completes
          LList := TList<TObject>.Create;
          LByNode.Add(LNode, LList);
          LSizes.Add(LNode, REQUEST_OVERHEAD);
        end;
        LSize := LSizes[LNode] + KAFKA_RECORD_BATCH_HEADER_SIZE + B.Bytes + 3 * Length(B.Topic);
        if (LList.Count > 0) and (LSize > FParams.MaxRequestSize) then
        begin
          MinTo(ANext, ANow); // the next request
          Continue;
        end;
        LSizes[LNode] := LSize;
        Q.Batches.Delete(0);
        Q.Muted := True;
        B.Closed := True;
        B.TopicId := LTopic.TopicId;
        LList.Add(B);
        FTransit.Add(B);
      end;
      if N > 0 then
        FDrainIndex := (FDrainIndex + 1) mod N;
    finally
      FMonitor.Leave;
    end;
    if LNeedMetadata then
      FCluster.Invalidate;
    for LPair in LByNode do
      SendToNode(LPair.Key, LPair.Value);
  finally
    LInFlight.Free;
    LSizes.Free;
    LByNode.Free;
  end;
end;

procedure TKafkaProducer.SendToNode(ANodeId: Int32; ABatches: TList<TObject>);
var
  LBroker: IKafkaBrokerConnection;
  LVersion: Int16;
  LRequest: TKafkaProduceRequest;
  LTopics: TStringList;
  B: TProducerBatch;
  I, T, P: Integer;
  LBody: TBytes;
  F: TInFlight;
  LSent: TList<TProducerBatch>;
begin
  try
    LBroker := FCluster.Broker(ANodeId);
    LVersion := LBroker.Connection.NegotiatedVersion(KAFKA_API_KEY_PRODUCE);
  except
    on E: EKafka do
    begin
      // Not an attempt: nothing was written. The node waits a backoff, and
      // the metadata is refreshed (the broker may be gone).
      FMonitor.Enter;
      try
        FNodeBackoff.AddOrSetValue(ANodeId, PcTickMs + FRetryBackoffMs);
      finally
        FMonitor.Leave;
      end;
      FCluster.Invalidate;
      for I := 0 to ABatches.Count - 1 do
        if E is EKafkaUnsupportedVersion then
          FailBatch(ABatches[I], EKafkaUnsupportedVersion, 0, E.Message)
        else
          Requeue(ABatches[I], False);
      Exit;
    end;
  end;
  LSent := TList<TProducerBatch>.Create;
  LTopics := TStringList.Create;
  try
    LRequest.Init;
    LRequest.Acks := AcksValue(FParams.Acks);
    LRequest.TimeoutMs := FRequestTimeoutMs;
    for I := 0 to ABatches.Count - 1 do
    begin
      B := TProducerBatch(ABatches[I]);
      if B.Encoded = nil then
        try
          B.Encoded := B.Batch.Encode;
        except
          on E: EKafka do
          begin
            FailBatch(B, EKafkaClass(E.ClassType), 0, E.Message);
            Continue;
          end;
        end;
      T := LTopics.IndexOf(B.Topic);
      if T < 0 then
      begin
        T := LTopics.Add(B.Topic);
        SetLength(LRequest.TopicData, T + 1);
        LRequest.TopicData[T].Init;
        // Name up to v12, TopicId from v13 (both ignorable).
        LRequest.TopicData[T].Name := B.Topic;
        LRequest.TopicData[T].TopicId := B.TopicId;
      end;
      P := Length(LRequest.TopicData[T].PartitionData);
      SetLength(LRequest.TopicData[T].PartitionData, P + 1);
      LRequest.TopicData[T].PartitionData[P].Init;
      LRequest.TopicData[T].PartitionData[P].Index := B.Partition;
      LRequest.TopicData[T].PartitionData[P].Records := B.Encoded;
      LRequest.TopicData[T].PartitionData[P].RecordsIsNull := False;
      Inc(B.Attempts);
      LSent.Add(B);
    end;
    if LSent.Count = 0 then
      Exit;
    LBody := LRequest.ToBytes(LVersion);
    try
      if FParams.Acks = kaNone then
      begin
        LBroker.Connection.SendOneWay(KAFKA_API_KEY_PRODUCE, LVersion, LBody);
        FMonitor.Enter;
        try
          Inc(FRequestsSent);
        finally
          FMonitor.Leave;
        end;
        // No answer comes: the records are done once written.
        for B in LSent do
          CompleteBatch(B, -1, -1);
        Exit;
      end;
      F := TInFlight.Create;
      F.NodeId := ANodeId;
      F.Broker := LBroker;
      F.Version := LVersion;
      F.SentAt := PcTickMs;
      SetLength(F.Batches, LSent.Count);
      for I := 0 to LSent.Count - 1 do
        F.Batches[I] := LSent[I];
      try
        F.Future := LBroker.Connection.Send(KAFKA_API_KEY_PRODUCE, LVersion, LBody,
          FWakeSignal);
      except
        F.Free;
        raise;
      end;
      // Only this thread completes requests, so a response that is already
      // in waits for the next pass.
      FMonitor.Enter;
      try
        FInFlight.Add(F);
        for B in LSent do
          FTransit.Remove(B);
        Inc(FRequestsSent);
      finally
        FMonitor.Leave;
      end;
    except
      on E: EKafka do
      begin
        // The write failed: the connection is broken (the next Broker call
        // opens another). The attempt counts, as part of it may have gone.
        FCluster.Invalidate;
        for B in LSent do
          RetryOrFail(B, EKafkaClass(E.ClassType), 0, E.Message, E is EKafkaConnection);
      end;
    end;
  finally
    LTopics.Free;
    LSent.Free;
  end;
end;

procedure TKafkaProducer.RetryOrFail(ABatch: TObject; AClass: EKafkaClass; ACode: Int16;
  const AMessage: string; ARetriable: Boolean);
var
  B: TProducerBatch;
  LStopping: Boolean;
begin
  B := TProducerBatch(ABatch);
  FMonitor.Enter;
  try
    LStopping := FStopping;
  finally
    FMonitor.Leave;
  end;
  if ARetriable and not LStopping and (B.Attempts <= FParams.Retries) and
    (PcTickMs + FRetryBackoffMs < B.CreatedAt + FParams.DeliveryTimeoutMs) then
    Requeue(B, True)
  else
    FailBatch(B, AClass, ACode, AMessage);
end;

procedure TKafkaProducer.Requeue(ABatch: TObject; ACountAttempt: Boolean);
var
  B: TProducerBatch;
  LObj: TObject;
  Q: TPartitionQueue;
begin
  B := TProducerBatch(ABatch);
  FMonitor.Enter;
  try
    if ACountAttempt then
      B.RetryAt := PcTickMs + FRetryBackoffMs;
    FQueues.TryGetValue(QueueKey(B.Topic, B.Partition), LObj);
    Q := TPartitionQueue(LObj);
    // At the head: the partition's order holds through retries.
    Q.Batches.Insert(0, B);
    Q.Muted := False;
    FTransit.Remove(B);
  finally
    FMonitor.Leave;
  end;
  FWake.Signal;
end;

procedure TKafkaProducer.ReleaseBatch(ABatch: TObject);
var
  B: TProducerBatch;
  LObj: TObject;
begin
  B := TProducerBatch(ABatch);
  FMonitor.Enter;
  try
    Dec(FBuffered, B.Bytes);
    FTransit.Remove(B);
    if FQueues.TryGetValue(QueueKey(B.Topic, B.Partition), LObj) then
      TPartitionQueue(LObj).Muted := False;
    // Sends waiting for memory.
    FMonitor.PulseAll;
  finally
    FMonitor.Leave;
  end;
end;

procedure TKafkaProducer.CompleteBatch(ABatch: TObject; ABaseOffset, ALogAppendTime: Int64);
var
  B: TProducerBatch;
  I: Integer;
  R: TKafkaDeliveryReport;
begin
  B := TProducerBatch(ABatch);
  ReleaseBatch(B);
  try
    for I := 0 to B.Count - 1 do
    begin
      R := Default(TKafkaDeliveryReport);
      R.Topic := B.Topic;
      R.Partition := B.Partition;
      if ABaseOffset >= 0 then
        R.Offset := ABaseOffset + I
      else
        R.Offset := -1;
      if ALogAppendTime >= 0 then
        R.Timestamp := ALogAppendTime
      else
        R.Timestamp := B.Batch.Records[I].Timestamp;
      B.Entries[I].Delivery.Complete(R);
    end;
  finally
    B.Free;
  end;
  FWake.Signal;
end;

procedure TKafkaProducer.FailBatch(ABatch: TObject; AClass: EKafkaClass; ACode: Int16;
  const AMessage: string);
var
  B: TProducerBatch;
  I: Integer;
  R: TKafkaDeliveryReport;
begin
  B := TProducerBatch(ABatch);
  ReleaseBatch(B);
  try
    for I := 0 to B.Count - 1 do
    begin
      R := FailedReport(B.Topic, B.Partition, AClass, ACode, AMessage);
      R.Timestamp := B.Batch.Records[I].Timestamp;
      B.Entries[I].Delivery.Complete(R);
    end;
  finally
    B.Free;
  end;
  FWake.Signal;
end;

procedure TKafkaProducer.AbortAll;
var
  LFlights: TList<TObject>;
  LQueued: TList<TProducerBatch>;
  I: Integer;
  F: TInFlight;
  B: TProducerBatch;
  Q: TPartitionQueue;
const
  MSG = 'The producer was closed before the record was delivered';
begin
  LFlights := TList<TObject>.Create;
  LQueued := TList<TProducerBatch>.Create;
  try
    FMonitor.Enter;
    try
      LFlights.AddRange(FInFlight);
      FInFlight.Clear;
      for I := 0 to FQueueList.Count - 1 do
      begin
        Q := TPartitionQueue(FQueueList[I]);
        LQueued.AddRange(Q.Batches);
        Q.Batches.Clear;
      end;
    finally
      FMonitor.Leave;
    end;
    for I := 0 to LFlights.Count - 1 do
    begin
      F := TInFlight(LFlights[I]);
      try
        F.Broker.Connection.Abandon(F.Future);
        for B in F.Batches do
          FailBatch(B, EKafka, 0, MSG + ' (it may have been written)');
      finally
        F.Free;
      end;
    end;
    for B in LQueued do
      FailBatch(B, EKafka, 0, MSG);
  finally
    LQueued.Free;
    LFlights.Free;
  end;
end;

end.
