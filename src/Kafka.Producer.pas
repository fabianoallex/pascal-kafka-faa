unit Kafka.Producer;

{$I kafka.inc}

{ The producer: Send queues a record and returns at once; a sender thread
  groups the records into batches per partition and sends each batch to its
  partition's leader, retrying what can be retried; each record's outcome
  goes to a callback and to a future.

  The shape is Java's KafkaProducer (docs/design.md, section 4, "Threads"),
  idempotent by default as Java's since 3.0. The parameters keep Java's names and
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
  MaxInFlightPerBroker requests waiting per broker. Without idempotence a
  partition has at most one batch in flight (Java mutes it so): a retried
  batch goes back to the head of its partition's queue, so records are
  written in the order they were sent, retries included, but may be written
  twice (a request that timed out may have been written): at-least-once.

  Idempotence (Params.Idempotence; Java 4.4.0's TransactionManager,
  RecordAccumulator and Sender, read for it). On by default: kidAuto turns
  it off when Acks isn't kaAll or Retries is 0, as Java does when
  enable.idempotence wasn't set; kidOn refuses those; MaxInFlightPerBroker
  above 5 is refused unless it is off. The sender asks any broker for a
  producer id (InitProducerId, no transactional id) before the first batch
  goes, again after a retriable error, and fails the records waiting on any
  other. Each batch carries the id, the epoch and its partition's next
  sequence, fixed when it is first sent and kept through its retries, so a
  broker that wrote it already answers the retry with the first offset
  instead of writing it again (the broker keeps the last five batches of
  each producer and partition). Up to five batches of a partition are in
  flight (one per request); a retried batch goes back in sequence order and
  is sent again only when it is the first not done, so retries go one at a
  time. DUPLICATE_SEQUENCE_NUMBER counts as delivered (offset -1).
  OUT_OF_ORDER_SEQUENCE_NUMBER is retried: for a batch whose predecessor
  failed, in its turn; for the next batch itself, a gap, after an epoch
  bump. UNKNOWN_PRODUCER_ID is retried after a bump. A batch that fails with
  its sequence used (INVALID_PRODUCER_EPOCH, a broken request out of time)
  bumps the epoch; one that expires while waiting for a retry leaves its
  sequence unresolved, and new batches of the partition wait until those
  before them are done, then either go on or bump. A bump is local, as
  Java's: the epoch plus one, the partition's batches not done renumbered
  from 0 and encoded again (a broker takes a higher epoch from sequence 0);
  a new id from InitProducerId only when the epoch is spent. Other
  partitions start from 0 on the new epoch once their old batches are done.

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

  KAFKA_DEFAULT_LINGER_MS = 5;
  KAFKA_DEFAULT_BATCH_SIZE = 16384;
  KAFKA_DEFAULT_BUFFER_MEMORY = 32 * 1024 * 1024;
  KAFKA_DEFAULT_DELIVERY_TIMEOUT_MS = 120000;
  KAFKA_DEFAULT_MAX_REQUEST_SIZE = 1024 * 1024;
  KAFKA_DEFAULT_MAX_IN_FLIGHT = 5;
  /// The most requests in flight per broker an idempotent producer may have:
  /// a broker keeps the last five batches of each producer and partition.
  KAFKA_MAX_IN_FLIGHT_FOR_IDEMPOTENCE = 5;

type
  /// Java's acks: all (-1, the default), 1, 0. The zero of the enumeration
  /// is the default on purpose.
  TKafkaAcks = (kaAll, kaLeader, kaNone);

  /// Java's enable.idempotence, with "left at its default" as a value of its
  /// own, since a record can't tell it from "set": kidAuto (the default) is
  /// on unless Acks isn't kaAll or Retries is 0, and then off without a word;
  /// kidOn refuses those two at Create; kidOff is the producer of phase K1.
  /// MaxInFlightPerBroker above 5 is refused whenever idempotence is on,
  /// kidAuto included, as Java does.
  TKafkaIdempotence = (kidAuto, kidOn, kidOff);

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
    /// The idempotent producer (the unit's header, "Idempotence").
    Idempotence: TKafkaIdempotence;
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
    /// Batches out of their queue and not in flight: on their way to a
    /// request, or out of one (answered, expired, aborted) until every
    /// future is done or the batch is queued again. Flush sees them here;
    /// a batch in none of the three lists would let it return early.
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
    // Idempotence (the sender thread's, but FProducerId and FProducerEpoch
    // are written under FMonitor for the diagnostics).
    FIdempotent: Boolean;
    FProducerId: Int64;
    FProducerEpoch: Int16;
    FInitBroker: IKafkaBrokerConnection;
    FInitFuture: IKafkaResponseFuture;
    FInitVersion: Int16;
    FInitSentAt: UInt64;
    FInitAt: UInt64;
    FInitCount: Integer;
    FBumpRequired: Boolean;
    FRewrite: TList<TObject>;
    FEpochBumps: Integer;
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
    procedure LeaveTransit(ABatch: TObject);
    procedure AbortAll;
    function CanRetryNow(ABatch: TObject): Boolean;
    // Idempotence, on the sender thread.
    function QueueOf(ABatch: TObject): TObject;
    function IsStale(AQueue: TObject): Boolean;
    procedure SetProducer(AId: Int64; AEpoch: Int16);
    procedure StepInitProducerId(ANow: UInt64; var ANext: UInt64);
    procedure FailUnsequenced(AClass: EKafkaClass; ACode: Int16; const AMessage: string);
    procedure ResolveSequences;
    procedure RequestBump(AQueue: TObject);
    procedure ApplyBump;
    procedure RewriteSequences;
    procedure StartSequencesAtBeginning(AQueue: TObject);
    function ShouldStopDrain(AQueue, ABatch: TObject): Boolean;
    procedure AssignSequence(AQueue, ABatch: TObject);
    function IdempotentCanRetry(ABatch: TObject; ACode: Int16; ALogStartOffset: Int64): Boolean;
    procedure BatchAcked(ABatch: TObject; ABaseOffset: Int64);
    procedure BatchFailed(ABatch: TObject; ACode: Int16; AAdjust: Boolean);
    procedure MarkUnresolved(ABatch: TObject);
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
    /// Whether the producer is idempotent (Params.Idempotence resolved).
    function Idempotent: Boolean;
    /// The producer id and epoch in use; -1 and -1 before the first
    /// InitProducerId answer, and without idempotence.
    function ProducerId: Int64;
    function ProducerEpoch: Int16;
    /// InitProducerId requests sent, and epoch bumps made, since Open (for
    /// tests and diagnostics).
    function InitProducerIdCount: Integer;
    function EpochBumps: Integer;
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
  Kafka.Messages.Produce,
  Kafka.Messages.InitProducerId;

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
    /// Idempotence: the batch was renumbered by an epoch bump (Java's
    /// sequenceHasBeenReset).
    SequenceReset: Boolean;
    procedure Add(ADelivery: TDelivery);
    function HasSequence: Boolean;
    function LastSequence: Int32;
  end;

  TPartitionQueue = class
  public
    Topic: string;
    Partition: Int32;
    Batches: TList<TProducerBatch>;
    /// A batch of this partition is in flight (without idempotence).
    Muted: Boolean;
    // Idempotence (Java's TxnPartitionEntry), the sender thread's.
    /// The id and epoch this partition's sequences belong to.
    SeqProducerId: Int64;
    SeqProducerEpoch: Int16;
    NextSequence: Int32;
    LastAckedSequence: Int32;
    LastAckedOffset: Int64;
    /// The batches with a sequence not done yet (in flight, or queued for a
    /// retry), by sequence.
    Sequenced: TList<TProducerBatch>;
    /// A batch with a sequence failed without an answer: whether the broker
    /// has it is unknown until the batches after it say.
    Unresolved: Boolean;
    UnresolvedNext: Int32;
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

/// ASequence plus AIncrement, wrapping past MaxInt to 0 (Java's
/// DefaultRecordBatch.incrementSequence).
function IncrementSequence(ASequence: Int32; AIncrement: Integer): Int32;
begin
  if Int64(ASequence) > Int64(High(Int32)) - AIncrement then
    Result := Int32(Int64(AIncrement) - (Int64(High(Int32)) - ASequence) - 1)
  else
    Result := ASequence + AIncrement;
end;

function TProducerBatch.HasSequence: Boolean;
begin
  Result := Batch.BaseSequence <> KAFKA_NO_SEQUENCE;
end;

function TProducerBatch.LastSequence: Int32;
begin
  Result := IncrementSequence(Batch.BaseSequence, Count - 1);
end;

{ TPartitionQueue }

constructor TPartitionQueue.Create(const ATopic: string; APartition: Int32);
begin
  inherited Create;
  Topic := ATopic;
  Partition := APartition;
  Batches := TList<TProducerBatch>.Create;
  Sequenced := TList<TProducerBatch>.Create;
  SeqProducerId := KAFKA_NO_PRODUCER_ID;
  SeqProducerEpoch := KAFKA_NO_PRODUCER_EPOCH;
  LastAckedSequence := -1;
  LastAckedOffset := -1;
end;

destructor TPartitionQueue.Destroy;
begin
  Sequenced.Free;
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
  // Java's ProducerConfig.postProcessAndValidateIdempotenceConfigs.
  case FParams.Idempotence of
    kidOff:
      FIdempotent := False;
    kidOn:
      begin
        if FParams.Acks <> kaAll then
          raise EArgumentException.Create('Acks must be kaAll to use the idempotent producer');
        if FParams.Retries = 0 then
          raise EArgumentException.Create('Retries must not be 0 to use the idempotent producer');
        FIdempotent := True;
      end;
  else
    FIdempotent := (FParams.Acks = kaAll) and (FParams.Retries > 0);
  end;
  if FIdempotent and (FParams.MaxInFlightPerBroker > KAFKA_MAX_IN_FLIGHT_FOR_IDEMPOTENCE) then
    raise EArgumentException.CreateFmt('MaxInFlightPerBroker must be at most %d for the ' +
      'idempotent producer (it is %d); set Idempotence to kidOff for more',
      [KAFKA_MAX_IN_FLIGHT_FOR_IDEMPOTENCE, FParams.MaxInFlightPerBroker]);
  FProducerId := KAFKA_NO_PRODUCER_ID;
  FProducerEpoch := KAFKA_NO_PRODUCER_EPOCH;
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
  FRewrite := TList<TObject>.Create;
end;

destructor TKafkaProducer.Destroy;
begin
  if FMonitor <> nil then
    Close(KAFKA_WAIT_INFINITE);
  FRewrite.Free;
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

function TKafkaProducer.Idempotent: Boolean;
begin
  Result := FIdempotent;
end;

function TKafkaProducer.ProducerId: Int64;
begin
  FMonitor.Enter;
  try
    Result := FProducerId;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaProducer.ProducerEpoch: Int16;
begin
  FMonitor.Enter;
  try
    Result := FProducerEpoch;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaProducer.InitProducerIdCount: Integer;
begin
  FMonitor.Enter;
  try
    Result := FInitCount;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaProducer.EpochBumps: Integer;
begin
  FMonitor.Enter;
  try
    Result := FEpochBumps;
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
      if FIdempotent then
      begin
        // Java's Sender.runOnce: resolve, bump, then a producer id if
        // there is none.
        ResolveSequences;
        ApplyBump;
        StepInitProducerId(LNow, LNext);
      end;
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
    // In transit until completed or queued again, so that a Flush starting
    // now waits for them (CompleteBatch and FailBatch take them out).
    for B in F.Batches do
      FTransit.Add(B);
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
          if (LPart.ErrorCode = KAFKA_ERROR_NONE) or (FIdempotent and
            (LPart.ErrorCode = KAFKA_ERROR_DUPLICATE_SEQUENCE_NUMBER)) then
          begin
            // DUPLICATE_SEQUENCE_NUMBER: written before, and too long ago for
            // the broker to say at which offset; Java reports success with
            // offset -1, and so does this (the broker sends -1).
            if FIdempotent then
              BatchAcked(B, LPart.BaseOffset);
            if LPart.ErrorCode <> KAFKA_ERROR_NONE then
              CompleteBatch(B, -1, -1)
            else
              CompleteBatch(B, LPart.BaseOffset, LPart.LogAppendTimeMs);
          end
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
            // Java checks the attempts and the deadline before asking the
            // transaction manager, which may request a bump.
            if FIdempotent then
              RetryOrFail(B, EKafkaBrokerError, LPart.ErrorCode, LMessage, CanRetryNow(B) and
                IdempotentCanRetry(B, LPart.ErrorCode, LPart.LogStartOffset))
            else
              RetryOrFail(B, EKafkaBrokerError, LPart.ErrorCode, LMessage,
                KafkaErrorIsRetriable(LPart.ErrorCode));
          end;
          Break;
        end;
        if LFound then
          Break;
      end;
      if not LFound then
      begin
        if FIdempotent then
          BatchFailed(B, 0, True);
        FailBatch(B, EKafkaProtocol, 0, Format('The Produce response from broker %d has no ' +
          'answer for %s-%d', [F.NodeId, B.Topic, B.Partition]));
      end;
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
          FTransit.Add(B);
        end;
      end;
    finally
      FMonitor.Leave;
    end;
    for B in LExpired do
    begin
      // A batch with a sequence was sent and is waiting for a retry: the
      // broker may have it (Java's markSequenceUnresolved).
      if FIdempotent and B.HasSequence then
        MarkUnresolved(B);
      FailBatch(B, EKafkaTimeout, 0, Format('Expiring %d record(s) for %s-%d: %d ms has ' +
        'passed since batch creation', [B.Count, B.Topic, B.Partition,
        Int64(ANow - B.CreatedAt)]));
    end;
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
      // Nothing goes out before the producer has an id (Java's
      // shouldStopDrainBatchesForPartition).
      if FIdempotent and (FProducerId = KAFKA_NO_PRODUCER_ID) then
        Exit;
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
        // Woken when the batch it waits for completes.
        if FIdempotent and ShouldStopDrain(Q, B) then
          Continue;
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
        if FIdempotent then
        begin
          if not B.HasSequence then
            AssignSequence(Q, B);
        end
        else
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
        begin
          if FIdempotent then
            BatchFailed(ABatches[I], 0, True);
          FailBatch(ABatches[I], EKafkaUnsupportedVersion, 0, E.Message);
        end
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
            if FIdempotent then
              BatchFailed(B, 0, True);
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

function TKafkaProducer.CanRetryNow(ABatch: TObject): Boolean;
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
  Result := not LStopping and (B.Attempts <= FParams.Retries) and
    (PcTickMs + FRetryBackoffMs < B.CreatedAt + FParams.DeliveryTimeoutMs);
end;

procedure TKafkaProducer.RetryOrFail(ABatch: TObject; AClass: EKafkaClass; ACode: Int16;
  const AMessage: string; ARetriable: Boolean);
var
  B: TProducerBatch;
begin
  B := TProducerBatch(ABatch);
  if ARetriable and CanRetryNow(B) then
    Requeue(B, True)
  else
  begin
    // Java adjusts the sequences only when the batch still had attempts
    // left: otherwise whether the broker took it is unknown.
    if FIdempotent then
      BatchFailed(B, ACode, B.Attempts <= FParams.Retries);
    FailBatch(B, AClass, ACode, AMessage);
  end;
end;

procedure TKafkaProducer.Requeue(ABatch: TObject; ACountAttempt: Boolean);
var
  B: TProducerBatch;
  LObj: TObject;
  Q: TPartitionQueue;
  I: Integer;
begin
  B := TProducerBatch(ABatch);
  FMonitor.Enter;
  try
    if ACountAttempt then
      B.RetryAt := PcTickMs + FRetryBackoffMs;
    FQueues.TryGetValue(QueueKey(B.Topic, B.Partition), LObj);
    Q := TPartitionQueue(LObj);
    // At the head: the partition's order holds through retries. With
    // idempotence, behind the queued batches with a lower sequence (several
    // were in flight and come back one by one: Java's insertInSequenceOrder).
    I := 0;
    if FIdempotent and B.HasSequence then
      while (I < Q.Batches.Count) and Q.Batches[I].HasSequence and
        (Q.Batches[I].Batch.BaseSequence < B.Batch.BaseSequence) do
        Inc(I);
    Q.Batches.Insert(I, B);
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
    if FQueues.TryGetValue(QueueKey(B.Topic, B.Partition), LObj) then
    begin
      TPartitionQueue(LObj).Muted := False;
      // The batch is freed next: no list may keep it.
      TPartitionQueue(LObj).Sequenced.Remove(B);
    end;
    // Sends waiting for memory.
    FMonitor.PulseAll;
  finally
    FMonitor.Leave;
  end;
end;

procedure TKafkaProducer.LeaveTransit(ABatch: TObject);
begin
  FMonitor.Enter;
  try
    FTransit.Remove(ABatch);
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
    LeaveTransit(B);
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
    LeaveTransit(B);
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
      for I := 0 to LFlights.Count - 1 do
        for B in TInFlight(LFlights[I]).Batches do
          FTransit.Add(B);
      for I := 0 to FQueueList.Count - 1 do
      begin
        Q := TPartitionQueue(FQueueList[I]);
        LQueued.AddRange(Q.Batches);
        for B in Q.Batches do
          FTransit.Add(B);
        Q.Batches.Clear;
      end;
    finally
      FMonitor.Leave;
    end;
    if FInitFuture <> nil then
    begin
      FInitBroker.Connection.Abandon(FInitFuture);
      FInitFuture := nil;
    end;
    FInitBroker := nil;
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

{ Idempotence: Java's TransactionManager (its idempotent half), on the sender
  thread. }

function TKafkaProducer.QueueOf(ABatch: TObject): TObject;
var
  B: TProducerBatch;
begin
  B := TProducerBatch(ABatch);
  FMonitor.Enter;
  try
    if not FQueues.TryGetValue(QueueKey(B.Topic, B.Partition), Result) then
      Result := nil;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaProducer.IsStale(AQueue: TObject): Boolean;
var
  Q: TPartitionQueue;
begin
  Q := TPartitionQueue(AQueue);
  Result := (Q.SeqProducerId <> FProducerId) or (Q.SeqProducerEpoch <> FProducerEpoch);
end;

procedure TKafkaProducer.SetProducer(AId: Int64; AEpoch: Int16);
begin
  FMonitor.Enter;
  try
    FProducerId := AId;
    FProducerEpoch := AEpoch;
  finally
    FMonitor.Leave;
  end;
end;

procedure TKafkaProducer.StepInitProducerId(ANow: UInt64; var ANext: UInt64);
var
  LRequest: TKafkaInitProducerIdRequest;
  LResponse: TKafkaInitProducerIdResponse;
  LBroker: IKafkaBrokerConnection;
  LVersion: Int16;
  LFuture: IKafkaResponseFuture;
begin
  if FProducerId <> KAFKA_NO_PRODUCER_ID then
    Exit;
  if FInitFuture <> nil then
  begin
    if not FInitFuture.IsDone then
    begin
      if ANow < FInitSentAt + FRequestTimeoutMs then
      begin
        MinTo(ANext, FInitSentAt + FRequestTimeoutMs);
        Exit;
      end;
      // Fails the future with EKafkaTimeout, handled below.
      FInitBroker.Connection.Abandon(FInitFuture);
    end;
    LFuture := FInitFuture;
    FInitFuture := nil;
    FInitBroker := nil;
    FInitAt := ANow + FRetryBackoffMs;
    try
      LResponse.FromBytes(LFuture.Body, FInitVersion);
    except
      on E: EKafka do
      begin
        // No answer (a broken connection, a timeout): asked again after the
        // backoff, as Java re-enqueues it.
        if E is EKafkaConnection then
          FCluster.Invalidate;
        MinTo(ANext, FInitAt);
        Exit;
      end;
    end;
    if LResponse.ErrorCode = KAFKA_ERROR_NONE then
    begin
      SetProducer(LResponse.ProducerId, LResponse.ProducerEpoch);
      // A bump asked for while there was no id: the fresh id is the bump.
      FBumpRequired := False;
      RewriteSequences;
      FInitAt := 0;
    end
    else if KafkaErrorIsRetriable(LResponse.ErrorCode) then
      MinTo(ANext, FInitAt) // a broker loading its producer ids (seen at startup)
    else
    begin
      // Java fails the producer for good on most of these (fatal state); here
      // the batches waiting fail with the error and the request is made again
      // after the backoff, as Java does for an authorization error.
      FailUnsequenced(EKafkaBrokerError, LResponse.ErrorCode,
        'InitProducerId: ' + KafkaErrorText(LResponse.ErrorCode));
      MinTo(ANext, FInitAt);
    end;
    Exit;
  end;
  if ANow < FInitAt then
  begin
    MinTo(ANext, FInitAt);
    Exit;
  end;
  try
    // Any broker answers it without a transactional id (Java's least loaded
    // node).
    LBroker := FCluster.AnyBroker;
    LVersion := LBroker.Connection.NegotiatedVersion(KAFKA_API_KEY_INIT_PRODUCER_ID);
    LRequest.Init;
    LRequest.TransactionalIdIsNull := True;
    LRequest.TransactionTimeoutMs := High(Int32);
    FInitFuture := LBroker.Connection.Send(KAFKA_API_KEY_INIT_PRODUCER_ID, LVersion,
      LRequest.ToBytes(LVersion), FWakeSignal);
    FInitBroker := LBroker;
    FInitVersion := LVersion;
    FInitSentAt := ANow;
    FMonitor.Enter;
    try
      Inc(FInitCount);
    finally
      FMonitor.Leave;
    end;
    MinTo(ANext, ANow + FRequestTimeoutMs);
  except
    on E: EKafka do
    begin
      FInitAt := ANow + FRetryBackoffMs;
      MinTo(ANext, FInitAt);
      // A broker without InitProducerId, or credentials refused: waiting
      // won't help the records already queued.
      if (E is EKafkaUnsupportedVersion) or (E is EKafkaAuthentication) then
        FailUnsequenced(EKafkaClass(E.ClassType), 0, E.Message)
      else
        FCluster.Invalidate;
    end;
  end;
end;

procedure TKafkaProducer.FailUnsequenced(AClass: EKafkaClass; ACode: Int16;
  const AMessage: string);
var
  LFailed: TList<TProducerBatch>;
  Q: TPartitionQueue;
  B: TProducerBatch;
  I, K: Integer;
begin
  LFailed := TList<TProducerBatch>.Create;
  try
    FMonitor.Enter;
    try
      for I := 0 to FQueueList.Count - 1 do
      begin
        Q := TPartitionQueue(FQueueList[I]);
        K := 0;
        while K < Q.Batches.Count do
        begin
          B := Q.Batches[K];
          if B.HasSequence then
            Inc(K)
          else
          begin
            Q.Batches.Delete(K);
            B.Closed := True;
            LFailed.Add(B);
            FTransit.Add(B);
          end;
        end;
      end;
    finally
      FMonitor.Leave;
    end;
    for B in LFailed do
      FailBatch(B, AClass, ACode, AMessage);
  finally
    LFailed.Free;
  end;
end;

procedure TKafkaProducer.ResolveSequences;
var
  LQueues: array of TPartitionQueue;
  Q: TPartitionQueue;
  I: Integer;
begin
  LQueues := nil;
  FMonitor.Enter;
  try
    SetLength(LQueues, FQueueList.Count);
    for I := 0 to FQueueList.Count - 1 do
      LQueues[I] := TPartitionQueue(FQueueList[I]);
  finally
    FMonitor.Leave;
  end;
  for Q in LQueues do
    if Q.Unresolved and (Q.Sequenced.Count = 0) then
    begin
      // Every batch after the unknown one is done: if the last acknowledged
      // sequence is the one before the next, the broker had it after all
      // (a batch expired, the ones after it succeeded); otherwise a gap
      // stays, and only a new epoch gets past it.
      if Int64(Q.NextSequence) - Q.LastAckedSequence <> 1 then
        RequestBump(Q);
      Q.Unresolved := False;
    end;
end;

procedure TKafkaProducer.RequestBump(AQueue: TObject);
begin
  FBumpRequired := True;
  if FRewrite.IndexOf(AQueue) < 0 then
    FRewrite.Add(AQueue);
end;

procedure TKafkaProducer.ApplyBump;
begin
  if not FBumpRequired or (FProducerId = KAFKA_NO_PRODUCER_ID) then
    Exit;
  FBumpRequired := False;
  FMonitor.Enter;
  try
    Inc(FEpochBumps);
  finally
    FMonitor.Leave;
  end;
  if FProducerEpoch = High(Int16) then
  begin
    // The epoch is spent: a new id from InitProducerId, then the
    // partitions to renumber are renumbered with it.
    SetProducer(KAFKA_NO_PRODUCER_ID, KAFKA_NO_PRODUCER_EPOCH);
    FInitAt := 0;
    Exit;
  end;
  // Java bumps an idempotent producer's epoch by itself: a broker takes a
  // higher epoch that starts at sequence 0 (docs/codec-captures.md).
  SetProducer(FProducerId, FProducerEpoch + 1);
  RewriteSequences;
end;

procedure TKafkaProducer.RewriteSequences;
var
  I: Integer;
begin
  for I := 0 to FRewrite.Count - 1 do
    StartSequencesAtBeginning(FRewrite[I]);
  FRewrite.Clear;
end;

procedure TKafkaProducer.StartSequencesAtBeginning(AQueue: TObject);
var
  Q: TPartitionQueue;
  B: TProducerBatch;
  LSequence: Int32;
begin
  // Java's TxnPartitionEntry.startSequencesAtBeginning: the batches not done
  // yet take the new id and epoch, numbered from 0 in their order; they are
  // encoded again when next sent.
  Q := TPartitionQueue(AQueue);
  LSequence := 0;
  for B in Q.Sequenced do
  begin
    B.Batch.ProducerId := FProducerId;
    B.Batch.ProducerEpoch := FProducerEpoch;
    B.Batch.BaseSequence := LSequence;
    B.Encoded := nil;
    B.SequenceReset := True;
    LSequence := IncrementSequence(LSequence, B.Count);
  end;
  Q.SeqProducerId := FProducerId;
  Q.SeqProducerEpoch := FProducerEpoch;
  Q.NextSequence := LSequence;
  Q.LastAckedSequence := -1;
  Q.Unresolved := False;
end;

function TKafkaProducer.ShouldStopDrain(AQueue, ABatch: TObject): Boolean;
var
  Q: TPartitionQueue;
  B: TProducerBatch;
begin
  // Java's RecordAccumulator.shouldStopDrainBatchesForPartition.
  Q := TPartitionQueue(AQueue);
  B := TProducerBatch(ABatch);
  if not B.HasSequence then
  begin
    // A new batch doesn't start a new epoch's sequence 0 while batches of
    // the old one are still out, nor go while a sequence is unresolved.
    if (Q.Sequenced.Count > 0) and IsStale(Q) then
      Exit(True);
    if Q.Unresolved then
      Exit(True);
  end;
  // A retried batch goes only when it is the first not done: retries go one
  // at a time, in order.
  Result := B.HasSequence and (Q.Sequenced.Count > 0) and
    (Q.Sequenced[0].Batch.BaseSequence <> B.Batch.BaseSequence);
end;

procedure TKafkaProducer.AssignSequence(AQueue, ABatch: TObject);
var
  Q: TPartitionQueue;
  B: TProducerBatch;
begin
  Q := TPartitionQueue(AQueue);
  B := TProducerBatch(ABatch);
  // A partition whose sequences belong to an older id or epoch starts again
  // at 0 once nothing of the old one is out (Java's
  // maybeUpdateProducerIdAndEpoch; ShouldStopDrain waited for that).
  if IsStale(Q) then
    StartSequencesAtBeginning(Q);
  B.Batch.ProducerId := FProducerId;
  B.Batch.ProducerEpoch := FProducerEpoch;
  B.Batch.BaseSequence := Q.NextSequence;
  B.Encoded := nil;
  Q.NextSequence := IncrementSequence(Q.NextSequence, B.Count);
  Q.Sequenced.Add(B);
end;

function TKafkaProducer.IdempotentCanRetry(ABatch: TObject; ACode: Int16;
  ALogStartOffset: Int64): Boolean;
var
  B: TProducerBatch;
  Q: TPartitionQueue;
begin
  // Java's TransactionManager.canRetry, without its transactional branches.
  B := TProducerBatch(ABatch);
  Q := TPartitionQueue(QueueOf(B));
  if ACode = KAFKA_ERROR_UNKNOWN_PRODUCER_ID then
  begin
    // The broker lost the producer's state. Retried in every case; with a
    // new epoch unless the answer has no log start yet or the batch was
    // renumbered already.
    if (ALogStartOffset <> -1) and not B.SequenceReset then
      RequestBump(Q);
    Exit(True);
  end;
  if ACode = KAFKA_ERROR_OUT_OF_ORDER_SEQUENCE_NUMBER then
  begin
    // Not the next batch: one before it failed, and this one is retried in
    // its turn. The next batch itself: a gap the broker won't get past
    // without a new epoch (unless an unresolved sequence may still explain
    // it).
    if not Q.Unresolved and (B.SequenceReset or
      (Int64(B.Batch.BaseSequence) - Q.LastAckedSequence <> 1)) then
      Exit(True);
    if not Q.Unresolved or (B.Batch.BaseSequence = Q.UnresolvedNext) then
      RequestBump(Q);
    Exit(True);
  end;
  Result := KafkaErrorIsRetriable(ACode);
end;

procedure TKafkaProducer.BatchAcked(ABatch: TObject; ABaseOffset: Int64);
var
  B: TProducerBatch;
  Q: TPartitionQueue;
begin
  B := TProducerBatch(ABatch);
  Q := TPartitionQueue(QueueOf(B));
  if (Q = nil) or not B.HasSequence then
    Exit;
  if B.LastSequence > Q.LastAckedSequence then
    Q.LastAckedSequence := B.LastSequence;
  if ABaseOffset >= 0 then
    Q.LastAckedOffset := ABaseOffset + B.Count - 1;
  Q.Sequenced.Remove(B);
end;

procedure TKafkaProducer.BatchFailed(ABatch: TObject; ACode: Int16; AAdjust: Boolean);
var
  B: TProducerBatch;
  Q: TPartitionQueue;
begin
  // Java's TransactionManager.handleFailedBatch for an idempotent producer:
  // the sequence was used and nothing will fill it, so the partition needs a
  // new epoch; not when the batch ran out of retries (AAdjust False): the
  // broker may have it, and the next batch's answer says.
  B := TProducerBatch(ABatch);
  Q := TPartitionQueue(QueueOf(B));
  if (Q = nil) or not B.HasSequence then
    Exit;
  Q.Sequenced.Remove(B);
  if (ACode = KAFKA_ERROR_OUT_OF_ORDER_SEQUENCE_NUMBER) or
    (ACode = KAFKA_ERROR_UNKNOWN_PRODUCER_ID) or AAdjust then
    RequestBump(Q);
end;

procedure TKafkaProducer.MarkUnresolved(ABatch: TObject);
var
  B: TProducerBatch;
  Q: TPartitionQueue;
  LNext: Int32;
begin
  B := TProducerBatch(ABatch);
  Q := TPartitionQueue(QueueOf(B));
  if Q = nil then
    Exit;
  LNext := IncrementSequence(B.LastSequence, 1);
  if not Q.Unresolved or (LNext > Q.UnresolvedNext) then
    Q.UnresolvedNext := LNext;
  Q.Unresolved := True;
  Q.Sequenced.Remove(B);
end;

end.
