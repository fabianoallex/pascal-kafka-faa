unit Kafka.Consumer;

{$I kafka.inc}

{ The consumer: Poll returns the records of its partitions, on the caller's
  thread, as Java's KafkaConsumer does. The partitions come from Assign
  (chosen by the caller) or from Subscribe (given by a consumer group,
  KIP-848); the two are exclusive, as in Java.

  Positions. Each assigned partition has a position: the offset of the next
  record Poll returns. With a GroupId, a new partition starts from the
  group's committed offset (OffsetFetch); without one, or with none
  committed, it is resolved from AutoOffsetReset (Java's auto.offset.reset:
  latest by default, earliest, or none, which raises). Seek sets one;
  SeekToBeginning and SeekToEnd are resolved at the next Poll or Position,
  as in Java. A Fetch answered with OFFSET_OUT_OF_RANGE (the position fell
  off the log, or past its end) resets the position from AutoOffsetReset.
  Reading the committed offsets blocks the Poll that needs them (up to the
  request timeout; Java reads them on its background thread).

  The group (GroupId, Subscribe). Subscribe only records the topics; the
  first Poll joins (Java 4.x does the same). The membership is
  Kafka.Group's IKafkaGroupMembership, KIP-848's TKafkaConsumerProtocol
  (GroupProtocol consumer, the one accepted: classic raises until it
  exists, docs/design.md decision 4). Its events reach the consumer inside
  Poll, on the caller's thread, and the consumer applies them before the
  user's handler sees them: an assigned partition is added (its position
  from the commit), a revoked or lost one stops being fetched and what was
  buffered for it is dropped. OnPartitionsRevoked runs after the
  auto-commit and before the partitions go; the coordinator gives them to
  another member only after that, so a commit made in it lands first.
  OnPartitionsLost (the member is out of the group; the partitions may
  have another owner already: don't commit) falls back to
  OnPartitionsRevoked when unset, as Java's default onPartitionsLost.
  A handler may call Commit, CommitAsync, Seek, Position, Committed and
  the like; Poll, Close, Subscribe, Unsubscribe and Assign raise there.

  Commits. Commit (synchronous, retried until DefaultApiTimeoutMs) and
  CommitAsync (sent from the membership's thread, once; the outcome
  through an "of object" callback, run inside a later Poll, Commit or
  Close, and a future that any thread may wait on), each with the
  consumed positions or offsets given. A synchronous commit first waits for
  the asynchronous ones queued before it and runs their callbacks (Java's
  commitSync), so a late one never overwrites a newer commit. With
  EnableAutoCommit (KafkaConsumerParams' default, as Java's) the consumed
  positions are committed asynchronously every AutoCommitIntervalMs inside
  Poll (only when they changed since the last commit: Java sends them
  again anyway), synchronously before a revoke, and at Close. A position is
  "consumed" once Poll returned the record before it: commits happen at
  the start of a Poll or in Close, after the caller processed what the
  previous Poll returned, which makes it at-least-once.

  Close. The auto-commit, the asynchronous commits waited for, the leave
  (OnPartitionsRevoked for what is held), the membership closed, then the
  connections; within its timeout (30 s, Java's), each step's failure
  swallowed so that the next still runs.

  max.poll.interval.ms (MaxPollIntervalMs) is the membership's rebalance
  timeout: a member not polled that long leaves the group, and the next
  Poll hands out the Lost event and joins again (Kafka.Group.Consumer).

  Poll. Returns at once with what is already fetched (at most
  MaxPollRecords); otherwise sends a Fetch to the leader of each partition
  that has nothing waiting and waits, up to the timeout, for an answer.
  Before returning records it sends the next fetches, so they travel while
  the caller processes (Java's pipelining). One Fetch at a time per broker.
  Each Fetch is a full one (no fetch session, KIP-227: SessionEpoch -1); the
  partitions are listed in a rotating order so that a partition behind a
  busy one isn't starved by FetchMaxBytes. Records come back in each
  partition's order; control batches (transaction markers) are skipped, and
  the records of a compressed batch before the position are dropped.
  Isolation is read_uncommitted, Java's default (read_committed is K4's,
  with transactions).

  Errors. A retriable error (a leader that moved, a broker that went away, a
  request that timed out) is retried by a later Poll, after a metadata
  refresh when the error calls for one and after RetryBackoffMs for a
  broker that failed. Any other error raises from Poll (EKafkaBrokerError
  with its code: TOPIC_AUTHORIZATION_FAILED, for instance), and so do bytes
  that don't decode (EKafkaProtocol: a CRC mismatch) and AutoOffsetReset
  none with no position.

  Threads. The consumer is not thread-safe, as Java's: one thread polls. A
  second thread inside a method raises EKafka at once rather than corrupting
  state (the polling thread may come back in from its own handlers).
  Wakeup is the exception: any thread may call it, and the Poll in
  progress (or the next one) raises EKafkaWakeup, the way to stop a polling
  loop from elsewhere. Close is called from the polling thread (or after
  it stopped).

  Tracing and metrics (Kafka.Tracing, Kafka.Metrics). With the tracer on,
  Poll is a "poll <topic>" span, the current one while it runs, exported
  only when records come back; StartProcessSpan opens the span of
  processing one record, a child of its traceparent, around the
  application's own code. The metrics: the records Poll returned (Drain),
  each partition's lag (the last Fetch answer's high watermark minus the
  position, given back when the partition goes), the commits by kind and
  outcome, the rebalance events.

  Delphi x FPC: the handlers are "of object" methods (no closures on FPC
  3.2.2); the wait is the connection's IKafkaSignal (Kafka.Connection), one
  event for every Fetch in flight, which the membership fires too when an
  event or a commit's outcome waits. }

interface

uses
  SysUtils,
  Classes,
  Generics.Collections,
  Kafka.Types,
  Kafka.Protocol.Primitives,
  Kafka.Protocol.RecordBatch,
  Kafka.Connection,
  Kafka.Cluster,
  PascalCommon.Tracing,
  PascalCommon.Metrics,
  Kafka.Group,
  Kafka.Group.Consumer;

const
  KAFKA_DEFAULT_FETCH_MIN_BYTES = 1;
  KAFKA_DEFAULT_FETCH_MAX_WAIT_MS = 500;
  KAFKA_DEFAULT_FETCH_MAX_BYTES = 52428800;
  KAFKA_DEFAULT_MAX_PARTITION_FETCH_BYTES = 1048576;
  KAFKA_DEFAULT_MAX_POLL_RECORDS = 500;
  /// What one fetched batch may decompress to: a bound against a hostile
  /// or corrupt batch, far above what a producer's batch holds.
  KAFKA_MAX_DECOMPRESSED_BATCH_BYTES = 256 * 1024 * 1024;
  KAFKA_DEFAULT_AUTO_COMMIT_INTERVAL_MS = 5000;
  /// Java's default.api.timeout.ms: Commit and Committed.
  KAFKA_DEFAULT_API_TIMEOUT_MS = 60000;
  /// Java's KafkaConsumer.close() without a timeout.
  KAFKA_DEFAULT_CONSUMER_CLOSE_TIMEOUT_MS = 30000;

type
  /// Java's auto.offset.reset; the zero (latest) is Java's default.
  TKafkaOffsetReset = (korLatest, korEarliest, korNone);

  /// Java's group.protocol: consumer is KIP-848 (Kafka 4.0 and later);
  /// classic (JoinGroup/SyncGroup) doesn't exist yet and is refused.
  TKafkaGroupProtocol = (kgpConsumer, kgpClassic);

  TKafkaConsumer = class;

  /// OnPartitionsAssigned, OnPartitionsRevoked, OnPartitionsLost: on the
  /// polling thread, inside Poll (or Close, Unsubscribe).
  TKafkaPartitionsEvent = procedure(ASender: TKafkaConsumer;
    const APartitions: TKafkaTopicPartitionArray) of object;

  TKafkaTimestampType = (kttCreateTime, kttLogAppendTime);

  TKafkaConsumerRecord = record
    Topic: string;
    Partition: Int32;
    Offset: Int64;
    Timestamp: Int64;
    TimestampType: TKafkaTimestampType;
    /// The batch's partition leader epoch (-1 when the producer set none).
    LeaderEpoch: Int32;
    Key: TBytes;
    KeyIsNull: Boolean;
    Value: TBytes;
    ValueIsNull: Boolean;
    Headers: TKafkaRecordHeaderArray;
  end;
  TKafkaConsumerRecordArray = array of TKafkaConsumerRecord;

  { Start from KafkaConsumerParams: Java's defaults. A field at 0 takes its
    default too, except FetchMaxWaitMs, where 0 means 0. }
  TKafkaConsumerParams = record
    /// The bootstrap, TLS, client id, timeouts (MaxBlockMs bounds Position,
    /// BeginningOffsets and EndOffsets; RetryBackoffMs; the connections'
    /// RequestTimeoutMs).
    Cluster: TKafkaClusterParams;
    AutoOffsetReset: TKafkaOffsetReset;
    /// The bytes a broker waits for before answering a Fetch...
    FetchMinBytes: Integer;
    /// ...or this long, whichever comes first.
    FetchMaxWaitMs: Cardinal;
    /// One Fetch answer's bytes (a broker returns at least one batch).
    FetchMaxBytes: Integer;
    /// One partition's bytes in a Fetch answer.
    MaxPartitionFetchBytes: Integer;
    /// The records one Poll returns at most.
    MaxPollRecords: Integer;
    /// Java's group.id: '' for none (Assign only, nothing committed).
    GroupId: string;
    GroupProtocol: TKafkaGroupProtocol;
    /// Java's enable.auto.commit: True in KafkaConsumerParams. Only with a
    /// GroupId.
    EnableAutoCommit: Boolean;
    /// 0 = KAFKA_DEFAULT_AUTO_COMMIT_INTERVAL_MS.
    AutoCommitIntervalMs: Cardinal;
    /// Java's max.poll.interval.ms, the member's rebalance timeout.
    /// 0 = KAFKA_DEFAULT_MAX_POLL_INTERVAL_MS.
    MaxPollIntervalMs: Cardinal;
    /// Java's default.api.timeout.ms: Commit and Committed without a
    /// timeout of their own. 0 = KAFKA_DEFAULT_API_TIMEOUT_MS.
    DefaultApiTimeoutMs: Cardinal;
    /// Java's retry.backoff.max.ms, for the group's retries.
    /// 0 = KAFKA_DEFAULT_RETRY_BACKOFF_MAX_MS.
    RetryBackoffMaxMs: Cardinal;
    /// Java's group.remote.assignor: '' lets the broker choose (uniform).
    GroupRemoteAssignor: string;
    /// Java's client.rack, sent when joining ('' sends none).
    ClientRack: string;
  end;

  TKafkaConsumer = class
  private type
    TResetKind = (rkNone, rkEarliest, rkLatest);

    TPartitionState = class
    public
      Topic: string;
      Partition: Int32;
      /// -1: none yet (AutoOffsetReset decides).
      Position: Int64;
      Reset: TResetKind;
      Buffer: TKafkaConsumerRecordArray;
      BufferIndex: Integer;
      /// The offset after the buffer's last batch (past compacted gaps and
      /// control batches).
      BufferNext: Int64;
      HasBuffer: Boolean;
      InFlight: Boolean;
      /// After an error that calls for fresh metadata: not fetched again
      /// until the cluster's RefreshCount passes this (-1: no wait).
      WaitRefreshAfter: Integer;
      /// The position comes from the group's committed offset (asked for
      /// before AutoOffsetReset decides).
      NeedsCommitted: Boolean;
      /// The leader epoch of the record before Position (-1: unknown).
      LastEpoch: Int32;
      /// The last Fetch answer's high watermark (-1: none yet).
      HighWatermark: Int64;
      /// The partition's series (Kafka.Metrics), owned by the registry, and
      /// the lag this state added to its series.
      ConsumedSeries: TPcCounterSeries;
      LagSeries: TPcUpDownCounterSeries;
      LagReported: Int64;
      /// Gives back the lag it reported.
      destructor Destroy; override;
      procedure ClearBuffer;
      /// The lag's series brought to HighWatermark - Position.
      procedure UpdateLag;
    end;

    TAsyncCommit = record
      Future: IKafkaCommitFuture;
      Callback: TKafkaCommitEvent;
      /// 'async' or 'auto', for the commits' metric.
      Kind: string;
      /// The offsets' key (OffsetsKey), for the auto-commit's bookkeeping.
      Key: string;
    end;
    TAsyncCommitArray = array of TAsyncCommit;

    TFetchFlight = class
    public
      NodeId: Int32;
      Broker: IKafkaBrokerConnection;
      Future: IKafkaResponseFuture;
      Version: Int16;
      Keys: TKafkaStringArray;
      Topics: TKafkaStringArray;
      TopicIds: TKafkaUuidArray;
      Partitions: TKafkaInt32Array;
      Offsets: TKafkaInt64Array;
      Deadline: UInt64;
    end;

    TOffsetQuery = record
      Topic: string;
      Partition: Int32;
      /// ListOffsets' timestamp: -2 earliest, -1 latest.
      Timestamp: Int64;
      Offset: Int64;
      Done: Boolean;
    end;
    TOffsetQueryArray = array of TOffsetQuery;
  private
    FParams: TKafkaConsumerParams;
    FRequestTimeoutMs: Cardinal;
    FMaxBlockMs: Cardinal;
    FRetryBackoffMs: Cardinal;
    FCluster: TKafkaCluster;
    FStates: TObjectDictionary<string, TPartitionState>;
    FOrder: TList<TPartitionState>;
    FFlights: TObjectList<TFetchFlight>;
    FNodeBackoff: TDictionary<Int32, UInt64>;
    FSignal: TKafkaEventSignal;
    FSignalRef: IKafkaSignal;
    FInUse: Integer;
    FWakeup: Integer;
    FOpened: Boolean;
    FClosed: Boolean;
    FFetchStart: Integer;
    FDrainStart: Integer;
    FNextMetadataAt: UInt64;
    FFetchesSent: Integer;
    // Reentry from the handlers, on the polling thread.
    FOwnerThread: TThreadID;
    FInCallback: Integer;
    FReentry: Integer;
    // The group.
    FApiTimeoutMs: Cardinal;
    FAutoCommitMs: Cardinal;
    FMaxPollIntervalMs: Cardinal;
    FAutoCommitEnabled: Boolean;
    FGroup: TKafkaConsumerProtocol;
    FMembership: IKafkaGroupMembership;
    FSubscription: TKafkaStringArray;
    FSubscribed: Boolean;
    FJoinPending: Boolean;
    FLeaving: Boolean;
    FAsync: TAsyncCommitArray;
    FAutoCommit: IKafkaCommitFuture;
    FCommittedKey: string;
    FNextAutoCommitAt: UInt64;
    FCommittedRetryAt: UInt64;
    FOnAssigned: TKafkaPartitionsEvent;
    FOnRevoked: TKafkaPartitionsEvent;
    FOnLost: TKafkaPartitionsEvent;
    procedure Acquire;
    /// Acquire, refused inside a handler (Poll, Close, Assign...).
    procedure AcquireOuter(const AWhat: string);
    procedure Release;
    procedure EnsureMembership;
    procedure RequireGroup(const AWhat: string);
    function NewState(const ATopic: string; APartition: Int32): TPartitionState;
    procedure AddPartitions(const APartitions: TKafkaTopicPartitionArray);
    procedure RemovePartitions(const APartitions: TKafkaTopicPartitionArray);
    procedure CallPartitions(AEvent: TKafkaPartitionsEvent;
      const APartitions: TKafkaTopicPartitionArray);
    procedure HandleGroupEvent(AKind: TKafkaGroupEventKind;
      const APartitions: TKafkaTopicPartitionArray);
    function ConsumedOffsets: TKafkaCommittedOffsetArray;
    function WaitAsyncCommits(ADeadline: UInt64): Boolean;
    procedure RunCommitCallbacks;
    procedure CommitSync(const AOffsets: TKafkaCommittedOffsetArray; ADeadline: UInt64;
      const AKind: string);
    function QueueAsync(const AOffsets: TKafkaCommittedOffsetArray;
      ACallback: TKafkaCommitEvent; const AKind: string): IKafkaCommitFuture;
    procedure CountCommit(const AKind, AErrorType: string);
    procedure CountRebalance(const AEvent: string);
    procedure GroupStep(ANow: UInt64);
    procedure ResolveCommitted;
    procedure LeaveGroup(ADeadline: UInt64);
    procedure DoUnsubscribe;
    procedure CheckOpen;
    procedure CheckWakeup;
    function StateOf(const ATopic: string; APartition: Int32): TPartitionState;
    procedure MaybeRefreshMetadata(ANow: UInt64);
    function NodeAvailable(ANodeId: Int32; ANow: UInt64): Boolean;
    procedure BackOffNode(ANodeId: Int32);
    procedure QueryOffsetsOnce(var AItems: TOffsetQueryArray);
    function QueryOffsets(const APartitions: array of TKafkaTopicPartition;
      ATimestamp: Int64): TKafkaInt64Array;
    procedure ResolvePositions;
    procedure CollectFetches(ANow: UInt64);
    procedure HandleFetch(AFlight: TFetchFlight);
    procedure SendFetches(ANow: UInt64);
    function Drain: TKafkaConsumerRecordArray;
    /// Poll without its span.
    function PollRecords(ATimeoutMs: Cardinal): TKafkaConsumerRecordArray;
    procedure SeekTo(const APartitions: array of TKafkaTopicPartition; AKind: TResetKind);
  public
    /// Checks the parameters; no I/O.
    constructor Create(const AParams: TKafkaConsumerParams);
    /// Close, then frees.
    destructor Destroy; override;
    /// The cluster's bootstrap. Once only.
    procedure Open;
    /// With a group: the auto-commit, the asynchronous commits, the leave
    /// (OnPartitionsRevoked) and the membership, within ATimeoutMs, their
    /// failures swallowed; then the fetches in flight are abandoned and the
    /// cluster closed. Idempotent.
    procedure Close(ATimeoutMs: Cardinal = KAFKA_DEFAULT_CONSUMER_CLOSE_TIMEOUT_MS);

    /// The partitions to read, replacing the current ones. A partition kept
    /// keeps its position; a new one has none yet (with a GroupId, the
    /// committed offset). No I/O. Refused while subscribed, except Assign([]),
    /// which is Unsubscribe (Java's).
    procedure Assign(const APartitions: array of TKafkaTopicPartition);
    /// The topics to read through the group (GroupId): replaces the
    /// subscription; the next Poll joins, or sends the change. Refused while
    /// partitions are assigned by hand.
    procedure Subscribe(const ATopics: array of string);
    /// Leaves the group (the auto-commit first, then OnPartitionsRevoked)
    /// and drops the subscription; Subscribe or Assign may follow.
    procedure Unsubscribe;
    function Subscription: TKafkaStringArray;
    /// The partitions Poll reads: Assign's, or what the group gave.
    function Assignment: TKafkaTopicPartitionArray;

    /// Commits the consumed positions of every assigned partition and
    /// waits (up to ATimeoutMs; 0 = DefaultApiTimeoutMs), after the
    /// asynchronous commits queued before. EKafkaTimeout, or
    /// EKafkaBrokerError for what a retry doesn't cure.
    procedure Commit(ATimeoutMs: Cardinal = 0);
    /// Commits AOffsets (Offset: the next record to read), as Commit.
    procedure CommitOffsets(const AOffsets: TKafkaCommittedOffsetArray; ATimeoutMs: Cardinal = 0);
    /// The consumed positions, committed without waiting: ACallback (on
    /// the polling thread, in a later Poll, Commit or Close) and the future
    /// report the outcome. Not retried.
    function CommitAsync(ACallback: TKafkaCommitEvent = nil): IKafkaCommitFuture;
    function CommitOffsetsAsync(const AOffsets: TKafkaCommittedOffsetArray;
      ACallback: TKafkaCommitEvent = nil): IKafkaCommitFuture;
    /// The group's committed offsets for APartitions, in their order (-1:
    /// none). 0 = DefaultApiTimeoutMs.
    function Committed(const APartitions: array of TKafkaTopicPartition;
      ATimeoutMs: Cardinal = 0): TKafkaCommittedOffsetArray;
    /// The next Poll returns ATopic-APartition's records from AOffset.
    procedure Seek(const ATopic: string; APartition: Int32; AOffset: Int64);
    /// The partitions' first offsets, resolved at the next Poll or Position
    /// (none given: every assigned partition).
    procedure SeekToBeginning(const APartitions: array of TKafkaTopicPartition);
    /// The partitions' ends: only records written from now on.
    procedure SeekToEnd(const APartitions: array of TKafkaTopicPartition);
    /// The offset of the next record Poll returns for the partition,
    /// resolving it (up to MaxBlockMs) when it isn't known yet.
    function Position(const ATopic: string; APartition: Int32): Int64;
    /// The records fetched (unit's header), waiting up to ATimeoutMs when
    /// none are; empty when none came.
    function Poll(ATimeoutMs: Cardinal): TKafkaConsumerRecordArray;
    /// Makes the Poll in progress, or the next one, raise EKafkaWakeup. Any
    /// thread.
    procedure Wakeup;
    /// The span of processing ARecord (Kafka.Tracing): kind consumer, a
    /// child of the record's traceparent, with this consumer's group and
    /// client id; the current span of the calling thread until
    /// KafkaFinishSpan ends it there. nil when the tracer is off.
    function StartProcessSpan(const ARecord: TKafkaConsumerRecord): IPcSpan;

    /// Each partition's first offset (ListOffsets), up to MaxBlockMs.
    function BeginningOffsets(const APartitions: array of TKafkaTopicPartition): TKafkaInt64Array;
    /// Each partition's end: the offset the next record written will get.
    function EndOffsets(const APartitions: array of TKafkaTopicPartition): TKafkaInt64Array;
    /// The topic's partitions, waiting up to MaxBlockMs for its metadata.
    function PartitionsFor(const ATopic: string): TKafkaPartitionInfoArray;
    /// Fetch requests sent since Open (for tests and diagnostics).
    function FetchesSent: Integer;
    property Cluster: TKafkaCluster read FCluster;
    property Params: TKafkaConsumerParams read FParams;
    /// The group's membership (nil without a GroupId, or before Open): its
    /// member id, epoch and state, for diagnostics and tests.
    property Group: TKafkaConsumerProtocol read FGroup;
    property OnPartitionsAssigned: TKafkaPartitionsEvent read FOnAssigned write FOnAssigned;
    property OnPartitionsRevoked: TKafkaPartitionsEvent read FOnRevoked write FOnRevoked;
    property OnPartitionsLost: TKafkaPartitionsEvent read FOnLost write FOnLost;
  end;

/// Java's defaults, with ABootstrapServers.
function KafkaConsumerParams(const ABootstrapServers: string): TKafkaConsumerParams;

implementation

uses
  PascalCommon.Threading,
  Kafka.Errors,
  Kafka.Messages.Apis,
  Kafka.Messages.Fetch,
  Kafka.Messages.ListOffsets,
  Kafka.Tracing,
  Kafka.Metrics;

const
  LIST_OFFSETS_EARLIEST = -2;
  LIST_OFFSETS_LATEST = -1;

function KafkaConsumerParams(const ABootstrapServers: string): TKafkaConsumerParams;
begin
  Result := Default(TKafkaConsumerParams);
  Result.Cluster := KafkaClusterParams(ABootstrapServers);
  // A consumer doesn't create the topics it reads (Java's
  // allow.auto.create.topics is true, but only with a group subscription).
  Result.Cluster.AllowAutoTopicCreation := False;
  Result.AutoOffsetReset := korLatest;
  Result.FetchMinBytes := KAFKA_DEFAULT_FETCH_MIN_BYTES;
  Result.FetchMaxWaitMs := KAFKA_DEFAULT_FETCH_MAX_WAIT_MS;
  Result.FetchMaxBytes := KAFKA_DEFAULT_FETCH_MAX_BYTES;
  Result.MaxPartitionFetchBytes := KAFKA_DEFAULT_MAX_PARTITION_FETCH_BYTES;
  Result.MaxPollRecords := KAFKA_DEFAULT_MAX_POLL_RECORDS;
  Result.GroupProtocol := kgpConsumer;
  Result.EnableAutoCommit := True;
  Result.AutoCommitIntervalMs := KAFKA_DEFAULT_AUTO_COMMIT_INTERVAL_MS;
  Result.MaxPollIntervalMs := KAFKA_DEFAULT_MAX_POLL_INTERVAL_MS;
  Result.DefaultApiTimeoutMs := KAFKA_DEFAULT_API_TIMEOUT_MS;
end;

/// The offsets as one string, to tell whether they changed.
function OffsetsKey(const AOffsets: TKafkaCommittedOffsetArray): string;
var
  I: Integer;
begin
  Result := '';
  for I := 0 to High(AOffsets) do
    Result := Result + AOffsets[I].Topic + #0 + IntToStr(AOffsets[I].Partition) + #0 +
      IntToStr(AOffsets[I].Offset) + #1;
end;

function RemainingUntil(ADeadline: UInt64): Cardinal;
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

function KeyOf(const ATopic: string; APartition: Int32): string;
begin
  Result := ATopic + #0 + IntToStr(APartition);
end;

{ TKafkaConsumer.TPartitionState }

destructor TKafkaConsumer.TPartitionState.Destroy;
begin
  KafkaMetricAdd(LagSeries, -LagReported);
  inherited;
end;

procedure TKafkaConsumer.TPartitionState.ClearBuffer;
begin
  Buffer := nil;
  BufferIndex := 0;
  BufferNext := -1;
  HasBuffer := False;
end;

procedure TKafkaConsumer.TPartitionState.UpdateLag;
var
  LLag: Int64;
begin
  if (HighWatermark < 0) or (Position < 0) or (HighWatermark < Position) then
    LLag := 0
  else
    LLag := HighWatermark - Position;
  if LLag <> LagReported then
  begin
    KafkaMetricAdd(LagSeries, LLag - LagReported);
    LagReported := LLag;
  end;
end;

{ TKafkaConsumer }

constructor TKafkaConsumer.Create(const AParams: TKafkaConsumerParams);
begin
  inherited Create;
  if AParams.GroupProtocol = kgpClassic then
    raise EKafka.Create('GroupProtocol classic (JoinGroup/SyncGroup, for brokers before ' +
      'Kafka 4.0) doesn''t exist yet in this library; use consumer (KIP-848)');
  if AParams.MaxPollIntervalMs > Cardinal(High(Int32)) then
    raise EArgumentException.CreateFmt('MaxPollIntervalMs %d is too large',
      [AParams.MaxPollIntervalMs]);
  FParams := AParams;
  FAutoCommitMs := FParams.AutoCommitIntervalMs;
  if FAutoCommitMs = 0 then
    FAutoCommitMs := KAFKA_DEFAULT_AUTO_COMMIT_INTERVAL_MS;
  FMaxPollIntervalMs := FParams.MaxPollIntervalMs;
  if FMaxPollIntervalMs = 0 then
    FMaxPollIntervalMs := KAFKA_DEFAULT_MAX_POLL_INTERVAL_MS;
  FApiTimeoutMs := FParams.DefaultApiTimeoutMs;
  if FApiTimeoutMs = 0 then
    FApiTimeoutMs := KAFKA_DEFAULT_API_TIMEOUT_MS;
  FAutoCommitEnabled := FParams.EnableAutoCommit and (FParams.GroupId <> '');
  if FParams.FetchMinBytes <= 0 then
    FParams.FetchMinBytes := KAFKA_DEFAULT_FETCH_MIN_BYTES;
  if FParams.FetchMaxBytes <= 0 then
    FParams.FetchMaxBytes := KAFKA_DEFAULT_FETCH_MAX_BYTES;
  if FParams.MaxPartitionFetchBytes <= 0 then
    FParams.MaxPartitionFetchBytes := KAFKA_DEFAULT_MAX_PARTITION_FETCH_BYTES;
  if FParams.MaxPollRecords <= 0 then
    FParams.MaxPollRecords := KAFKA_DEFAULT_MAX_POLL_RECORDS;
  if FParams.FetchMaxWaitMs > Cardinal(High(Int32)) then
    raise EArgumentException.CreateFmt('FetchMaxWaitMs %d is too large', [FParams.FetchMaxWaitMs]);
  FRequestTimeoutMs := FParams.Cluster.Connection.RequestTimeoutMs;
  if FRequestTimeoutMs = 0 then
    FRequestTimeoutMs := KAFKA_DEFAULT_REQUEST_TIMEOUT_MS;
  FMaxBlockMs := FParams.Cluster.MaxBlockMs;
  if FMaxBlockMs = 0 then
    FMaxBlockMs := KAFKA_DEFAULT_MAX_BLOCK_MS;
  FRetryBackoffMs := FParams.Cluster.RetryBackoffMs;
  if FRetryBackoffMs = 0 then
    FRetryBackoffMs := KAFKA_DEFAULT_RETRY_BACKOFF_MS;
  FCluster := TKafkaCluster.Create(FParams.Cluster);
  FStates := TObjectDictionary<string, TPartitionState>.Create([doOwnsValues]);
  FOrder := TList<TPartitionState>.Create;
  FFlights := TObjectList<TFetchFlight>.Create(True);
  FNodeBackoff := TDictionary<Int32, UInt64>.Create;
  FSignal := TKafkaEventSignal.Create;
  FSignalRef := FSignal;
end;

destructor TKafkaConsumer.Destroy;
begin
  if FFlights <> nil then
    try
      Close;
    except
      on Exception do ; // a destructor doesn't raise
    end;
  FAutoCommit := nil;
  FAsync := nil;
  FMembership := nil; // frees the membership (closed already)
  FGroup := nil;
  FSignalRef := nil;
  FNodeBackoff.Free;
  FFlights.Free;
  FOrder.Free;
  FStates.Free;
  FCluster.Free;
  inherited;
end;

procedure TKafkaConsumer.Acquire;
begin
  if PcAtomicCompareExchange(FInUse, 1, 0) <> 0 then
  begin
    // The polling thread, back in from one of its handlers: FOwnerThread
    // can only hold this thread's id if this thread wrote it.
    if (FInCallback > 0) and (FOwnerThread = TThread.CurrentThread.ThreadID) then
    begin
      Inc(FReentry);
      Exit;
    end;
    raise EKafka.Create('The consumer is not safe for multi-threaded access ' +
      '(another thread is inside it; Wakeup is the one method any thread may call)');
  end;
  FOwnerThread := TThread.CurrentThread.ThreadID;
end;

procedure TKafkaConsumer.AcquireOuter(const AWhat: string);
begin
  Acquire;
  if FReentry > 0 then
  begin
    Release;
    raise EKafka.CreateFmt('%s can''t be called from a rebalance or commit handler', [AWhat]);
  end;
end;

procedure TKafkaConsumer.Release;
begin
  if FReentry > 0 then
  begin
    Dec(FReentry);
    Exit;
  end;
  PcAtomicSet(FInUse, 0);
end;

procedure TKafkaConsumer.CheckOpen;
begin
  if FClosed then
    raise EKafka.Create('The consumer is closed');
  if not FOpened then
    raise EKafka.Create('The consumer is not open (call Open)');
end;

procedure TKafkaConsumer.CheckWakeup;
begin
  if PcAtomicSet(FWakeup, 0) <> 0 then
    raise EKafkaWakeup.Create('Wakeup was called');
end;

procedure TKafkaConsumer.Wakeup;
begin
  PcAtomicSet(FWakeup, 1);
  FSignal.Signal;
end;

procedure TKafkaConsumer.Open;
begin
  Acquire;
  try
    if FOpened or FClosed then
      raise EKafka.Create('The consumer was opened already');
    FCluster.Open;
    FOpened := True;
    if FParams.GroupId <> '' then
      EnsureMembership;
    FNextAutoCommitAt := PcTickMs + FAutoCommitMs;
  finally
    Release;
  end;
end;

procedure TKafkaConsumer.Close(ATimeoutMs: Cardinal);
var
  F: TFetchFlight;
  LDeadline: UInt64;
begin
  if FClosed or FLeaving then
    Exit;
  if FInCallback > 0 then
    raise EKafka.Create('Close can''t be called from a rebalance or commit handler');
  if ATimeoutMs = KAFKA_WAIT_INFINITE then
    LDeadline := High(UInt64)
  else
    LDeadline := PcTickMs + ATimeoutMs;
  try
    if FOpened and (FMembership <> nil) then
      try
        LeaveGroup(LDeadline);
      except
        on Exception do ; // Close goes on (the unit's header)
      end;
  finally
    FClosed := True;
    for F in FFlights do
      if F.Future <> nil then
        F.Broker.Connection.Abandon(F.Future);
    FFlights.Clear;
    FCluster.Close;
  end;
end;

{ The group }

procedure TKafkaConsumer.EnsureMembership;
var
  P: TKafkaConsumerProtocolParams;
begin
  if FMembership <> nil then
    Exit;
  P := KafkaConsumerProtocolParams(FParams.GroupId);
  P.RackId := FParams.ClientRack;
  P.ServerAssignor := FParams.GroupRemoteAssignor;
  P.RebalanceTimeoutMs := FMaxPollIntervalMs;
  P.RetryBackoffMs := FRetryBackoffMs;
  P.RetryBackoffMaxMs := FParams.RetryBackoffMaxMs;
  P.Notify := FSignalRef;
  FGroup := TKafkaConsumerProtocol.Create(FCluster, P);
  FMembership := FGroup;
end;

procedure TKafkaConsumer.RequireGroup(const AWhat: string);
begin
  CheckOpen;
  if FParams.GroupId = '' then
    raise EKafka.CreateFmt('%s needs a GroupId (Java''s group.id)', [AWhat]);
  EnsureMembership;
end;

function TKafkaConsumer.NewState(const ATopic: string; APartition: Int32): TPartitionState;
begin
  Result := TPartitionState.Create;
  Result.Topic := ATopic;
  Result.Partition := APartition;
  Result.Position := -1;
  Result.WaitRefreshAfter := -1;
  Result.LastEpoch := -1;
  Result.NeedsCommitted := FParams.GroupId <> '';
  Result.HighWatermark := -1;
  Result.ConsumedSeries := KafkaConsumedMessagesSeries(ATopic, FParams.GroupId);
  Result.LagSeries := KafkaConsumerLagSeries(ATopic, APartition, FParams.GroupId);
  Result.ClearBuffer;
end;

procedure TKafkaConsumer.AddPartitions(const APartitions: TKafkaTopicPartitionArray);
var
  I: Integer;
  K: string;
  S: TPartitionState;
begin
  for I := 0 to High(APartitions) do
  begin
    K := KeyOf(APartitions[I].Topic, APartitions[I].Partition);
    if FStates.ContainsKey(K) then
      Continue;
    S := NewState(APartitions[I].Topic, APartitions[I].Partition);
    FStates.Add(K, S);
    FOrder.Add(S);
  end;
  FFetchStart := 0;
  FDrainStart := 0;
end;

procedure TKafkaConsumer.RemovePartitions(const APartitions: TKafkaTopicPartitionArray);
var
  I: Integer;
  K: string;
  S: TPartitionState;
begin
  // What was buffered goes with the state; a Fetch in flight for it finds
  // no state and its answer is dropped (HandleFetch).
  for I := 0 to High(APartitions) do
  begin
    K := KeyOf(APartitions[I].Topic, APartitions[I].Partition);
    if not FStates.TryGetValue(K, S) then
      Continue;
    FOrder.Remove(S);
    FStates.Remove(K);
  end;
  FFetchStart := 0;
  FDrainStart := 0;
end;

procedure TKafkaConsumer.CallPartitions(AEvent: TKafkaPartitionsEvent;
  const APartitions: TKafkaTopicPartitionArray);
begin
  Inc(FInCallback);
  try
    AEvent(Self, APartitions);
  finally
    Dec(FInCallback);
  end;
end;

procedure TKafkaConsumer.HandleGroupEvent(AKind: TKafkaGroupEventKind;
  const APartitions: TKafkaTopicPartitionArray);
begin
  case AKind of
    kgeAssigned:
      begin
        CountRebalance('assigned');
        AddPartitions(APartitions);
        if Assigned(FOnAssigned) then
          CallPartitions(FOnAssigned, APartitions);
      end;
    kgeRevoked:
      try
        CountRebalance('revoked');
        // Java's maybeAutoCommitSyncBeforeRebalance: every consumed
        // position, before the handler; a failure doesn't stop the revoke
        // (the records may be read again: at-least-once).
        if FAutoCommitEnabled and not FLeaving then
          try
            CommitSync(ConsumedOffsets, PcTickMs + FApiTimeoutMs, 'auto');
          except
            on EKafka do ;
          end;
        if Assigned(FOnRevoked) then
          CallPartitions(FOnRevoked, APartitions);
      finally
        RemovePartitions(APartitions);
      end;
    kgeLost:
      try
        CountRebalance('lost');
        if Assigned(FOnLost) then
          CallPartitions(FOnLost, APartitions)
        else if Assigned(FOnRevoked) then
          CallPartitions(FOnRevoked, APartitions);
      finally
        RemovePartitions(APartitions);
      end;
  end;
end;

function TKafkaConsumer.ConsumedOffsets: TKafkaCommittedOffsetArray;
var
  S: TPartitionState;
  N: Integer;
begin
  Result := nil;
  N := 0;
  for S in FOrder do
    if (S.Position >= 0) and (S.Reset = rkNone) and not S.NeedsCommitted then
    begin
      SetLength(Result, N + 1);
      Result[N].Topic := S.Topic;
      Result[N].Partition := S.Partition;
      Result[N].Offset := S.Position;
      Result[N].LeaderEpoch := S.LastEpoch;
      Result[N].Metadata := '';
      Inc(N);
    end;
end;

function TKafkaConsumer.WaitAsyncCommits(ADeadline: UInt64): Boolean;
var
  I: Integer;
begin
  Result := True;
  for I := 0 to High(FAsync) do
    if not FAsync[I].Future.Wait(RemainingUntil(ADeadline)) then
      Exit(False);
end;

procedure TKafkaConsumer.RunCommitCallbacks;
var
  E: TAsyncCommit;
  R: TKafkaCommitReport;
begin
  // The membership completes them in the order they were queued.
  while (Length(FAsync) > 0) and FAsync[0].Future.IsDone do
  begin
    E := FAsync[0];
    FAsync := Copy(FAsync, 1, Length(FAsync) - 1);
    R := E.Future.Report;
    CountCommit(E.Kind, KafkaErrorType(R.ErrorClass, R.ErrorCode));
    if not R.Succeeded and (FCommittedKey = E.Key) then
      FCommittedKey := ''; // the auto-commit tries again
    if Assigned(E.Callback) then
    begin
      Inc(FInCallback);
      try
        E.Callback(R);
      finally
        Dec(FInCallback);
      end;
    end;
  end;
end;

procedure TKafkaConsumer.CountCommit(const AKind, AErrorType: string);
begin
  KafkaMetricAdd(KafkaCommitsSeries(FParams.GroupId, AKind, AErrorType), 1);
end;

procedure TKafkaConsumer.CountRebalance(const AEvent: string);
begin
  KafkaMetricAdd(KafkaRebalancesSeries(FParams.GroupId, AEvent), 1);
end;

procedure TKafkaConsumer.CommitSync(const AOffsets: TKafkaCommittedOffsetArray;
  ADeadline: UInt64; const AKind: string);
var
  LTimeoutMs: Cardinal;
  LCode: Int16;
begin
  if Length(AOffsets) = 0 then
    Exit;
  LTimeoutMs := RemainingUntil(ADeadline);
  try
    // An asynchronous commit queued before must not land after this one.
    if not WaitAsyncCommits(ADeadline) then
      raise EKafkaTimeout.CreateFmt('The asynchronous commits before this one were not ' +
        'done in %d ms', [LTimeoutMs]);
    RunCommitCallbacks;
    FMembership.Commit(AOffsets, RemainingUntil(ADeadline));
  except
    on E: EKafka do
    begin
      LCode := 0;
      if E is EKafkaBrokerError then
        LCode := EKafkaBrokerError(E).ErrorCode;
      CountCommit(AKind, KafkaErrorType(EKafkaClass(E.ClassType), LCode));
      raise;
    end;
  end;
  CountCommit(AKind, '');
  FCommittedKey := OffsetsKey(AOffsets);
end;

function TKafkaConsumer.QueueAsync(const AOffsets: TKafkaCommittedOffsetArray;
  ACallback: TKafkaCommitEvent; const AKind: string): IKafkaCommitFuture;
var
  N: Integer;
begin
  Result := FMembership.CommitAsync(AOffsets);
  N := Length(FAsync);
  SetLength(FAsync, N + 1);
  FAsync[N].Future := Result;
  FAsync[N].Callback := ACallback;
  FAsync[N].Key := OffsetsKey(AOffsets);
  FAsync[N].Kind := AKind;
end;

procedure TKafkaConsumer.GroupStep(ANow: UInt64);
var
  LOffsets: TKafkaCommittedOffsetArray;
  LKey: string;
begin
  if FMembership = nil then
    Exit;
  if FJoinPending then
  begin
    FJoinPending := False;
    FMembership.Subscribe(FSubscription);
  end;
  FMembership.Poll(HandleGroupEvent);
  RunCommitCallbacks;
  // The auto-commit on its interval (Java's maybeAutoCommitAsync: not
  // while the previous one is in flight).
  if not FAutoCommitEnabled or (ANow < FNextAutoCommitAt) then
    Exit;
  FNextAutoCommitAt := ANow + FAutoCommitMs;
  if (FAutoCommit <> nil) and not FAutoCommit.IsDone then
    Exit;
  LOffsets := ConsumedOffsets;
  LKey := OffsetsKey(LOffsets);
  if (Length(LOffsets) = 0) or (LKey = FCommittedKey) then
    Exit;
  FAutoCommit := QueueAsync(LOffsets, nil, 'auto');
  FCommittedKey := LKey;
end;

procedure TKafkaConsumer.ResolveCommitted;
var
  LParts: TKafkaTopicPartitionArray;
  LGot: TKafkaCommittedOffsetArray;
  S: TPartitionState;
  N, I: Integer;
begin
  LParts := nil;
  N := 0;
  for S in FOrder do
    if S.NeedsCommitted and (S.Reset = rkNone) and (S.Position < 0) then
    begin
      SetLength(LParts, N + 1);
      LParts[N] := KafkaTopicPartition(S.Topic, S.Partition);
      Inc(N);
    end;
  if (N = 0) or (PcTickMs < FCommittedRetryAt) then
    Exit;
  EnsureMembership;
  try
    LGot := FMembership.Committed(LParts, FRequestTimeoutMs);
  except
    on E: EKafka do
    begin
      if (E is EKafkaTimeout) or (E is EKafkaConnection) then
      begin
        FCommittedRetryAt := PcTickMs + FRetryBackoffMs;
        Exit;
      end;
      raise;
    end;
  end;
  for I := 0 to High(LGot) do
    if FStates.TryGetValue(KeyOf(LGot[I].Topic, LGot[I].Partition), S) and S.NeedsCommitted then
    begin
      S.NeedsCommitted := False;
      // None committed: AutoOffsetReset decides (ResolvePositions).
      if (LGot[I].Offset >= 0) and (S.Reset = rkNone) and (S.Position < 0) then
      begin
        S.Position := LGot[I].Offset;
        S.LastEpoch := LGot[I].LeaderEpoch;
        S.ClearBuffer;
      end;
    end;
end;

procedure TKafkaConsumer.LeaveGroup(ADeadline: UInt64);
var
  LMembership: IKafkaGroupMembership;
begin
  if FMembership = nil then
    Exit;
  FLeaving := True;
  try
    // Java's close: the auto-commit, the rebalance handlers, the leave,
    // then the asynchronous commits; here the asynchronous commits come
    // before the leave, while the member can still commit.
    if FAutoCommitEnabled then
      try
        CommitSync(ConsumedOffsets, ADeadline, 'auto');
      except
        on EKafka do ;
      end;
    WaitAsyncCommits(ADeadline);
    try
      RunCommitCallbacks;
    finally
      if FSubscribed then
        FMembership.Leave(HandleGroupEvent, RemainingUntil(ADeadline));
    end;
  finally
    FLeaving := False;
    LMembership := FMembership;
    FMembership := nil;
    FGroup := nil;
    FAutoCommit := nil;
    LMembership.Close;
    // Whatever is still queued failed with the membership's Close.
    FAsync := nil;
  end;
end;

procedure TKafkaConsumer.Subscribe(const ATopics: array of string);
var
  L: TStringList;
  I: Integer;
begin
  if Length(ATopics) = 0 then
    raise EArgumentException.Create('Subscribe needs at least one topic (Unsubscribe for none)');
  for I := 0 to High(ATopics) do
    if ATopics[I] = '' then
      raise EArgumentException.Create('A topic name is empty');
  AcquireOuter('Subscribe');
  try
    RequireGroup('Subscribe');
    if not FSubscribed and (FOrder.Count > 0) then
      raise EKafka.Create('Subscribe and Assign are mutually exclusive (Java: "Subscription ' +
        'to topics, partitions and pattern are mutually exclusive"); call Assign([]) first');
    L := TStringList.Create;
    try
      L.CaseSensitive := True;
      L.Sorted := True;
      L.Duplicates := dupIgnore;
      for I := 0 to High(ATopics) do
        L.Add(ATopics[I]);
      SetLength(FSubscription, L.Count);
      for I := 0 to L.Count - 1 do
        FSubscription[I] := L[I];
    finally
      L.Free;
    end;
    FSubscribed := True;
    // The join, or the change, goes with the next Poll (Java 4.x).
    FJoinPending := True;
  finally
    Release;
  end;
end;

procedure TKafkaConsumer.DoUnsubscribe;
begin
  if not FSubscribed then
    Exit;
  try
    LeaveGroup(PcTickMs + FApiTimeoutMs);
  finally
    FSubscribed := False;
    FJoinPending := False;
    FSubscription := nil;
    FOrder.Clear;
    FStates.Clear;
    FFetchStart := 0;
    FDrainStart := 0;
  end;
end;

procedure TKafkaConsumer.Unsubscribe;
begin
  AcquireOuter('Unsubscribe');
  try
    CheckOpen;
    DoUnsubscribe;
  finally
    Release;
  end;
end;

function TKafkaConsumer.Subscription: TKafkaStringArray;
begin
  Result := Copy(FSubscription, 0, Length(FSubscription));
end;

procedure TKafkaConsumer.Commit(ATimeoutMs: Cardinal);
begin
  Acquire;
  try
    RequireGroup('Commit');
    if ATimeoutMs = 0 then
      ATimeoutMs := FApiTimeoutMs;
    CommitSync(ConsumedOffsets, PcTickMs + ATimeoutMs, 'sync');
  finally
    Release;
  end;
end;

procedure TKafkaConsumer.CommitOffsets(const AOffsets: TKafkaCommittedOffsetArray;
  ATimeoutMs: Cardinal);
begin
  Acquire;
  try
    RequireGroup('Commit');
    if ATimeoutMs = 0 then
      ATimeoutMs := FApiTimeoutMs;
    CommitSync(AOffsets, PcTickMs + ATimeoutMs, 'sync');
  finally
    Release;
  end;
end;

function TKafkaConsumer.CommitAsync(ACallback: TKafkaCommitEvent): IKafkaCommitFuture;
begin
  Acquire;
  try
    RequireGroup('CommitAsync');
    Result := QueueAsync(ConsumedOffsets, ACallback, 'async');
  finally
    Release;
  end;
end;

function TKafkaConsumer.CommitOffsetsAsync(const AOffsets: TKafkaCommittedOffsetArray;
  ACallback: TKafkaCommitEvent): IKafkaCommitFuture;
begin
  Acquire;
  try
    RequireGroup('CommitAsync');
    Result := QueueAsync(AOffsets, ACallback, 'async');
  finally
    Release;
  end;
end;

function TKafkaConsumer.Committed(const APartitions: array of TKafkaTopicPartition;
  ATimeoutMs: Cardinal): TKafkaCommittedOffsetArray;
begin
  Acquire;
  try
    RequireGroup('Committed');
    if ATimeoutMs = 0 then
      ATimeoutMs := FApiTimeoutMs;
    Result := FMembership.Committed(APartitions, ATimeoutMs);
  finally
    Release;
  end;
end;

function TKafkaConsumer.StateOf(const ATopic: string; APartition: Int32): TPartitionState;
begin
  if not FStates.TryGetValue(KeyOf(ATopic, APartition), Result) then
    raise EKafka.CreateFmt('%s-%d is not assigned to this consumer', [ATopic, APartition]);
end;

procedure TKafkaConsumer.Assign(const APartitions: array of TKafkaTopicPartition);
var
  LNew: TObjectDictionary<string, TPartitionState>;
  LOld: TObjectDictionary<string, TPartitionState>;
  S: TPartitionState;
  I: Integer;
  K: string;
begin
  AcquireOuter('Assign');
  try
    if FSubscribed then
    begin
      if Length(APartitions) > 0 then
        raise EKafka.Create('Subscribe and Assign are mutually exclusive: call Unsubscribe ' +
          '(or Assign([])) first');
      CheckOpen;
      DoUnsubscribe;
      Exit;
    end;
    for I := 0 to High(APartitions) do
    begin
      if APartitions[I].Topic = '' then
        raise EArgumentException.Create('A partition needs a topic');
      if APartitions[I].Partition < 0 then
        raise EArgumentException.CreateFmt('Invalid partition %d', [APartitions[I].Partition]);
    end;
    LNew := TObjectDictionary<string, TPartitionState>.Create([doOwnsValues]);
    FOrder.Clear;
    for I := 0 to High(APartitions) do
    begin
      K := KeyOf(APartitions[I].Topic, APartitions[I].Partition);
      if LNew.ContainsKey(K) then
        Continue;
      if FStates.ContainsKey(K) then
        // Kept, with its position and buffer.
        S := FStates.ExtractPair(K).Value
      else
        S := NewState(APartitions[I].Topic, APartitions[I].Partition);
      LNew.Add(K, S);
      FOrder.Add(S);
    end;
    LOld := FStates;
    FStates := LNew;
    LOld.Free;
    FFetchStart := 0;
    FDrainStart := 0;
  finally
    Release;
  end;
end;

function TKafkaConsumer.Assignment: TKafkaTopicPartitionArray;
var
  I: Integer;
begin
  Result := nil;
  SetLength(Result, FOrder.Count);
  for I := 0 to FOrder.Count - 1 do
  begin
    Result[I].Topic := FOrder[I].Topic;
    Result[I].Partition := FOrder[I].Partition;
  end;
end;

procedure TKafkaConsumer.Seek(const ATopic: string; APartition: Int32; AOffset: Int64);
var
  S: TPartitionState;
begin
  if AOffset < 0 then
    raise EArgumentException.CreateFmt('Invalid offset %d', [AOffset]);
  Acquire;
  try
    S := StateOf(ATopic, APartition);
    S.Position := AOffset;
    S.Reset := rkNone;
    S.NeedsCommitted := False;
    S.LastEpoch := -1;
    S.ClearBuffer;
  finally
    Release;
  end;
end;

procedure TKafkaConsumer.SeekTo(const APartitions: array of TKafkaTopicPartition;
  AKind: TResetKind);
var
  S: TPartitionState;
  I: Integer;
begin
  Acquire;
  try
    if Length(APartitions) = 0 then
    begin
      for S in FOrder do
      begin
        S.Reset := AKind;
        S.NeedsCommitted := False;
        S.ClearBuffer;
      end;
      Exit;
    end;
    for I := 0 to High(APartitions) do
      StateOf(APartitions[I].Topic, APartitions[I].Partition);
    for I := 0 to High(APartitions) do
    begin
      S := StateOf(APartitions[I].Topic, APartitions[I].Partition);
      S.Reset := AKind;
      S.NeedsCommitted := False;
      S.ClearBuffer;
    end;
  finally
    Release;
  end;
end;

procedure TKafkaConsumer.SeekToBeginning(const APartitions: array of TKafkaTopicPartition);
begin
  SeekTo(APartitions, rkEarliest);
end;

procedure TKafkaConsumer.SeekToEnd(const APartitions: array of TKafkaTopicPartition);
begin
  SeekTo(APartitions, rkLatest);
end;

function TKafkaConsumer.NodeAvailable(ANodeId: Int32; ANow: UInt64): Boolean;
var
  LUntil: UInt64;
begin
  Result := not (FNodeBackoff.TryGetValue(ANodeId, LUntil) and (LUntil > ANow));
end;

procedure TKafkaConsumer.BackOffNode(ANodeId: Int32);
begin
  FNodeBackoff.AddOrSetValue(ANodeId, PcTickMs + FRetryBackoffMs);
  FCluster.Invalidate;
end;

procedure TKafkaConsumer.MaybeRefreshMetadata(ANow: UInt64);
var
  LMetadata: IKafkaClusterMetadata;
  LMissing: TStringList;
  LTopics: array of string;
  LTopic: TKafkaTopicInfo;
  S: TPartitionState;
  I: Integer;
begin
  if ANow < FNextMetadataAt then
    Exit;
  LMetadata := FCluster.Metadata;
  LTopics := nil;
  LMissing := TStringList.Create;
  try
    LMissing.Sorted := True;
    LMissing.Duplicates := dupIgnore;
    for S in FOrder do
      if (LMetadata = nil) or not LMetadata.FindTopic(S.Topic, LTopic) then
        LMissing.Add(S.Topic);
    if (LMissing.Count = 0) and not FCluster.NeedsRefresh then
      Exit;
    FNextMetadataAt := ANow + FRetryBackoffMs;
    LTopics := nil;
    SetLength(LTopics, LMissing.Count);
    for I := 0 to LMissing.Count - 1 do
      LTopics[I] := LMissing[I];
    try
      FCluster.Refresh(LTopics);
    except
      on EKafka do ; // tried again after the backoff
    end;
  finally
    LMissing.Free;
  end;
end;

procedure TKafkaConsumer.QueryOffsetsOnce(var AItems: TOffsetQueryArray);
var
  LMetadata: IKafkaClusterMetadata;
  LTopic: TKafkaTopicInfo;
  LNodes: TDictionary<Int32, Boolean>;
  LNode, LLeader: Int32;
  LRequest: TKafkaListOffsetsRequest;
  LResponse: TKafkaListOffsetsResponse;
  LBroker: IKafkaBrokerConnection;
  LIdx: TKafkaInt32Array;
  I, T, P, N, K: Integer;
  LNow: UInt64;
  LCode: Int16;
begin
  LMetadata := FCluster.Metadata;
  if LMetadata = nil then
    Exit;
  LNow := PcTickMs;
  LNodes := TDictionary<Int32, Boolean>.Create;
  try
    for I := 0 to High(AItems) do
      if not AItems[I].Done and LMetadata.FindTopic(AItems[I].Topic, LTopic) and
        (AItems[I].Partition < Length(LTopic.Partitions)) and
        (LTopic.Partitions[AItems[I].Partition].LeaderId >= 0) then
        LNodes.AddOrSetValue(LTopic.Partitions[AItems[I].Partition].LeaderId, True);
    for LNode in LNodes.Keys do
    begin
      if not NodeAvailable(LNode, LNow) then
        Continue;
      LRequest.Init;
      LRequest.ReplicaId := -1;
      LRequest.TimeoutMs := FRequestTimeoutMs;
      LIdx := nil;
      for I := 0 to High(AItems) do
      begin
        if AItems[I].Done or not LMetadata.FindTopic(AItems[I].Topic, LTopic) or
          (AItems[I].Partition >= Length(LTopic.Partitions)) then
          Continue;
        LLeader := LTopic.Partitions[AItems[I].Partition].LeaderId;
        if LLeader <> LNode then
          Continue;
        T := -1;
        for K := 0 to High(LRequest.Topics) do
          if LRequest.Topics[K].Name = AItems[I].Topic then
            T := K;
        if T < 0 then
        begin
          T := Length(LRequest.Topics);
          SetLength(LRequest.Topics, T + 1);
          LRequest.Topics[T].Init;
          LRequest.Topics[T].Name := AItems[I].Topic;
        end;
        P := Length(LRequest.Topics[T].Partitions);
        SetLength(LRequest.Topics[T].Partitions, P + 1);
        LRequest.Topics[T].Partitions[P].Init;
        LRequest.Topics[T].Partitions[P].PartitionIndex := AItems[I].Partition;
        LRequest.Topics[T].Partitions[P].Timestamp := AItems[I].Timestamp;
        N := Length(LIdx);
        SetLength(LIdx, N + 1);
        LIdx[N] := I;
      end;
      if Length(LIdx) = 0 then
        Continue;
      try
        LBroker := FCluster.Broker(LNode);
        LResponse := LBroker.Connection.ListOffsets(LRequest);
      except
        on E: EKafka do
        begin
          if (E is EKafkaConnection) or (E is EKafkaTimeout) then
          begin
            BackOffNode(LNode);
            Continue;
          end;
          raise;
        end;
      end;
      for T := 0 to High(LResponse.Topics) do
        for P := 0 to High(LResponse.Topics[T].Partitions) do
          for K := 0 to High(LIdx) do
          begin
            I := LIdx[K];
            if (AItems[I].Topic <> LResponse.Topics[T].Name) or
              (AItems[I].Partition <> LResponse.Topics[T].Partitions[P].PartitionIndex) then
              Continue;
            LCode := LResponse.Topics[T].Partitions[P].ErrorCode;
            if LCode = KAFKA_ERROR_NONE then
            begin
              AItems[I].Offset := LResponse.Topics[T].Partitions[P].Offset;
              AItems[I].Done := True;
            end
            else if KafkaErrorIsRetriable(LCode) then
            begin
              if KafkaErrorIsInvalidMetadata(LCode) then
                FCluster.Invalidate;
            end
            else
              raise EKafkaBrokerError.Create(LCode, Format('ListOffsets %s-%d: %s',
                [AItems[I].Topic, AItems[I].Partition, KafkaErrorText(LCode)]));
          end;
    end;
  finally
    LNodes.Free;
  end;
end;

function TKafkaConsumer.QueryOffsets(const APartitions: array of TKafkaTopicPartition;
  ATimestamp: Int64): TKafkaInt64Array;
var
  LItems: TOffsetQueryArray;
  LDeadline: UInt64;
  I: Integer;
  LAll: Boolean;
  LTopics: array of string;
begin
  CheckOpen;
  LItems := nil;
  SetLength(LItems, Length(APartitions));
  SetLength(LTopics, Length(APartitions));
  for I := 0 to High(APartitions) do
  begin
    LItems[I].Topic := APartitions[I].Topic;
    LItems[I].Partition := APartitions[I].Partition;
    LItems[I].Timestamp := ATimestamp;
    LItems[I].Offset := -1;
    LTopics[I] := APartitions[I].Topic;
  end;
  LDeadline := PcTickMs + FMaxBlockMs;
  // Waits for each topic to exist (EKafkaTimeout, EKafkaBrokerError).
  for I := 0 to High(LTopics) do
    FCluster.Topic(LTopics[I]);
  repeat
    QueryOffsetsOnce(LItems);
    LAll := True;
    for I := 0 to High(LItems) do
      if not LItems[I].Done then
        LAll := False;
    if LAll then
      Break;
    if PcTickMs >= LDeadline then
      raise EKafkaTimeout.CreateFmt('ListOffsets: no answer for every partition in %d ms',
        [FMaxBlockMs]);
    FSignal.WaitFor(FRetryBackoffMs);
    try
      FCluster.Refresh(LTopics);
    except
      on EKafkaConnection do ;
    end;
  until False;
  Result := nil;
  SetLength(Result, Length(LItems));
  for I := 0 to High(LItems) do
    Result[I] := LItems[I].Offset;
end;

function TKafkaConsumer.BeginningOffsets(
  const APartitions: array of TKafkaTopicPartition): TKafkaInt64Array;
begin
  Acquire;
  try
    Result := QueryOffsets(APartitions, LIST_OFFSETS_EARLIEST);
  finally
    Release;
  end;
end;

function TKafkaConsumer.EndOffsets(
  const APartitions: array of TKafkaTopicPartition): TKafkaInt64Array;
begin
  Acquire;
  try
    Result := QueryOffsets(APartitions, LIST_OFFSETS_LATEST);
  finally
    Release;
  end;
end;

function TKafkaConsumer.PartitionsFor(const ATopic: string): TKafkaPartitionInfoArray;
begin
  Result := FCluster.Topic(ATopic).Partitions;
end;

function TKafkaConsumer.FetchesSent: Integer;
begin
  Result := FFetchesSent;
end;

procedure TKafkaConsumer.ResolvePositions;
var
  LItems: TOffsetQueryArray;
  LStates: array of TPartitionState;
  S: TPartitionState;
  N, I: Integer;
begin
  // The group's committed offsets first, for the partitions that start
  // from them.
  if FParams.GroupId <> '' then
    ResolveCommitted;
  LItems := nil;
  LStates := nil;
  N := 0;
  for S in FOrder do
  begin
    if ((S.Reset = rkNone) and (S.Position >= 0)) or S.NeedsCommitted then
      Continue;
    if (S.Reset = rkNone) and (FParams.AutoOffsetReset = korNone) then
      raise EKafkaBrokerError.Create(KAFKA_ERROR_OFFSET_OUT_OF_RANGE, Format('%s-%d has no ' +
        'position and AutoOffsetReset is none: Seek first', [S.Topic, S.Partition]));
    SetLength(LItems, N + 1);
    SetLength(LStates, N + 1);
    LItems[N].Topic := S.Topic;
    LItems[N].Partition := S.Partition;
    if (S.Reset = rkEarliest) or ((S.Reset = rkNone) and
      (FParams.AutoOffsetReset = korEarliest)) then
      LItems[N].Timestamp := LIST_OFFSETS_EARLIEST
    else
      LItems[N].Timestamp := LIST_OFFSETS_LATEST;
    LItems[N].Offset := -1;
    LItems[N].Done := False;
    LStates[N] := S;
    Inc(N);
  end;
  if N = 0 then
    Exit;
  QueryOffsetsOnce(LItems);
  for I := 0 to N - 1 do
    if LItems[I].Done then
    begin
      LStates[I].Position := LItems[I].Offset;
      LStates[I].Reset := rkNone;
      LStates[I].LastEpoch := -1;
      LStates[I].ClearBuffer;
    end;
end;

function TKafkaConsumer.Position(const ATopic: string; APartition: Int32): Int64;
var
  S: TPartitionState;
  LDeadline: UInt64;
begin
  Acquire;
  try
    CheckOpen;
    S := StateOf(ATopic, APartition);
    LDeadline := PcTickMs + FMaxBlockMs;
    while (S.Reset <> rkNone) or (S.Position < 0) do
    begin
      CheckWakeup;
      MaybeRefreshMetadata(PcTickMs);
      ResolvePositions;
      if (S.Reset = rkNone) and (S.Position >= 0) then
        Break;
      if PcTickMs >= LDeadline then
        raise EKafkaTimeout.CreateFmt('No position for %s-%d in %d ms',
          [ATopic, APartition, FMaxBlockMs]);
      FSignal.WaitFor(FRetryBackoffMs);
    end;
    Result := S.Position;
  finally
    Release;
  end;
end;

procedure TKafkaConsumer.CollectFetches(ANow: UInt64);
var
  I: Integer;
  F: TFetchFlight;
begin
  I := 0;
  while I < FFlights.Count do
  begin
    F := FFlights[I];
    if not F.Future.IsDone then
    begin
      if ANow < F.Deadline then
      begin
        Inc(I);
        Continue;
      end;
      // Its future fails with EKafkaTimeout: retried like a broken
      // connection.
      F.Broker.Connection.Abandon(F.Future);
    end;
    FFlights.Extract(F);
    try
      HandleFetch(F);
    finally
      F.Free;
    end;
  end;
end;

procedure TKafkaConsumer.HandleFetch(AFlight: TFetchFlight);
var
  LResponse: TKafkaFetchResponse;
  LPart: TKafkaFetchResponsePartitionData;
  S: TPartitionState;
  T, P, I, R, LIndex, LTrailing: Integer;
  LMatch: Boolean;
  LBatches: TKafkaRecordBatchArray;
  LRecords: TKafkaConsumerRecordArray;
  LCount: Integer;
  LNext, LOffset: Int64;
  LRec: TKafkaRecord;
  LCode: Int16;
begin
  // Whatever the outcome, these partitions may be fetched again.
  for I := 0 to High(AFlight.Keys) do
    if FStates.TryGetValue(AFlight.Keys[I], S) then
      S.InFlight := False;
  try
    LResponse.FromBytes(AFlight.Future.Body, AFlight.Version);
  except
    on E: EKafka do
    begin
      if (E is EKafkaConnection) or (E is EKafkaTimeout) then
      begin
        BackOffNode(AFlight.NodeId);
        Exit;
      end;
      raise;
    end;
  end;
  if LResponse.ErrorCode <> KAFKA_ERROR_NONE then
  begin
    if KafkaErrorIsRetriable(LResponse.ErrorCode) then
    begin
      BackOffNode(AFlight.NodeId);
      Exit;
    end;
    raise EKafkaBrokerError.Create(LResponse.ErrorCode, Format('Fetch from broker %d: %s',
      [AFlight.NodeId, KafkaErrorText(LResponse.ErrorCode)]));
  end;
  for T := 0 to High(LResponse.Responses) do
    for P := 0 to High(LResponse.Responses[T].Partitions) do
    begin
      LPart := LResponse.Responses[T].Partitions[P];
      LIndex := -1;
      for I := 0 to High(AFlight.Keys) do
      begin
        if AFlight.Version >= 13 then
          LMatch := LResponse.Responses[T].TopicId.Equals(AFlight.TopicIds[I])
        else
          LMatch := LResponse.Responses[T].Topic = AFlight.Topics[I];
        if LMatch and (AFlight.Partitions[I] = LPart.PartitionIndex) then
          LIndex := I;
      end;
      if LIndex < 0 then
        Continue;
      // Unassigned, sought elsewhere or reset meanwhile: the answer is stale.
      if not FStates.TryGetValue(AFlight.Keys[LIndex], S) or (S.Reset <> rkNone) or
        (S.Position <> AFlight.Offsets[LIndex]) or S.HasBuffer then
        Continue;
      LCode := LPart.ErrorCode;
      if LCode = KAFKA_ERROR_OFFSET_OUT_OF_RANGE then
      begin
        case FParams.AutoOffsetReset of
          korEarliest: S.Reset := rkEarliest;
          korLatest: S.Reset := rkLatest;
        else
          raise EKafkaBrokerError.Create(LCode, Format('%s-%d: offset %d is out of range ' +
            'and AutoOffsetReset is none', [S.Topic, S.Partition, S.Position]));
        end;
        Continue;
      end;
      if LCode <> KAFKA_ERROR_NONE then
      begin
        if KafkaErrorIsRetriable(LCode) then
        begin
          if KafkaErrorIsInvalidMetadata(LCode) then
          begin
            // The leader may have moved: the metadata first, then this
            // partition again.
            S.WaitRefreshAfter := FCluster.RefreshCount;
            FCluster.Invalidate;
          end;
          Continue;
        end;
        raise EKafkaBrokerError.Create(LCode, Format('Fetch %s-%d: %s',
          [S.Topic, S.Partition, KafkaErrorText(LCode)]));
      end;
      S.HighWatermark := LPart.HighWatermark;
      S.UpdateLag;
      if LPart.RecordsIsNull or (Length(LPart.Records) = 0) then
        Continue;
      LOffset := AFlight.Offsets[LIndex];
      LBatches := KafkaDecodeRecordBatches(LPart.Records, KAFKA_MAX_DECOMPRESSED_BATCH_BYTES,
        LTrailing);
      LRecords := nil;
      LCount := 0;
      LNext := LOffset;
      for I := 0 to High(LBatches) do
      begin
        if LBatches[I].NextOffset <= LOffset then
          Continue;
        if not LBatches[I].IsControl then
        begin
          if LCount + Length(LBatches[I].Records) > Length(LRecords) then
            SetLength(LRecords, LCount + Length(LBatches[I].Records));
          for R := 0 to High(LBatches[I].Records) do
          begin
            LRec := LBatches[I].Records[R];
            if LRec.Offset < LOffset then
              Continue; // a compressed batch starts before the position
            LRecords[LCount].Topic := S.Topic;
            LRecords[LCount].Partition := S.Partition;
            LRecords[LCount].Offset := LRec.Offset;
            LRecords[LCount].Timestamp := LRec.Timestamp;
            if LBatches[I].IsLogAppendTime then
              LRecords[LCount].TimestampType := kttLogAppendTime
            else
              LRecords[LCount].TimestampType := kttCreateTime;
            LRecords[LCount].LeaderEpoch := LBatches[I].PartitionLeaderEpoch;
            LRecords[LCount].Key := LRec.Key;
            LRecords[LCount].KeyIsNull := LRec.KeyIsNull;
            LRecords[LCount].Value := LRec.Value;
            LRecords[LCount].ValueIsNull := LRec.ValueIsNull;
            LRecords[LCount].Headers := LRec.Headers;
            Inc(LCount);
          end;
        end;
        if LBatches[I].NextOffset > LNext then
          LNext := LBatches[I].NextOffset;
      end;
      if LNext > LOffset then
      begin
        SetLength(LRecords, LCount);
        S.Buffer := LRecords;
        S.BufferIndex := 0;
        S.BufferNext := LNext;
        S.HasBuffer := True;
      end;
      // No whole batch (one larger than MaxPartitionFetchBytes, behind
      // others in the answer): the rotation puts the partition first in a
      // later Fetch, and a broker always returns the first one whole.
    end;
end;

procedure TKafkaConsumer.SendFetches(ANow: UInt64);
var
  LMetadata: IKafkaClusterMetadata;
  LTopic: TKafkaTopicInfo;
  LBusy: TDictionary<Int32, Boolean>;
  LByNode: TObjectDictionary<Int32, TList<TPartitionState>>;
  LList: TList<TPartitionState>;
  LPair: TPair<Int32, TList<TPartitionState>>;
  LNode: Int32;
  S: TPartitionState;
  F: TFetchFlight;
  LRequest: TKafkaFetchRequest;
  LBroker: IKafkaBrokerConnection;
  LVersion: Int16;
  I, N, T, P, K: Integer;
  LNeedMetadata: Boolean;
begin
  LMetadata := FCluster.Metadata;
  if (LMetadata = nil) or (FOrder.Count = 0) then
    Exit;
  LNeedMetadata := False;
  LBusy := TDictionary<Int32, Boolean>.Create;
  LByNode := TObjectDictionary<Int32, TList<TPartitionState>>.Create([doOwnsValues]);
  try
    for F in FFlights do
      LBusy.AddOrSetValue(F.NodeId, True);
    N := FOrder.Count;
    for I := 0 to N - 1 do
    begin
      S := FOrder[(FFetchStart + I) mod N];
      if (S.Position < 0) or (S.Reset <> rkNone) or S.HasBuffer or S.InFlight then
        Continue;
      if S.WaitRefreshAfter >= 0 then
      begin
        if FCluster.RefreshCount <= S.WaitRefreshAfter then
          Continue;
        S.WaitRefreshAfter := -1;
      end;
      if not LMetadata.FindTopic(S.Topic, LTopic) or (S.Partition >= Length(LTopic.Partitions)) or
        (LTopic.Partitions[S.Partition].LeaderId < 0) then
      begin
        LNeedMetadata := True;
        Continue;
      end;
      LNode := LTopic.Partitions[S.Partition].LeaderId;
      if LBusy.ContainsKey(LNode) or not NodeAvailable(LNode, ANow) then
        Continue;
      if not LByNode.TryGetValue(LNode, LList) then
      begin
        LList := TList<TPartitionState>.Create;
        LByNode.Add(LNode, LList);
      end;
      LList.Add(S);
    end;
    FFetchStart := FFetchStart + 1;
    if FFetchStart >= N then
      FFetchStart := 0;
    if LNeedMetadata then
      FCluster.Invalidate;
    for LPair in LByNode do
    begin
      LNode := LPair.Key;
      LList := LPair.Value;
      try
        LBroker := FCluster.Broker(LNode);
        LVersion := LBroker.Connection.NegotiatedVersion(KAFKA_API_KEY_FETCH);
      except
        on E: EKafka do
        begin
          if E is EKafkaUnsupportedVersion then
            raise;
          BackOffNode(LNode);
          Continue;
        end;
      end;
      LRequest.Init;
      LRequest.MaxWaitMs := FParams.FetchMaxWaitMs;
      LRequest.MinBytes := FParams.FetchMinBytes;
      LRequest.MaxBytes := FParams.FetchMaxBytes;
      LRequest.IsolationLevel := 0;
      F := TFetchFlight.Create;
      try
        F.NodeId := LNode;
        F.Broker := LBroker;
        F.Version := LVersion;
        SetLength(F.Keys, LList.Count);
        SetLength(F.Topics, LList.Count);
        SetLength(F.TopicIds, LList.Count);
        SetLength(F.Partitions, LList.Count);
        SetLength(F.Offsets, LList.Count);
        for K := 0 to LList.Count - 1 do
        begin
          S := LList[K];
          LMetadata.FindTopic(S.Topic, LTopic);
          F.Keys[K] := KeyOf(S.Topic, S.Partition);
          F.Topics[K] := S.Topic;
          F.TopicIds[K] := LTopic.TopicId;
          F.Partitions[K] := S.Partition;
          F.Offsets[K] := S.Position;
          T := -1;
          for I := 0 to High(LRequest.Topics) do
            if LRequest.Topics[I].Topic = S.Topic then
              T := I;
          if T < 0 then
          begin
            T := Length(LRequest.Topics);
            SetLength(LRequest.Topics, T + 1);
            LRequest.Topics[T].Init;
            // Name up to v12, TopicId from v13 (both ignorable).
            LRequest.Topics[T].Topic := S.Topic;
            LRequest.Topics[T].TopicId := LTopic.TopicId;
          end;
          P := Length(LRequest.Topics[T].Partitions);
          SetLength(LRequest.Topics[T].Partitions, P + 1);
          LRequest.Topics[T].Partitions[P].Init;
          LRequest.Topics[T].Partitions[P].Partition := S.Partition;
          LRequest.Topics[T].Partitions[P].FetchOffset := S.Position;
          LRequest.Topics[T].Partitions[P].PartitionMaxBytes := FParams.MaxPartitionFetchBytes;
        end;
        try
          F.Future := LBroker.Connection.Send(KAFKA_API_KEY_FETCH, LVersion,
            LRequest.ToBytes(LVersion), FSignalRef);
        except
          on E: EKafkaConnection do
          begin
            BackOffNode(LNode);
            FreeAndNil(F);
            Continue;
          end;
        end;
        F.Deadline := PcTickMs + FRequestTimeoutMs + FParams.FetchMaxWaitMs;
        FFlights.Add(F);
        F := nil;
        Inc(FFetchesSent);
        for S in LList do
          S.InFlight := True;
      finally
        F.Free;
      end;
    end;
  finally
    LByNode.Free;
    LBusy.Free;
  end;
end;

function TKafkaConsumer.Drain: TKafkaConsumerRecordArray;
var
  N, I, LCount, LTake, K: Integer;
  S: TPartitionState;
begin
  Result := nil;
  LCount := 0;
  N := FOrder.Count;
  for I := 0 to N - 1 do
  begin
    if LCount >= FParams.MaxPollRecords then
      Break;
    S := FOrder[(FDrainStart + I) mod N];
    if not S.HasBuffer then
      Continue;
    LTake := Length(S.Buffer) - S.BufferIndex;
    if LTake > FParams.MaxPollRecords - LCount then
      LTake := FParams.MaxPollRecords - LCount;
    if LTake > 0 then
    begin
      SetLength(Result, LCount + LTake);
      for K := 0 to LTake - 1 do
        Result[LCount + K] := S.Buffer[S.BufferIndex + K];
      Inc(S.BufferIndex, LTake);
      Inc(LCount, LTake);
      S.Position := Result[LCount - 1].Offset + 1;
      S.LastEpoch := Result[LCount - 1].LeaderEpoch;
      KafkaMetricAdd(S.ConsumedSeries, LTake);
    end;
    if S.BufferIndex >= Length(S.Buffer) then
    begin
      if S.BufferNext > S.Position then
        S.Position := S.BufferNext;
      S.ClearBuffer;
    end;
    S.UpdateLag;
  end;
  if N > 0 then
  begin
    Inc(FDrainStart);
    if FDrainStart >= N then
      FDrainStart := 0;
  end;
end;

function PollTopic(const ARecords: TKafkaConsumerRecordArray): string;
var
  I: Integer;
begin
  Result := ARecords[0].Topic;
  for I := 1 to High(ARecords) do
    if ARecords[I].Topic <> Result then
      Exit('');
end;

function TKafkaConsumer.Poll(ATimeoutMs: Cardinal): TKafkaConsumerRecordArray;
var
  LSpan: IPcSpan;
begin
  if not TPcTracing.Enabled then
    Exit(PollRecords(ATimeoutMs));
  // The current span while Poll runs (the handlers inside are its
  // children), exported only when records come back: an empty Poll lets it
  // go unfinished (Kafka.Tracing).
  LSpan := KafkaStartPollSpan(FParams.GroupId, FParams.Cluster.Connection.ClientId);
  try
    Result := PollRecords(ATimeoutMs);
  except
    on E: Exception do
    begin
      // A wakeup is how a loop stops, not a failure of the Poll.
      if not (E is EKafkaWakeup) then
        KafkaFinishSpan(LSpan, E);
      raise;
    end;
  end;
  if Length(Result) > 0 then
    KafkaFinishPollSpan(LSpan, PollTopic(Result), Length(Result));
end;

function TKafkaConsumer.StartProcessSpan(const ARecord: TKafkaConsumerRecord): IPcSpan;
var
  LSize: Int64;
begin
  if not TPcTracing.Enabled then
    Exit(nil);
  if ARecord.ValueIsNull then
    LSize := -1
  else
    LSize := Length(ARecord.Value);
  Result := KafkaStartProcessSpan(ARecord.Topic, ARecord.Partition, ARecord.Offset,
    ARecord.Headers, LSize, FParams.GroupId, FParams.Cluster.Connection.ClientId);
end;

function TKafkaConsumer.PollRecords(ATimeoutMs: Cardinal): TKafkaConsumerRecordArray;
var
  LDeadline, LNow, LWake: UInt64;
  F: TFetchFlight;
  S: TPartitionState;
begin
  AcquireOuter('Poll');
  try
    CheckOpen;
    if (FOrder.Count = 0) and not FSubscribed then
      raise EKafka.Create('The consumer is not subscribed to any topic or assigned any ' +
        'partition (call Subscribe or Assign)');
    if ATimeoutMs = KAFKA_WAIT_INFINITE then
      LDeadline := High(UInt64)
    else
      LDeadline := PcTickMs + ATimeoutMs;
    repeat
      CheckWakeup;
      LNow := PcTickMs;
      // The join, the group's events (which change the partitions), the
      // commits' handlers and the auto-commit, before anything is fetched.
      GroupStep(LNow);
      MaybeRefreshMetadata(LNow);
      ResolvePositions;
      CollectFetches(PcTickMs);
      Result := Drain;
      SendFetches(PcTickMs);
      if Length(Result) > 0 then
        Exit;
      LNow := PcTickMs;
      if LNow >= LDeadline then
        Exit;
      // Until an answer (the signal), the next fetch deadline, or a backoff
      // for whatever couldn't be sent.
      LWake := High(UInt64);
      for F in FFlights do
        if F.Deadline < LWake then
          LWake := F.Deadline;
      // A partition neither in flight nor buffered is waiting for a
      // position, a leader or a broker's backoff: look again after one.
      for S in FOrder do
        if not S.InFlight and not S.HasBuffer then
        begin
          if LNow + FRetryBackoffMs < LWake then
            LWake := LNow + FRetryBackoffMs;
          Break;
        end;
      // The auto-commit's next turn; and a Poll that waits long still
      // tells the membership it is alive (the poll timer).
      if FAutoCommitEnabled and (FMembership <> nil) and (FNextAutoCommitAt < LWake) then
        LWake := FNextAutoCommitAt;
      if FSubscribed and (LNow + FMaxPollIntervalMs div 2 < LWake) then
        LWake := LNow + FMaxPollIntervalMs div 2;
      if LWake > LDeadline then
        LWake := LDeadline;
      if LWake > LNow then
        FSignal.WaitFor(Cardinal(LWake - LNow));
    until False;
  finally
    Release;
  end;
end;

end.
