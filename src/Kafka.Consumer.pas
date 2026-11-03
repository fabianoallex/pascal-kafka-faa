unit Kafka.Consumer;

{$I kafka.inc}

{ The consumer with manually assigned partitions: Assign says which
  partitions to read, Seek where, and Poll returns the records, on the
  caller's thread, as Java's KafkaConsumer.assign/seek/poll do. Consumer
  groups (KIP-848, and committed offsets) are phase K2.

  Positions. Each assigned partition has a position: the offset of the next
  record Poll returns. A new partition has none until it is resolved from
  AutoOffsetReset (Java's auto.offset.reset: latest by default, earliest, or
  none, which raises); Seek sets one; SeekToBeginning and SeekToEnd are
  resolved at the next Poll or Position, as in Java. A Fetch answered with
  OFFSET_OUT_OF_RANGE (the position fell off the log, or past its end)
  resets the position the same way. Positions live only in the consumer:
  nothing is committed until K2.

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
  state. Wakeup is the exception: any thread may call it, and the Poll in
  progress (or the next one) raises EKafkaWakeup, the way to stop a polling
  loop from elsewhere. Close is called from the polling thread (or after
  it stopped).

  Delphi x FPC: nothing specific; the wait is the connection's IKafkaSignal
  (Kafka.Connection), one event for every Fetch in flight. }

interface

uses
  SysUtils,
  Classes,
  Generics.Collections,
  Kafka.Types,
  Kafka.Protocol.Primitives,
  Kafka.Protocol.RecordBatch,
  Kafka.Connection,
  Kafka.Cluster;

const
  KAFKA_DEFAULT_FETCH_MIN_BYTES = 1;
  KAFKA_DEFAULT_FETCH_MAX_WAIT_MS = 500;
  KAFKA_DEFAULT_FETCH_MAX_BYTES = 52428800;
  KAFKA_DEFAULT_MAX_PARTITION_FETCH_BYTES = 1048576;
  KAFKA_DEFAULT_MAX_POLL_RECORDS = 500;
  /// What one fetched batch may decompress to: a bound against a hostile
  /// or corrupt batch, far above what a producer's batch holds.
  KAFKA_MAX_DECOMPRESSED_BATCH_BYTES = 256 * 1024 * 1024;

type
  /// Java's auto.offset.reset; the zero (latest) is Java's default.
  TKafkaOffsetReset = (korLatest, korEarliest, korNone);

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
      procedure ClearBuffer;
    end;

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
    procedure Acquire;
    procedure Release;
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
    procedure SeekTo(const APartitions: array of TKafkaTopicPartition; AKind: TResetKind);
  public
    /// Checks the parameters; no I/O.
    constructor Create(const AParams: TKafkaConsumerParams);
    /// Close, then frees.
    destructor Destroy; override;
    /// The cluster's bootstrap. Once only.
    procedure Open;
    /// Abandons the fetches in flight and closes the cluster. Idempotent.
    procedure Close;

    /// The partitions to read, replacing the current ones. A partition kept
    /// keeps its position; a new one has none yet. No I/O.
    procedure Assign(const APartitions: array of TKafkaTopicPartition);
    function Assignment: TKafkaTopicPartitionArray;
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
  end;

/// Java's defaults, with ABootstrapServers.
function KafkaConsumerParams(const ABootstrapServers: string): TKafkaConsumerParams;

implementation

uses
  PascalCommon.Threading,
  Kafka.Errors,
  Kafka.Messages.Apis,
  Kafka.Messages.Fetch,
  Kafka.Messages.ListOffsets;

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
end;

function KeyOf(const ATopic: string; APartition: Int32): string;
begin
  Result := ATopic + #0 + IntToStr(APartition);
end;

{ TKafkaConsumer.TPartitionState }

procedure TKafkaConsumer.TPartitionState.ClearBuffer;
begin
  Buffer := nil;
  BufferIndex := 0;
  BufferNext := -1;
  HasBuffer := False;
end;

{ TKafkaConsumer }

constructor TKafkaConsumer.Create(const AParams: TKafkaConsumerParams);
begin
  inherited Create;
  FParams := AParams;
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
    Close;
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
    raise EKafka.Create('The consumer is not safe for multi-threaded access ' +
      '(another thread is inside it; Wakeup is the one method any thread may call)');
end;

procedure TKafkaConsumer.Release;
begin
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
  finally
    Release;
  end;
end;

procedure TKafkaConsumer.Close;
var
  F: TFetchFlight;
begin
  if FClosed then
    Exit;
  FClosed := True;
  for F in FFlights do
    if F.Future <> nil then
      F.Broker.Connection.Abandon(F.Future);
  FFlights.Clear;
  FCluster.Close;
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
  Acquire;
  try
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
      begin
        S := TPartitionState.Create;
        S.Topic := APartitions[I].Topic;
        S.Partition := APartitions[I].Partition;
        S.Position := -1;
        S.WaitRefreshAfter := -1;
        S.ClearBuffer;
      end;
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
  LItems := nil;
  LStates := nil;
  N := 0;
  for S in FOrder do
  begin
    if (S.Reset = rkNone) and (S.Position >= 0) then
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
    end;
    if S.BufferIndex >= Length(S.Buffer) then
    begin
      if S.BufferNext > S.Position then
        S.Position := S.BufferNext;
      S.ClearBuffer;
    end;
  end;
  if N > 0 then
  begin
    Inc(FDrainStart);
    if FDrainStart >= N then
      FDrainStart := 0;
  end;
end;

function TKafkaConsumer.Poll(ATimeoutMs: Cardinal): TKafkaConsumerRecordArray;
var
  LDeadline, LNow, LWake: UInt64;
  F: TFetchFlight;
  S: TPartitionState;
begin
  Acquire;
  try
    CheckOpen;
    if FOrder.Count = 0 then
      raise EKafka.Create('No partition is assigned (call Assign)');
    if ATimeoutMs = KAFKA_WAIT_INFINITE then
      LDeadline := High(UInt64)
    else
      LDeadline := PcTickMs + ATimeoutMs;
    repeat
      CheckWakeup;
      LNow := PcTickMs;
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
