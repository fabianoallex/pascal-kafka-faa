unit Kafka.Group.Consumer;

{$I kafka.inc}

{ The consumer group protocol of KIP-848 (ConsumerGroupHeartbeat): the
  IKafkaGroupMembership (Kafka.Group) that Kafka 4 brokers speak, written
  after Java 4.4.0's AbstractMembershipManager, ConsumerMembershipManager
  and ConsumerHeartbeatRequestManager.

  The member. Its id is the client's own (KIP-1082): a random UUID made at
  creation and kept for the membership's life, sent with every heartbeat.
  Its epoch is 0 to join, what the coordinator answers while in the group,
  and -1 to leave.

  The heartbeat thread. Subscribe starts it. It finds the coordinator
  (Kafka.Group.Coordinator), sends a heartbeat at once to join, then every
  HeartbeatIntervalMs the coordinator gives (5 s by default on the broker),
  and at once again whenever there is something to tell: joining again, or
  acknowledging an assignment the consumer took. One heartbeat in flight
  at a time. A heartbeat carries only what changed since the last one,
  as Java's HeartbeatState: the subscription, the rebalance timeout, the
  server assignor and the owned partitions are sent when joining and when
  they differ from what was last sent; after any failure everything is
  sent again.

  Reconciliation. The coordinator gives the member's target assignment by
  topic id; the names come from the cluster's metadata (a refresh when an
  id isn't known yet: the part that can be named is reconciled meanwhile,
  as Java's). The partitions the consumer holds and the target are
  compared: what the target no longer has is revoked, what is new is
  assigned. Both go to the consumer as events (Kafka.Group: queued here,
  handed out in the consumer's Poll, on its thread), revoked first, and
  only when the consumer has handled them does the next heartbeat
  acknowledge the new assignment: the coordinator gives a partition to
  another member only after this one acknowledged letting it go, so a
  revoke, and the commit the consumer makes in it, comes before anyone
  else reads the partition. A new target that arrives meanwhile waits for
  the reconciliation in progress, then is reconciled in turn.

  Errors in a heartbeat's answer (Java's choices):
  - NOT_COORDINATOR, COORDINATOR_NOT_AVAILABLE, or the connection failing:
    the coordinator is looked up again, and the heartbeat goes as soon as
    it is found (the lookups have their backoff).
  - COORDINATOR_LOAD_IN_PROGRESS: the same coordinator, after a backoff
    (RetryBackoffMs doubled per failure up to RetryBackoffMaxMs). Java
    retries at once while joining; this waits the backoff then too.
  - FENCED_MEMBER_EPOCH, UNKNOWN_MEMBER_ID (the member was removed: its
    session expired, or an epoch went wrong): the partitions are lost (a
    Lost event: someone else may own them already, so they are not
    committed), and once the consumer handled it the member joins again
    with epoch 0, the same member id.
  - TOPIC_AUTHORIZATION_FAILED: raised from the next Poll; the member stays
    in the group, as Java's (the ACL may be added).
  - Anything else (GROUP_AUTHORIZATION_FAILED, UNSUPPORTED_ASSIGNOR,
    GROUP_MAX_SIZE_REACHED, INVALID_REQUEST, a code this library doesn't
    know...) is fatal: the member stops, its partitions are lost, and Poll
    raises the error once. A broker without ConsumerGroupHeartbeat (before
    Kafka 4.0, or with the protocol turned off) is fatal the same way, with
    EKafkaUnsupportedVersion.

  Leaving. Leave revokes the partitions still held (Lost if the member was
  no longer in the group), stops the thread, and sends one heartbeat with
  epoch -1 to the known coordinator, waiting for its answer up to the given
  time; it isn't retried, and without a known coordinator it isn't sent
  (Java's choices too). Close stops without leaving.

  Commits. Commit (OffsetCommit) and Committed (OffsetFetch v8 or later,
  require_stable) run on the caller's thread with the member id and epoch;
  topic ids (v10) when the metadata has them all, names otherwise. They
  retry what Java's CommitRequestManager retries until the timeout:
  coordinator errors (and the lookup again), a timed out request,
  COORDINATOR_LOAD_IN_PROGRESS, an unknown topic, and STALE_MEMBER_EPOCH
  while the member is still in the group (with the newer epoch). Anything
  else raises EKafkaBrokerError at once: UNKNOWN_MEMBER_ID (the member is
  out of the group), the authorization errors, OFFSET_METADATA_TOO_LARGE.

  Threads. The heartbeat thread owns the protocol state, under the wait
  hub's lock (Kafka.Group); the consumer's thread takes the events, commits
  and leaves. Neither calls the other's code: the heartbeat thread only
  queues events and fires the Notify signal (TKafkaConsumerProtocolParams),
  which must be quick and not raise (an event's set, as the consumer's).
  Not done yet: leaving and joining again when Poll isn't called within
  the rebalance timeout (Java's max.poll.interval.ms check), static
  membership (group.instance.id), regex subscriptions. }

interface

uses
  SysUtils,
  Classes,
  Generics.Collections,
  Kafka.Types,
  Kafka.Protocol.Primitives,
  Kafka.Connection,
  Kafka.Cluster,
  Kafka.Group,
  Kafka.Group.Coordinator;

type
  TKafkaConsumerProtocolParams = record
    GroupId: string;
    /// Java's client.rack: sent when joining ('' sends none).
    RackId: string;
    /// Java's group.remote.assignor: '' lets the broker choose (uniform).
    ServerAssignor: string;
    /// The rebalance timeout the coordinator gives the member to revoke
    /// partitions; Java sends max.poll.interval.ms.
    /// 0 = KAFKA_DEFAULT_MAX_POLL_INTERVAL_MS.
    RebalanceTimeoutMs: Cardinal;
    /// 0 = the cluster's RetryBackoffMs.
    RetryBackoffMs: Cardinal;
    /// 0 = KAFKA_DEFAULT_RETRY_BACKOFF_MAX_MS.
    RetryBackoffMaxMs: Cardinal;
    /// Fired from the heartbeat thread when an event or an error waits for
    /// Poll (nil: none).
    Notify: IKafkaSignal;
  end;

  TKafkaConsumerProtocol = class(TInterfacedObject, IKafkaGroupMembership)
  private type
    TTopicAssignment = record
      TopicId: TKafkaUuid;
      /// Ascending.
      Partitions: TKafkaInt32Array;
    end;
    TTopicAssignmentArray = array of TTopicAssignment;

    { Java's LocalAssignment: the local epoch goes up with each new target,
      so an acknowledgement goes out even when the partitions came back
      the same. }
    TAssignment = record
      /// -1: none (not joined, or cleared).
      LocalEpoch: Int64;
      /// By topic id (its string form, ascending).
      Topics: TTopicAssignmentArray;
    end;

    TQueuedEvent = record
      Kind: TKafkaGroupEventKind;
      Partitions: TKafkaTopicPartitionArray;
    end;
    TQueuedEventArray = array of TQueuedEvent;

    THeartbeatThread = class(TThread)
    private
      FOwner: TKafkaConsumerProtocol;
    protected
      procedure Execute; override;
    public
      constructor Create(AOwner: TKafkaConsumerProtocol);
    end;

    TStep = (stWait, stExit, stAnswer, stTimedOut, stSend, stRefreshMetadata);
  private
    FCluster: TKafkaCluster;
    FParams: TKafkaConsumerProtocolParams;
    FRebalanceTimeoutMs: Int32;
    FRetryBackoffMs: Cardinal;
    FRetryBackoffMaxMs: Cardinal;
    FRequestTimeoutMs: Cardinal;
    FHub: TKafkaWaitHub;
    FHubRef: IKafkaSignal;
    FCoordinator: TKafkaGroupCoordinator;
    FThread: THeartbeatThread;
    FClosed: Boolean;
    FLeft: Boolean;

    // Everything below: under FHub's lock.
    FMemberId: string;
    FState: TKafkaMemberState;
    FEpoch: Int32;
    FSubscription: TKafkaStringArray;
    FSubscriptionKey: string;
    // What the last heartbeat sent (Java's SentFields).
    FSentRebalanceTimeoutMs: Int32;
    FSentSubscription: Boolean;
    FSentSubscriptionKey: string;
    FSentServerAssignor: Boolean;
    FSentAssignment: Boolean;
    FSentAssignmentKey: string;
    // Timing.
    FHeartbeatIntervalMs: Cardinal;
    FNextHeartbeatAt: UInt64;
    FBackoffUntil: UInt64;
    FFailures: Integer;
    FLastSentAt: UInt64;
    FHeartbeatCount: Integer;
    // The heartbeat in flight.
    FInFlight: IKafkaResponseFuture;
    FInFlightBroker: IKafkaBrokerConnection;
    FInFlightVersion: Int16;
    FInFlightDeadline: UInt64;
    // Reconciliation.
    FTarget: TAssignment;
    FCurrent: TAssignment;
    FPending: TAssignment;
    FReconciling: Boolean;
    FNames: TDictionary<string, string>;
    FNeedMetadata: Boolean;
    FNextMetadataAt: UInt64;
    FSubscriptionChanged: Boolean;
    // What the handled events gave the consumer.
    FOwned: TKafkaTopicPartitionArray;
    FEvents: TQueuedEventArray;
    FEventHead: Integer;
    FDelivering: Boolean;
    // An error for Poll.
    FHasError: Boolean;
    FErrorClass: EKafkaClass;
    FErrorCode: Int16;
    FErrorMessage: string;

    procedure Run;
    function NextStep(out AWaitMs: Cardinal; out AFuture: IKafkaResponseFuture;
      out ABroker: IKafkaBrokerConnection; out AVersion: Int16): TStep;
    procedure SendHeartbeat;
    procedure HandleAnswer(const AFuture: IKafkaResponseFuture;
      const ABroker: IKafkaBrokerConnection; AVersion: Int16);
    procedure HeartbeatFailed(const AReason: string);
    procedure RefreshMetadata;
    // Under the lock:
    procedure ResetSentFields;
    procedure Backoff;
    procedure SetError(AClass: EKafkaClass; ACode: Int16; const AMessage: string; AFatal: Boolean);
    procedure Queue(AKind: TKafkaGroupEventKind; const APartitions: TKafkaTopicPartitionArray);
    procedure DropQueuedEvents;
    function EventsIdle: Boolean;
    procedure AfterEvents;
    procedure Rejoin;
    procedure TransitionToFenced;
    function Reconcile: Boolean;
    function FindTopicName(const AMetadata: IKafkaClusterMetadata; const AId: TKafkaUuid;
      out AName: string): Boolean;
    procedure StopThread;
    function TopicIdOf(const ATopic: string): TKafkaUuid;
    function TryCommitOnce(const AOffsets: TKafkaCommittedOffsetArray; ADeadline: UInt64;
      out AReason: string): Boolean;
    function TryCommittedOnce(const APartitions: array of TKafkaTopicPartition; ADeadline: UInt64;
      var AResult: TKafkaCommittedOffsetArray; out AReason: string): Boolean;
    procedure RetryPause(AAttempt: Integer; ADeadline: UInt64; const AWhat, AReason: string;
      ATimeoutMs: Cardinal);
    procedure CurrentMember(out AMemberId: string; out AEpoch: Int32);
  public
    /// The cluster is the consumer's (open, and alive until this is closed).
    constructor Create(ACluster: TKafkaCluster; const AParams: TKafkaConsumerProtocolParams);
    /// Close, then frees.
    destructor Destroy; override;

    // IKafkaGroupMembership (Kafka.Group).
    function GroupId: string;
    function MemberId: string;
    function MemberEpoch: Int32;
    function State: TKafkaMemberState;
    procedure Subscribe(const ATopics: array of string);
    function Poll(AHandler: TKafkaGroupEventHandler): Integer;
    function Assignment: TKafkaTopicPartitionArray;
    procedure Commit(const AOffsets: TKafkaCommittedOffsetArray; ATimeoutMs: Cardinal);
    function Committed(const APartitions: array of TKafkaTopicPartition;
      ATimeoutMs: Cardinal): TKafkaCommittedOffsetArray;
    procedure Leave(AHandler: TKafkaGroupEventHandler; ATimeoutMs: Cardinal);
    procedure Close;

    /// Heartbeats sent (for tests and diagnostics).
    function HeartbeatCount: Integer;
    /// The interval the coordinator gave (0 before its first answer).
    function HeartbeatIntervalMs: Cardinal;
    property Coordinator: TKafkaGroupCoordinator read FCoordinator;
  end;

/// AGroupId, every other field at Java's default.
function KafkaConsumerProtocolParams(const AGroupId: string): TKafkaConsumerProtocolParams;
/// A member id as KIP-1082 has the client make it: a random (version 4)
/// UUID in Kafka's string form, never one that starts with '-' (Java's
/// Uuid.randomUuid).
function KafkaNewMemberId: string;

implementation

uses
  PascalCommon.Threading,
  PascalCommon.Random,
  Kafka.Errors,
  Kafka.Messages.Apis,
  Kafka.Messages.ConsumerGroupHeartbeat,
  Kafka.Messages.OffsetCommit,
  Kafka.Messages.OffsetFetch;

const
  JOIN_EPOCH = 0;
  LEAVE_EPOCH = -1;
  /// The longest the heartbeat thread sleeps without a reason to wake: a
  /// guard, every change pulses the hub anyway.
  IDLE_WAIT_MS = 1000;
  NOT_SUPPORTED_MESSAGE = 'The cluster doesn''t support the consumer group protocol of ' +
    'KIP-848 (ConsumerGroupHeartbeat): it needs Kafka 4.0 or later, with the protocol on';

function KafkaConsumerProtocolParams(const AGroupId: string): TKafkaConsumerProtocolParams;
begin
  Result := Default(TKafkaConsumerProtocolParams);
  Result.GroupId := AGroupId;
  Result.RebalanceTimeoutMs := KAFKA_DEFAULT_MAX_POLL_INTERVAL_MS;
end;

function KafkaNewMemberId: string;
var
  LId: TKafkaUuid;
  LBytes: TBytes;
begin
  LId := TKafkaUuid.Zero;
  repeat
    LBytes := PcRandomBytes(16);
    Move(LBytes[0], LId.Bytes[0], 16);
    LId.Bytes[6] := (LId.Bytes[6] and $0F) or $40;
    LId.Bytes[8] := (LId.Bytes[8] and $3F) or $80;
    Result := LId.ToString;
  until (Result <> '') and (Result[1] <> '-');
end;

{ Assignment helpers }

procedure SortInt32(var A: TKafkaInt32Array);
var
  I, J: Integer;
  V: Int32;
begin
  for I := 1 to High(A) do
  begin
    V := A[I];
    J := I - 1;
    while (J >= 0) and (A[J] > V) do
    begin
      A[J + 1] := A[J];
      Dec(J);
    end;
    A[J + 1] := V;
  end;
end;

function SameInt32s(const A, B: TKafkaInt32Array): Boolean;
var
  I: Integer;
begin
  if Length(A) <> Length(B) then
    Exit(False);
  for I := 0 to High(A) do
    if A[I] <> B[I] then
      Exit(False);
  Result := True;
end;

function SamePartitions(const A, B: TKafkaConsumerProtocol.TAssignment): Boolean;
var
  I: Integer;
begin
  if Length(A.Topics) <> Length(B.Topics) then
    Exit(False);
  for I := 0 to High(A.Topics) do
    if not A.Topics[I].TopicId.Equals(B.Topics[I].TopicId) or
      not SameInt32s(A.Topics[I].Partitions, B.Topics[I].Partitions) then
      Exit(False);
  Result := True;
end;

function SameAssignment(const A, B: TKafkaConsumerProtocol.TAssignment): Boolean;
begin
  Result := (A.LocalEpoch = B.LocalEpoch) and SamePartitions(A, B);
end;

function NoAssignment: TKafkaConsumerProtocol.TAssignment;
begin
  Result.LocalEpoch := -1;
  Result.Topics := nil;
end;

function AssignmentKey(const A: TKafkaConsumerProtocol.TAssignment): string;
var
  I, K: Integer;
begin
  Result := IntToStr(A.LocalEpoch);
  for I := 0 to High(A.Topics) do
  begin
    Result := Result + ';' + A.Topics[I].TopicId.ToString;
    for K := 0 to High(A.Topics[I].Partitions) do
      Result := Result + ',' + IntToStr(A.Topics[I].Partitions[K]);
  end;
end;

function IndexOfPartition(const A: TKafkaTopicPartitionArray; const ATopic: string;
  APartition: Int32): Integer;
var
  I: Integer;
begin
  for I := 0 to High(A) do
    if (A[I].Partition = APartition) and (A[I].Topic = ATopic) then
      Exit(I);
  Result := -1;
end;

/// The partitions of A that aren't in B, in A's order.
function Minus(const A, B: TKafkaTopicPartitionArray): TKafkaTopicPartitionArray;
var
  I, N: Integer;
begin
  Result := nil;
  N := 0;
  for I := 0 to High(A) do
    if IndexOfPartition(B, A[I].Topic, A[I].Partition) < 0 then
    begin
      SetLength(Result, N + 1);
      Result[N] := A[I];
      Inc(N);
    end;
end;

procedure Append(var A: TKafkaTopicPartitionArray; const B: TKafkaTopicPartitionArray);
var
  I, N: Integer;
begin
  for I := 0 to High(B) do
    if IndexOfPartition(A, B[I].Topic, B[I].Partition) < 0 then
    begin
      N := Length(A);
      SetLength(A, N + 1);
      A[N] := B[I];
    end;
end;

{ TKafkaConsumerProtocol.THeartbeatThread }

constructor TKafkaConsumerProtocol.THeartbeatThread.Create(AOwner: TKafkaConsumerProtocol);
begin
  inherited Create(True);
  FOwner := AOwner;
  FreeOnTerminate := False;
end;

procedure TKafkaConsumerProtocol.THeartbeatThread.Execute;
begin
  FOwner.Run;
end;

{ TKafkaConsumerProtocol }

constructor TKafkaConsumerProtocol.Create(ACluster: TKafkaCluster;
  const AParams: TKafkaConsumerProtocolParams);
begin
  inherited Create;
  if AParams.GroupId = '' then
    raise EArgumentException.Create('A group id is required');
  if AParams.RebalanceTimeoutMs > Cardinal(High(Int32)) then
    raise EArgumentException.CreateFmt('RebalanceTimeoutMs %d is too large',
      [AParams.RebalanceTimeoutMs]);
  FCluster := ACluster;
  FParams := AParams;
  FRebalanceTimeoutMs := Int32(AParams.RebalanceTimeoutMs);
  if FRebalanceTimeoutMs = 0 then
    FRebalanceTimeoutMs := KAFKA_DEFAULT_MAX_POLL_INTERVAL_MS;
  FRetryBackoffMs := AParams.RetryBackoffMs;
  if FRetryBackoffMs = 0 then
    FRetryBackoffMs := ACluster.Params.RetryBackoffMs;
  if FRetryBackoffMs = 0 then
    FRetryBackoffMs := KAFKA_DEFAULT_RETRY_BACKOFF_MS;
  FRetryBackoffMaxMs := AParams.RetryBackoffMaxMs;
  if FRetryBackoffMaxMs = 0 then
    FRetryBackoffMaxMs := KAFKA_DEFAULT_RETRY_BACKOFF_MAX_MS;
  if FRetryBackoffMaxMs < FRetryBackoffMs then
    FRetryBackoffMaxMs := FRetryBackoffMs;
  FRequestTimeoutMs := ACluster.Params.Connection.RequestTimeoutMs;
  if FRequestTimeoutMs = 0 then
    FRequestTimeoutMs := KAFKA_DEFAULT_REQUEST_TIMEOUT_MS;
  FHub := TKafkaWaitHub.Create;
  FHubRef := FHub;
  FCoordinator := TKafkaGroupCoordinator.Create(ACluster, AParams.GroupId, FRetryBackoffMs,
    FRetryBackoffMaxMs, FHub);
  FNames := TDictionary<string, string>.Create;
  FMemberId := KafkaNewMemberId;
  FState := kmsUnsubscribed;
  FEpoch := JOIN_EPOCH;
  FTarget := NoAssignment;
  FCurrent := NoAssignment;
  FPending := NoAssignment;
  ResetSentFields;
end;

destructor TKafkaConsumerProtocol.Destroy;
begin
  if FHub <> nil then
    Close;
  FThread.Free;
  FCoordinator.Free;
  FNames.Free;
  FInFlight := nil;
  FInFlightBroker := nil;
  FHubRef := nil;
  inherited;
end;

function TKafkaConsumerProtocol.GroupId: string;
begin
  Result := FParams.GroupId;
end;

function TKafkaConsumerProtocol.MemberId: string;
begin
  FHub.Enter;
  try
    Result := FMemberId;
  finally
    FHub.Leave;
  end;
end;

function TKafkaConsumerProtocol.MemberEpoch: Int32;
begin
  FHub.Enter;
  try
    Result := FEpoch;
  finally
    FHub.Leave;
  end;
end;

function TKafkaConsumerProtocol.State: TKafkaMemberState;
begin
  FHub.Enter;
  try
    Result := FState;
  finally
    FHub.Leave;
  end;
end;

function TKafkaConsumerProtocol.HeartbeatCount: Integer;
begin
  FHub.Enter;
  try
    Result := FHeartbeatCount;
  finally
    FHub.Leave;
  end;
end;

function TKafkaConsumerProtocol.HeartbeatIntervalMs: Cardinal;
begin
  FHub.Enter;
  try
    Result := FHeartbeatIntervalMs;
  finally
    FHub.Leave;
  end;
end;

procedure TKafkaConsumerProtocol.CurrentMember(out AMemberId: string; out AEpoch: Int32);
begin
  FHub.Enter;
  try
    AMemberId := FMemberId;
    AEpoch := FEpoch;
  finally
    FHub.Leave;
  end;
end;

{ Under the lock }

procedure TKafkaConsumerProtocol.ResetSentFields;
begin
  FSentRebalanceTimeoutMs := -1;
  FSentSubscription := False;
  FSentSubscriptionKey := '';
  FSentServerAssignor := False;
  FSentAssignment := False;
  FSentAssignmentKey := '';
end;

procedure TKafkaConsumerProtocol.Backoff;
begin
  FBackoffUntil := PcTickMs + KafkaBackoffMs(FFailures, FRetryBackoffMs, FRetryBackoffMaxMs);
  Inc(FFailures);
  FNextHeartbeatAt := FBackoffUntil;
end;

procedure TKafkaConsumerProtocol.SetError(AClass: EKafkaClass; ACode: Int16;
  const AMessage: string; AFatal: Boolean);
begin
  FHasError := True;
  FErrorClass := AClass;
  FErrorCode := ACode;
  FErrorMessage := AMessage;
  if AFatal and (FState <> kmsFatal) then
  begin
    // Java's transitionToFatal: the partitions are released as lost.
    DropQueuedEvents;
    FReconciling := False;
    if (Length(FOwned) > 0) or FDelivering then
      Queue(kgeLost, nil);
    FState := kmsFatal;
  end;
  FHub.PulseAll;
  if FParams.Notify <> nil then
    FParams.Notify.Signal;
end;

procedure TKafkaConsumerProtocol.Queue(AKind: TKafkaGroupEventKind;
  const APartitions: TKafkaTopicPartitionArray);
var
  N: Integer;
begin
  N := Length(FEvents);
  SetLength(FEvents, N + 1);
  FEvents[N].Kind := AKind;
  FEvents[N].Partitions := APartitions;
  if FParams.Notify <> nil then
    FParams.Notify.Signal;
end;

procedure TKafkaConsumerProtocol.DropQueuedEvents;
begin
  // The one being handled, if any, is no longer in the queue.
  FEvents := nil;
  FEventHead := 0;
end;

function TKafkaConsumerProtocol.EventsIdle: Boolean;
begin
  Result := (FEventHead >= Length(FEvents)) and not FDelivering;
end;

procedure TKafkaConsumerProtocol.AfterEvents;
begin
  if not EventsIdle then
    Exit;
  if FReconciling then
  begin
    FReconciling := False;
    // Not if the member was fenced, left or failed meanwhile (Java's
    // maybeAbortReconciliation).
    if FState = kmsReconciling then
    begin
      FCurrent := FPending;
      FState := kmsAcknowledging;
    end;
  end;
  if FState = kmsFenced then
    Rejoin;
end;

procedure TKafkaConsumerProtocol.Rejoin;
begin
  FCurrent := NoAssignment;
  FTarget := NoAssignment;
  FNames.Clear;
  FState := kmsJoining;
  FEpoch := JOIN_EPOCH;
  ResetSentFields;
  FNextHeartbeatAt := 0;
end;

procedure TKafkaConsumerProtocol.TransitionToFenced;
begin
  if FState in [kmsUnsubscribed, kmsFatal, kmsPrepareLeaving, kmsLeaving] then
    Exit;
  FState := kmsFenced;
  FEpoch := JOIN_EPOCH;
  DropQueuedEvents;
  FReconciling := False;
  // Its partitions as the consumer holds them when it takes the event.
  if (Length(FOwned) > 0) or FDelivering then
    Queue(kgeLost, nil);
  FBackoffUntil := 0;
  FNextHeartbeatAt := 0;
end;

function TKafkaConsumerProtocol.FindTopicName(const AMetadata: IKafkaClusterMetadata;
  const AId: TKafkaUuid; out AName: string): Boolean;
var
  I: Integer;
  LInfo: TKafkaTopicInfo;
begin
  if FNames.TryGetValue(AId.ToString, AName) then
    Exit(True);
  if AMetadata <> nil then
    for I := 0 to High(FSubscription) do
      if AMetadata.FindTopic(FSubscription[I], LInfo) and LInfo.TopicId.Equals(AId) then
      begin
        AName := LInfo.Name;
        FNames.AddOrSetValue(AId.ToString, AName);
        Exit(True);
      end;
  Result := False;
end;

function TKafkaConsumerProtocol.Reconcile: Boolean;
var
  LMetadata: IKafkaClusterMetadata;
  LResolved: TKafkaConsumerProtocol.TAssignment;
  LAssigned, LRevoked, LAdded: TKafkaTopicPartitionArray;
  LName: string;
  I, K, N: Integer;
begin
  Result := False;
  if SameAssignment(FCurrent, FTarget) then
    Exit;
  LMetadata := FCluster.Metadata;
  LResolved.LocalEpoch := FTarget.LocalEpoch;
  LResolved.Topics := nil;
  LAssigned := nil;
  for I := 0 to High(FTarget.Topics) do
  begin
    if not FindTopicName(LMetadata, FTarget.Topics[I].TopicId, LName) then
    begin
      Result := True; // a metadata refresh may name it
      Continue;
    end;
    N := Length(LResolved.Topics);
    SetLength(LResolved.Topics, N + 1);
    LResolved.Topics[N] := FTarget.Topics[I];
    for K := 0 to High(FTarget.Topics[I].Partitions) do
    begin
      N := Length(LAssigned);
      SetLength(LAssigned, N + 1);
      LAssigned[N] := KafkaTopicPartition(LName, FTarget.Topics[I].Partitions[K]);
    end;
  end;
  if (FCurrent.LocalEpoch <> -1) and SamePartitions(LResolved, FCurrent) then
  begin
    // Nothing to hand the consumer; a new target with the same partitions
    // (or the part of it that has names) is still acknowledged.
    if FCurrent.LocalEpoch <> LResolved.LocalEpoch then
    begin
      FCurrent := LResolved;
      FState := kmsAcknowledging;
    end;
    Exit;
  end;
  LRevoked := Minus(FOwned, LAssigned);
  LAdded := Minus(LAssigned, FOwned);
  FPending := LResolved;
  FReconciling := True;
  if Length(LRevoked) > 0 then
    Queue(kgeRevoked, LRevoked);
  Queue(kgeAssigned, LAdded);
end;

function TKafkaConsumerProtocol.NextStep(out AWaitMs: Cardinal; out AFuture: IKafkaResponseFuture;
  out ABroker: IKafkaBrokerConnection; out AVersion: Int16): TStep;
var
  LNow, LWake: UInt64;
  LUrgent: Boolean;

  procedure WakeAt(AAt: UInt64);
  begin
    if AAt < LWake then
      LWake := AAt;
  end;

begin
  AWaitMs := 0;
  AFuture := nil;
  ABroker := nil;
  AVersion := 0;
  if FHub.StopRequested then
    Exit(stExit);
  LNow := PcTickMs;
  LWake := LNow + IDLE_WAIT_MS;
  if FInFlight <> nil then
  begin
    if FInFlight.IsDone or (LNow >= FInFlightDeadline) then
    begin
      AFuture := FInFlight;
      ABroker := FInFlightBroker;
      AVersion := FInFlightVersion;
      FInFlight := nil;
      FInFlightBroker := nil;
      if AFuture.IsDone then
        Exit(stAnswer);
      Exit(stTimedOut);
    end;
    WakeAt(FInFlightDeadline);
  end;
  AfterEvents;
  if (FState = kmsReconciling) and not FReconciling and EventsIdle then
  begin
    FNeedMetadata := Reconcile;
    if FNeedMetadata then
    begin
      if LNow >= FNextMetadataAt then
      begin
        FNextMetadataAt := LNow + FRetryBackoffMs;
        Exit(stRefreshMetadata);
      end;
      WakeAt(FNextMetadataAt);
    end;
  end;
  if FSubscriptionChanged then
  begin
    // The subscription's metadata first (Java asks for it on subscribe),
    // so that the first assignment can be named at once.
    FSubscriptionChanged := False;
    FNextMetadataAt := LNow + FRetryBackoffMs;
    Exit(stRefreshMetadata);
  end;
  if (FInFlight = nil) and (FState in [kmsJoining, kmsReconciling, kmsAcknowledging, kmsStable]) then
  begin
    // Java's shouldHeartbeatNow: joining and acknowledging don't wait for
    // the interval (they do wait out a backoff here).
    LUrgent := FState in [kmsJoining, kmsAcknowledging];
    if LUrgent and (LNow >= FBackoffUntil) then
      Exit(stSend);
    if LNow >= FNextHeartbeatAt then
      Exit(stSend);
    if LUrgent then
      WakeAt(FBackoffUntil)
    else
      WakeAt(FNextHeartbeatAt);
  end;
  AWaitMs := Cardinal(LWake - LNow);
  if AWaitMs = 0 then
    AWaitMs := 1;
  Result := stWait;
end;

procedure TKafkaConsumerProtocol.Run;
var
  LStep: TStep;
  LWait: Cardinal;
  LFuture: IKafkaResponseFuture;
  LBroker: IKafkaBrokerConnection;
  LVersion: Int16;
begin
  try
    while True do
    begin
      FHub.Enter;
      try
        LStep := NextStep(LWait, LFuture, LBroker, LVersion);
        if LStep = stWait then
          FHub.Wait(LWait);
      finally
        FHub.Leave;
      end;
      case LStep of
        stExit:
          Exit;
        stAnswer:
          HandleAnswer(LFuture, LBroker, LVersion);
        stTimedOut:
          begin
            LBroker.Connection.Abandon(LFuture);
            HeartbeatFailed(Format('ConsumerGroupHeartbeat to %s got no answer in %d ms',
              [LBroker.Connection.Address, FRequestTimeoutMs]));
          end;
        stSend:
          SendHeartbeat;
        stRefreshMetadata:
          RefreshMetadata;
      end;
      LFuture := nil;
      LBroker := nil;
    end;
  except
    on E: Exception do
    begin
      FHub.Enter;
      try
        if not FHub.StopRequested then
          SetError(EKafka, 0, 'The group''s heartbeat thread failed: ' + E.Message, True);
      finally
        FHub.Leave;
      end;
    end;
  end;
end;

procedure TKafkaConsumerProtocol.RefreshMetadata;
var
  LTopics: TKafkaStringArray;
begin
  FHub.Enter;
  try
    LTopics := Copy(FSubscription, 0, Length(FSubscription));
  finally
    FHub.Leave;
  end;
  try
    FCluster.Refresh(LTopics);
  except
    on EKafka do ; // the next attempt, after RetryBackoffMs
  end;
end;

procedure TKafkaConsumerProtocol.HeartbeatFailed(const AReason: string);
begin
  // Java's onFailure: a retriable failure; the coordinator is looked up
  // again (its connection may be the problem).
  FCoordinator.MarkUnknown(AReason);
  FHub.Enter;
  try
    ResetSentFields;
    Backoff;
  finally
    FHub.Leave;
  end;
end;

procedure TKafkaConsumerProtocol.SendHeartbeat;
var
  LBroker: IKafkaBrokerConnection;
  LWait: Cardinal;
  LVersion: Int16;
  R: TKafkaConsumerGroupHeartbeatRequest;
  LAll: Boolean;
  LFuture: IKafkaResponseFuture;
  LBody: TBytes;
  I, N: Integer;
begin
  try
    if not FCoordinator.TryConnection(LBroker, LWait) then
    begin
      FHub.Enter;
      try
        if PcTickMs + LWait > FNextHeartbeatAt then
          FNextHeartbeatAt := PcTickMs + LWait;
        if FNextHeartbeatAt > FBackoffUntil then
          FBackoffUntil := FNextHeartbeatAt;
      finally
        FHub.Leave;
      end;
      Exit;
    end;
  except
    on E: EKafkaBrokerError do
    begin
      FHub.Enter;
      try
        SetError(EKafkaBrokerError, E.ErrorCode, E.Message, True);
      finally
        FHub.Leave;
      end;
      Exit;
    end;
    on E: EKafkaConnection do
    begin
      if FHub.Stopped then
        Exit;
      raise;
    end;
    on E: EKafka do
    begin
      FHub.Enter;
      try
        SetError(EKafkaClass(E.ClassType), 0, E.Message, True);
      finally
        FHub.Leave;
      end;
      Exit;
    end;
  end;
  if not LBroker.Connection.TryNegotiatedVersion(KAFKA_API_KEY_CONSUMER_GROUP_HEARTBEAT, LVersion) then
  begin
    FHub.Enter;
    try
      SetError(EKafkaUnsupportedVersion, 0, NOT_SUPPORTED_MESSAGE, True);
    finally
      FHub.Leave;
    end;
    Exit;
  end;
  FHub.Enter;
  try
    if (FInFlight <> nil) or not (FState in [kmsJoining, kmsReconciling, kmsAcknowledging,
      kmsStable]) then
      Exit;
    // Java's HeartbeatState.buildRequestData.
    R.Init;
    R.GroupId := FParams.GroupId;
    R.MemberId := FMemberId;
    R.MemberEpoch := FEpoch;
    LAll := FState = kmsJoining;
    if LAll or (FSentRebalanceTimeoutMs <> FRebalanceTimeoutMs) then
    begin
      R.RebalanceTimeoutMs := FRebalanceTimeoutMs;
      FSentRebalanceTimeoutMs := FRebalanceTimeoutMs;
    end;
    if LAll or not FSentSubscription or (FSentSubscriptionKey <> FSubscriptionKey) then
    begin
      R.SubscribedTopicNames := Copy(FSubscription, 0, Length(FSubscription));
      R.SubscribedTopicNamesIsNull := False;
      FSentSubscription := True;
      FSentSubscriptionKey := FSubscriptionKey;
    end;
    if (FParams.ServerAssignor <> '') and (LAll or not FSentServerAssignor) then
    begin
      R.ServerAssignor := FParams.ServerAssignor;
      R.ServerAssignorIsNull := False;
      FSentServerAssignor := True;
    end;
    if LAll or not FSentAssignment or (FSentAssignmentKey <> AssignmentKey(FCurrent)) then
    begin
      N := Length(FCurrent.Topics);
      SetLength(R.TopicPartitions, N);
      for I := 0 to N - 1 do
      begin
        R.TopicPartitions[I].Init;
        R.TopicPartitions[I].TopicId := FCurrent.Topics[I].TopicId;
        R.TopicPartitions[I].Partitions := Copy(FCurrent.Topics[I].Partitions, 0,
          Length(FCurrent.Topics[I].Partitions));
      end;
      R.TopicPartitionsIsNull := False;
      FSentAssignment := True;
      FSentAssignmentKey := AssignmentKey(FCurrent);
    end;
    if LAll and (FParams.RackId <> '') then
    begin
      R.RackId := FParams.RackId;
      R.RackIdIsNull := False;
    end;
    // Java's onHeartbeatRequestGenerated.
    if FState = kmsAcknowledging then
    begin
      if SameAssignment(FCurrent, FTarget) then
        FState := kmsStable
      else
        FState := kmsReconciling;
    end;
    try
      LBody := R.ToBytes(LVersion);
    except
      on E: EKafkaUnsupportedVersion do
      begin
        SetError(EKafkaUnsupportedVersion, 0, E.Message, True);
        Exit;
      end;
    end;
    // This thread is the only one that sends heartbeats: the bookkeeping
    // can be done before the write, which goes outside the lock (the
    // connection's reader needs it to signal answers).
    FLastSentAt := PcTickMs;
    FNextHeartbeatAt := FLastSentAt + FHeartbeatIntervalMs;
    Inc(FHeartbeatCount);
  finally
    FHub.Leave;
  end;
  try
    LFuture := LBroker.Connection.Send(KAFKA_API_KEY_CONSUMER_GROUP_HEARTBEAT, LVersion, LBody,
      FHubRef);
  except
    on E: EKafka do
    begin
      // The connection broke: a failed heartbeat.
      HeartbeatFailed(E.Message);
      Exit;
    end;
  end;
  FHub.Enter;
  try
    FInFlight := LFuture;
    FInFlightBroker := LBroker;
    FInFlightVersion := LVersion;
    FInFlightDeadline := PcTickMs + FRequestTimeoutMs;
  finally
    FHub.Leave;
  end;
end;

procedure TKafkaConsumerProtocol.HandleAnswer(const AFuture: IKafkaResponseFuture;
  const ABroker: IKafkaBrokerConnection; AVersion: Int16);
var
  R: TKafkaConsumerGroupHeartbeatResponse;
  LBody: TBytes;
  LMarkUnknown: Boolean;
  LMessage: string;
  LTarget: TKafkaConsumerProtocol.TAssignment;
  I, K, J: Integer;
  LKey: string;
  LTopic: TTopicAssignment;
begin
  try
    LBody := AFuture.Body;
  except
    on E: EKafkaConnection do
    begin
      HeartbeatFailed(E.Message);
      Exit;
    end;
    on E: EKafkaTimeout do
    begin
      HeartbeatFailed(E.Message);
      Exit;
    end;
  end;
  R.FromBytes(LBody, AVersion);
  LMarkUnknown := False;
  FHub.Enter;
  try
    if R.ErrorCode = 0 then
    begin
      FFailures := 0;
      FBackoffUntil := 0;
      if R.HeartbeatIntervalMs > 0 then
        FHeartbeatIntervalMs := Cardinal(R.HeartbeatIntervalMs);
      FNextHeartbeatAt := FLastSentAt + FHeartbeatIntervalMs;
      // Java's onHeartbeatSuccess.
      if FState in [kmsUnsubscribed, kmsFenced, kmsFatal, kmsPrepareLeaving, kmsLeaving] then
        Exit;
      if R.MemberEpoch < 0 then
        Exit;
      if not R.MemberIdIsNull and (R.MemberId <> '') then
        FMemberId := R.MemberId; // v0: the coordinator may choose it
      FEpoch := R.MemberEpoch;
      if R.AssignmentIsNull then
        Exit;
      // Java's processAssignmentReceived: a new target only when the
      // partitions differ (its local epoch goes up).
      LTarget.LocalEpoch := FTarget.LocalEpoch;
      SetLength(LTarget.Topics, Length(R.Assignment.TopicPartitions));
      for I := 0 to High(R.Assignment.TopicPartitions) do
      begin
        LTarget.Topics[I].TopicId := R.Assignment.TopicPartitions[I].TopicId;
        LTarget.Topics[I].Partitions := Copy(R.Assignment.TopicPartitions[I].Partitions, 0,
          Length(R.Assignment.TopicPartitions[I].Partitions));
        SortInt32(LTarget.Topics[I].Partitions);
      end;
      // Topics by id, for a comparison that doesn't depend on the order.
      for I := 1 to High(LTarget.Topics) do
      begin
        LTopic := LTarget.Topics[I];
        LKey := LTopic.TopicId.ToString;
        J := I - 1;
        while (J >= 0) and (LTarget.Topics[J].TopicId.ToString > LKey) do
        begin
          LTarget.Topics[J + 1] := LTarget.Topics[J];
          Dec(J);
        end;
        LTarget.Topics[J + 1] := LTopic;
      end;
      // A topic with no partitions is no assignment.
      K := 0;
      for I := 0 to High(LTarget.Topics) do
        if Length(LTarget.Topics[I].Partitions) > 0 then
        begin
          LTarget.Topics[K] := LTarget.Topics[I];
          Inc(K);
        end;
      SetLength(LTarget.Topics, K);
      if (FTarget.LocalEpoch = -1) or not SamePartitions(FTarget, LTarget) then
      begin
        LTarget.LocalEpoch := FTarget.LocalEpoch + 1;
        FTarget := LTarget;
      end;
      if not SameAssignment(FCurrent, FTarget) then
        FState := kmsReconciling
      else if FState in [kmsReconciling, kmsJoining] then
        FState := kmsStable;
      Exit;
    end;
    // Java's onErrorResponse.
    ResetSentFields;
    LMessage := Format('ConsumerGroupHeartbeat for group ''%s'' to %s: %s',
      [FParams.GroupId, ABroker.Connection.Address, KafkaErrorText(R.ErrorCode)]);
    if not R.ErrorMessageIsNull and (R.ErrorMessage <> '') then
      LMessage := LMessage + ' (' + R.ErrorMessage + ')';
    case R.ErrorCode of
      KAFKA_ERROR_NOT_COORDINATOR, KAFKA_ERROR_COORDINATOR_NOT_AVAILABLE:
        begin
          LMarkUnknown := True;
          FBackoffUntil := 0;
          FNextHeartbeatAt := 0;
        end;
      KAFKA_ERROR_COORDINATOR_LOAD_IN_PROGRESS:
        Backoff;
      KAFKA_ERROR_FENCED_MEMBER_EPOCH, KAFKA_ERROR_UNKNOWN_MEMBER_ID:
        TransitionToFenced;
      KAFKA_ERROR_TOPIC_AUTHORIZATION_FAILED:
        begin
          SetError(EKafkaBrokerError, R.ErrorCode, LMessage, False);
          Backoff;
        end;
      KAFKA_ERROR_UNSUPPORTED_VERSION:
        SetError(EKafkaUnsupportedVersion, R.ErrorCode, NOT_SUPPORTED_MESSAGE, True);
    else
      SetError(EKafkaBrokerError, R.ErrorCode, LMessage, True);
    end;
  finally
    FHub.PulseAll;
    FHub.Leave;
  end;
  if LMarkUnknown then
    FCoordinator.MarkUnknown(LMessage);
end;

{ The consumer's side }

procedure TKafkaConsumerProtocol.Subscribe(const ATopics: array of string);
var
  L: TStringList;
  I: Integer;
  LKey: string;
begin
  if Length(ATopics) = 0 then
    raise EArgumentException.Create('Subscribe needs at least one topic');
  L := TStringList.Create;
  try
    L.CaseSensitive := True; // topic names are
    L.Sorted := True;
    L.Duplicates := dupIgnore;
    for I := 0 to High(ATopics) do
    begin
      if ATopics[I] = '' then
        raise EArgumentException.Create('A topic name is empty');
      L.Add(ATopics[I]);
    end;
    FHub.Enter;
    try
      if FClosed or FLeft then
        raise EKafka.Create('The group membership is closed');
      if FState = kmsFatal then
        raise EKafka.CreateFmt('The group membership failed: %s', [FErrorMessage]);
      SetLength(FSubscription, L.Count);
      LKey := '';
      for I := 0 to L.Count - 1 do
      begin
        FSubscription[I] := L[I];
        LKey := LKey + L[I] + #0;
      end;
      if LKey <> FSubscriptionKey then
        FSubscriptionChanged := True;
      FSubscriptionKey := LKey;
      if FState = kmsUnsubscribed then
      begin
        FState := kmsJoining;
        FEpoch := JOIN_EPOCH;
        ResetSentFields;
        FNextHeartbeatAt := 0;
      end;
      FHub.PulseAll;
    finally
      FHub.Leave;
    end;
  finally
    L.Free;
  end;
  if FThread = nil then
  begin
    FThread := THeartbeatThread.Create(Self);
    FThread.Start;
  end;
end;

function TKafkaConsumerProtocol.Poll(AHandler: TKafkaGroupEventHandler): Integer;
var
  LEvent: TQueuedEvent;
  LMine: TKafkaTopicPartitionArray;
  LClass: EKafkaClass;
  LCode: Int16;
  LMessage: string;
begin
  Result := 0;
  while True do
  begin
    FHub.Enter;
    try
      if FEventHead >= Length(FEvents) then
        Break;
      LEvent := FEvents[FEventHead];
      Inc(FEventHead);
      if FEventHead >= Length(FEvents) then
        DropQueuedEvents;
      FDelivering := True;
      case LEvent.Kind of
        kgeLost:
          LEvent.Partitions := Copy(FOwned, 0, Length(FOwned));
        kgeRevoked:
          begin
            LMine := Minus(LEvent.Partitions, Minus(LEvent.Partitions, FOwned));
            LEvent.Partitions := LMine;
          end;
      end;
    finally
      FHub.Leave;
    end;
    try
      if Assigned(AHandler) and ((LEvent.Kind = kgeAssigned) or (Length(LEvent.Partitions) > 0)) then
        AHandler(LEvent.Kind, LEvent.Partitions);
    finally
      FHub.Enter;
      try
        case LEvent.Kind of
          kgeRevoked:
            FOwned := Minus(FOwned, LEvent.Partitions);
          kgeAssigned:
            Append(FOwned, LEvent.Partitions);
          kgeLost:
            FOwned := nil;
        end;
        FDelivering := False;
        FHub.PulseAll;
      finally
        FHub.Leave;
      end;
    end;
    Inc(Result);
  end;
  FHub.Enter;
  try
    if not FHasError then
      Exit;
    FHasError := False;
    LClass := FErrorClass;
    LCode := FErrorCode;
    LMessage := FErrorMessage;
  finally
    FHub.Leave;
  end;
  if LClass = EKafkaBrokerError then
    raise EKafkaBrokerError.Create(LCode, LMessage);
  raise LClass.Create(LMessage);
end;

function TKafkaConsumerProtocol.Assignment: TKafkaTopicPartitionArray;
begin
  FHub.Enter;
  try
    Result := Copy(FOwned, 0, Length(FOwned));
  finally
    FHub.Leave;
  end;
end;

procedure TKafkaConsumerProtocol.StopThread;
begin
  FHub.Stop;
  if FThread <> nil then
    FThread.WaitFor;
end;

procedure TKafkaConsumerProtocol.Leave(AHandler: TKafkaGroupEventHandler; ATimeoutMs: Cardinal);
var
  LInGroup: Boolean;
  LEpoch: Int32;
  LOwned: TKafkaTopicPartitionArray;
  LBroker: IKafkaBrokerConnection;
  LVersion: Int16;
  R: TKafkaConsumerGroupHeartbeatRequest;
  LFuture: IKafkaResponseFuture;
begin
  if FClosed or FLeft then
    Exit;
  FLeft := True;
  FHub.Enter;
  try
    LInGroup := (FThread <> nil) and not (FState in [kmsUnsubscribed, kmsFatal]);
    LEpoch := FEpoch;
    if LInGroup then
      FState := kmsPrepareLeaving;
    DropQueuedEvents;
    FReconciling := False;
    LOwned := Copy(FOwned, 0, Length(FOwned));
  finally
    FHub.Leave;
  end;
  try
    if (Length(LOwned) > 0) and Assigned(AHandler) then
    begin
      // Java's invokeOnPartitionsRevokedOrLostToReleaseAssignment.
      if LEpoch > 0 then
        AHandler(kgeRevoked, LOwned)
      else
        AHandler(kgeLost, LOwned);
    end;
  finally
    FHub.Enter;
    try
      FOwned := nil;
      if LInGroup then
        FState := kmsLeaving;
    finally
      FHub.Leave;
    end;
    StopThread;
    if LInGroup and FCoordinator.KnownConnection(LBroker) and
      LBroker.Connection.TryNegotiatedVersion(KAFKA_API_KEY_CONSUMER_GROUP_HEARTBEAT, LVersion) then
    begin
      R.Init;
      R.GroupId := FParams.GroupId;
      R.MemberId := MemberId;
      R.MemberEpoch := LEAVE_EPOCH;
      try
        LFuture := LBroker.Connection.Send(KAFKA_API_KEY_CONSUMER_GROUP_HEARTBEAT, LVersion,
          R.ToBytes(LVersion));
        // Its answer changes nothing: the member is gone either way.
        LFuture.Wait(ATimeoutMs);
      except
        on EKafka do ;
      end;
    end;
    FHub.Enter;
    try
      FState := kmsUnsubscribed;
      FEpoch := LEAVE_EPOCH;
    finally
      FHub.Leave;
    end;
    Close;
  end;
end;

procedure TKafkaConsumerProtocol.Close;
begin
  if FClosed then
    Exit;
  FClosed := True;
  StopThread;
  FHub.Enter;
  try
    FInFlight := nil;
    FInFlightBroker := nil;
    DropQueuedEvents;
  finally
    FHub.Leave;
  end;
end;

{ Commits }

function TKafkaConsumerProtocol.TopicIdOf(const ATopic: string): TKafkaUuid;
var
  LMetadata: IKafkaClusterMetadata;
  LInfo: TKafkaTopicInfo;
begin
  LMetadata := FCluster.Metadata;
  if (LMetadata <> nil) and LMetadata.FindTopic(ATopic, LInfo) then
    Result := LInfo.TopicId
  else
    Result := TKafkaUuid.Zero;
end;

procedure TKafkaConsumerProtocol.RetryPause(AAttempt: Integer; ADeadline: UInt64;
  const AWhat, AReason: string; ATimeoutMs: Cardinal);
var
  LUntil: UInt64;
begin
  LUntil := PcTickMs + KafkaBackoffMs(AAttempt, FRetryBackoffMs, FRetryBackoffMaxMs);
  if LUntil > ADeadline then
    LUntil := ADeadline;
  while PcTickMs < LUntil do
    if not FHub.Pause(LUntil) then
      raise EKafkaConnection.Create('The group membership is closed');
  if PcTickMs >= ADeadline then
    raise EKafkaTimeout.CreateFmt('%s for group ''%s'' did not succeed in %d ms: %s',
      [AWhat, FParams.GroupId, ATimeoutMs, AReason]);
end;

procedure TKafkaConsumerProtocol.Commit(const AOffsets: TKafkaCommittedOffsetArray;
  ATimeoutMs: Cardinal);
var
  LDeadline: UInt64;
  LAttempt: Integer;
  LReason: string;
begin
  if Length(AOffsets) = 0 then
    Exit;
  if FClosed then
    raise EKafka.Create('The group membership is closed');
  LDeadline := PcTickMs + ATimeoutMs;
  LAttempt := 0;
  while not TryCommitOnce(AOffsets, LDeadline, LReason) do
  begin
    RetryPause(LAttempt, LDeadline, 'OffsetCommit', LReason, ATimeoutMs);
    Inc(LAttempt);
  end;
end;

function RemainingMs(ADeadline: UInt64): Cardinal;
var
  LNow: UInt64;
begin
  LNow := PcTickMs;
  if LNow >= ADeadline then
    Result := 0
  else
    Result := Cardinal(ADeadline - LNow);
end;

function TKafkaConsumerProtocol.TryCommitOnce(const AOffsets: TKafkaCommittedOffsetArray;
  ADeadline: UInt64; out AReason: string): Boolean;
var
  LBroker: IKafkaBrokerConnection;
  LVersion: Int16;
  LMember: string;
  LEpoch: Int32;
  R: TKafkaOffsetCommitRequest;
  LAnswer: TKafkaOffsetCommitResponse;
  LFuture: IKafkaResponseFuture;
  LBody: TBytes;
  LWait: UInt64;
  LAllIds: Boolean;
  I, T, P, N: Integer;
  LCode: Int16;
  LMessage: string;
begin
  Result := False;
  AReason := '';
  LBroker := FCoordinator.Connection(RemainingMs(ADeadline));
  LVersion := LBroker.Connection.NegotiatedVersion(KAFKA_API_KEY_OFFSET_COMMIT);
  CurrentMember(LMember, LEpoch);
  R.Init;
  R.GroupId := FParams.GroupId;
  if LEpoch > 0 then
  begin
    R.MemberId := LMember;
    R.GenerationIdOrMemberEpoch := LEpoch;
  end;
  // One entry per topic, in the order they first appear.
  LAllIds := True;
  for I := 0 to High(AOffsets) do
  begin
    T := 0;
    while (T < Length(R.Topics)) and (R.Topics[T].Name <> AOffsets[I].Topic) do
      Inc(T);
    if T = Length(R.Topics) then
    begin
      SetLength(R.Topics, T + 1);
      R.Topics[T].Init;
      R.Topics[T].Name := AOffsets[I].Topic;
      R.Topics[T].TopicId := TopicIdOf(AOffsets[I].Topic);
      if R.Topics[T].TopicId.IsZero then
        LAllIds := False;
    end;
    N := Length(R.Topics[T].Partitions);
    SetLength(R.Topics[T].Partitions, N + 1);
    R.Topics[T].Partitions[N].Init;
    R.Topics[T].Partitions[N].PartitionIndex := AOffsets[I].Partition;
    R.Topics[T].Partitions[N].CommittedOffset := AOffsets[I].Offset;
    R.Topics[T].Partitions[N].CommittedLeaderEpoch := AOffsets[I].LeaderEpoch;
    R.Topics[T].Partitions[N].CommittedMetadata := AOffsets[I].Metadata;
    R.Topics[T].Partitions[N].CommittedMetadataIsNull := False;
  end;
  // v10 names topics by id only (Java's canUseTopicIds).
  if (LVersion >= 10) and not LAllIds then
    LVersion := 9;
  try
    LFuture := LBroker.Connection.Send(KAFKA_API_KEY_OFFSET_COMMIT, LVersion, R.ToBytes(LVersion),
      FHubRef);
    LWait := PcTickMs + FRequestTimeoutMs;
    if not FHub.WaitFuture(LFuture, LWait) then
    begin
      LBroker.Connection.Abandon(LFuture);
      raise EKafkaTimeout.CreateFmt('OffsetCommit to %s got no answer in %d ms',
        [LBroker.Connection.Address, FRequestTimeoutMs]);
    end;
    LBody := LFuture.Body;
  except
    on E: EKafkaConnection do
    begin
      if FHub.Stopped then
        raise;
      FCoordinator.MarkUnknown(E.Message);
      AReason := E.Message;
      Exit;
    end;
    on E: EKafkaTimeout do
    begin
      FCoordinator.MarkUnknown(E.Message);
      AReason := E.Message;
      Exit;
    end;
  end;
  LAnswer.FromBytes(LBody, LVersion);
  for T := 0 to High(LAnswer.Topics) do
    for P := 0 to High(LAnswer.Topics[T].Partitions) do
    begin
      LCode := LAnswer.Topics[T].Partitions[P].ErrorCode;
      if LCode = 0 then
        Continue;
      LMessage := Format('OffsetCommit for group ''%s'' to %s: %s', [FParams.GroupId,
        LBroker.Connection.Address, KafkaErrorText(LCode)]);
      AReason := LMessage;
      case LCode of
        KAFKA_ERROR_COORDINATOR_NOT_AVAILABLE, KAFKA_ERROR_NOT_COORDINATOR,
        KAFKA_ERROR_REQUEST_TIMED_OUT:
          begin
            FCoordinator.MarkUnknown(LMessage);
            Exit;
          end;
        KAFKA_ERROR_COORDINATOR_LOAD_IN_PROGRESS, KAFKA_ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        KAFKA_ERROR_UNKNOWN_TOPIC_ID:
          Exit;
        KAFKA_ERROR_STALE_MEMBER_EPOCH:
          // Retried with the newer epoch while the member is in the group.
          if MemberEpoch > 0 then
            Exit;
      end;
      raise EKafkaBrokerError.Create(LCode, LMessage);
    end;
  Result := True;
end;

function TKafkaConsumerProtocol.Committed(const APartitions: array of TKafkaTopicPartition;
  ATimeoutMs: Cardinal): TKafkaCommittedOffsetArray;
var
  LDeadline: UInt64;
  LAttempt: Integer;
  LReason: string;
  I: Integer;
begin
  if FClosed then
    raise EKafka.Create('The group membership is closed');
  Result := nil;
  SetLength(Result, Length(APartitions));
  for I := 0 to High(APartitions) do
  begin
    Result[I].Topic := APartitions[I].Topic;
    Result[I].Partition := APartitions[I].Partition;
    Result[I].Offset := -1;
    Result[I].LeaderEpoch := -1;
    Result[I].Metadata := '';
  end;
  if Length(APartitions) = 0 then
    Exit;
  LDeadline := PcTickMs + ATimeoutMs;
  LAttempt := 0;
  while not TryCommittedOnce(APartitions, LDeadline, Result, LReason) do
  begin
    RetryPause(LAttempt, LDeadline, 'OffsetFetch', LReason, ATimeoutMs);
    Inc(LAttempt);
  end;
end;

function TKafkaConsumerProtocol.TryCommittedOnce(const APartitions: array of TKafkaTopicPartition;
  ADeadline: UInt64; var AResult: TKafkaCommittedOffsetArray; out AReason: string): Boolean;
var
  LBroker: IKafkaBrokerConnection;
  LVersion: Int16;
  LMember: string;
  LEpoch: Int32;
  R: TKafkaOffsetFetchRequest;
  LAnswer: TKafkaOffsetFetchResponse;
  LGroup: TKafkaOffsetFetchResponseGroup;
  LFuture: IKafkaResponseFuture;
  LBody: TBytes;
  LAllIds, LFound: Boolean;
  I, T, P, N, G: Integer;
  LCode: Int16;
  LMessage, LName: string;
begin
  Result := False;
  AReason := '';
  LBroker := FCoordinator.Connection(RemainingMs(ADeadline));
  LVersion := LBroker.Connection.NegotiatedVersion(KAFKA_API_KEY_OFFSET_FETCH);
  if LVersion < 8 then
    raise EKafkaUnsupportedVersion.CreateFmt('OffsetFetch v8 or later is needed for a consumer ' +
      'group; %s offers up to v%d', [LBroker.Connection.Address, LVersion]);
  CurrentMember(LMember, LEpoch);
  R.Init;
  R.RequireStable := True;
  SetLength(R.Groups, 1);
  R.Groups[0].Init;
  R.Groups[0].GroupId := FParams.GroupId;
  if LEpoch > 0 then
  begin
    R.Groups[0].MemberId := LMember;
    R.Groups[0].MemberIdIsNull := False;
    R.Groups[0].MemberEpoch := LEpoch;
  end;
  R.Groups[0].TopicsIsNull := False;
  LAllIds := True;
  for I := 0 to High(APartitions) do
  begin
    T := 0;
    while (T < Length(R.Groups[0].Topics)) and (R.Groups[0].Topics[T].Name <> APartitions[I].Topic) do
      Inc(T);
    if T = Length(R.Groups[0].Topics) then
    begin
      SetLength(R.Groups[0].Topics, T + 1);
      R.Groups[0].Topics[T].Init;
      R.Groups[0].Topics[T].Name := APartitions[I].Topic;
      R.Groups[0].Topics[T].TopicId := TopicIdOf(APartitions[I].Topic);
      if R.Groups[0].Topics[T].TopicId.IsZero then
        LAllIds := False;
    end;
    N := Length(R.Groups[0].Topics[T].PartitionIndexes);
    SetLength(R.Groups[0].Topics[T].PartitionIndexes, N + 1);
    R.Groups[0].Topics[T].PartitionIndexes[N] := APartitions[I].Partition;
  end;
  if (LVersion >= 10) and not LAllIds then
    LVersion := 9;
  try
    LFuture := LBroker.Connection.Send(KAFKA_API_KEY_OFFSET_FETCH, LVersion, R.ToBytes(LVersion),
      FHubRef);
    if not FHub.WaitFuture(LFuture, PcTickMs + FRequestTimeoutMs) then
    begin
      LBroker.Connection.Abandon(LFuture);
      raise EKafkaTimeout.CreateFmt('OffsetFetch to %s got no answer in %d ms',
        [LBroker.Connection.Address, FRequestTimeoutMs]);
    end;
    LBody := LFuture.Body;
  except
    on E: EKafkaConnection do
    begin
      if FHub.Stopped then
        raise;
      FCoordinator.MarkUnknown(E.Message);
      AReason := E.Message;
      Exit;
    end;
    on E: EKafkaTimeout do
    begin
      FCoordinator.MarkUnknown(E.Message);
      AReason := E.Message;
      Exit;
    end;
  end;
  LAnswer.FromBytes(LBody, LVersion);
  LFound := False;
  for G := 0 to High(LAnswer.Groups) do
    if LAnswer.Groups[G].GroupId = FParams.GroupId then
    begin
      LGroup := LAnswer.Groups[G];
      LFound := True;
      Break;
    end;
  if not LFound then
    raise EKafkaProtocol.CreateFmt('OffsetFetch v%d from %s has no answer for group ''%s''',
      [LVersion, LBroker.Connection.Address, FParams.GroupId]);
  LCode := LGroup.ErrorCode;
  if LCode <> 0 then
  begin
    LMessage := Format('OffsetFetch for group ''%s'' to %s: %s', [FParams.GroupId,
      LBroker.Connection.Address, KafkaErrorText(LCode)]);
    AReason := LMessage;
    case LCode of
      KAFKA_ERROR_NOT_COORDINATOR, KAFKA_ERROR_COORDINATOR_NOT_AVAILABLE:
        begin
          FCoordinator.MarkUnknown(LMessage);
          Exit;
        end;
      KAFKA_ERROR_COORDINATOR_LOAD_IN_PROGRESS:
        Exit;
      KAFKA_ERROR_STALE_MEMBER_EPOCH:
        if MemberEpoch > 0 then
          Exit;
      KAFKA_ERROR_UNKNOWN_MEMBER_ID, KAFKA_ERROR_GROUP_AUTHORIZATION_FAILED:
        ;
    else
      if KafkaErrorIsRetriable(LCode) then
        Exit;
    end;
    raise EKafkaBrokerError.Create(LCode, LMessage);
  end;
  for T := 0 to High(LGroup.Topics) do
  begin
    LName := LGroup.Topics[T].Name;
    if LVersion >= 10 then
    begin
      // By id: the name is the one the request gave that id.
      LName := '';
      for I := 0 to High(R.Groups[0].Topics) do
        if R.Groups[0].Topics[I].TopicId.Equals(LGroup.Topics[T].TopicId) then
          LName := R.Groups[0].Topics[I].Name;
    end;
    for P := 0 to High(LGroup.Topics[T].Partitions) do
    begin
      LCode := LGroup.Topics[T].Partitions[P].ErrorCode;
      if LCode <> 0 then
      begin
        LMessage := Format('OffsetFetch for group ''%s'', %s-%d: %s', [FParams.GroupId, LName,
          LGroup.Topics[T].Partitions[P].PartitionIndex, KafkaErrorText(LCode)]);
        AReason := LMessage;
        case LCode of
          KAFKA_ERROR_UNSTABLE_OFFSET_COMMIT, KAFKA_ERROR_UNKNOWN_TOPIC_OR_PARTITION,
          KAFKA_ERROR_UNKNOWN_TOPIC_ID:
            Exit;
        end;
        raise EKafkaBrokerError.Create(LCode, LMessage);
      end;
      for I := 0 to High(AResult) do
        if (AResult[I].Topic = LName) and
          (AResult[I].Partition = LGroup.Topics[T].Partitions[P].PartitionIndex) then
        begin
          AResult[I].Offset := LGroup.Topics[T].Partitions[P].CommittedOffset;
          AResult[I].LeaderEpoch := LGroup.Topics[T].Partitions[P].CommittedLeaderEpoch;
          if LGroup.Topics[T].Partitions[P].MetadataIsNull then
            AResult[I].Metadata := ''
          else
            AResult[I].Metadata := LGroup.Topics[T].Partitions[P].Metadata;
        end;
    end;
  end;
  Result := True;
end;

end.
