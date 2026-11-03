unit Kafka.Group;

{$I kafka.inc}

{ What a consumer group looks like to the consumer, whatever the protocol:
  the membership interface (docs/design.md, decision 4), its events, the
  committed offsets, and the wait hub the group units share.

  The seam. IKafkaGroupMembership is what the consumer talks to. KIP-848's
  implementation is TKafkaConsumerProtocol (Kafka.Group.Consumer); the
  classic protocol (JoinGroup/SyncGroup with an assignor on the client) can
  come later as a second one, without changing the consumer or its users.
  Hence the shape: incremental events (partitions revoked, assigned, lost,
  never "the whole new list"), commits that carry whatever the protocol
  needs (a member epoch here, a generation in the classic protocol), and
  nothing in the interface that only one protocol has.

  Events reach the consumer inside its Poll, on its own thread (section 4,
  "Threads"): the membership queues them, the consumer's Poll hands them to
  a handler, and only once the handler returned does the membership go on
  (acknowledging an assignment, rejoining after losing one). A handler that
  raises ends that Poll with its exception; the event still counts as
  handled.

  The wait hub. A heartbeat thread and the consumer's thread both wait on
  requests to the coordinator, on a backoff, and on each other. TKafkaWaitHub
  is one monitor (pascal-common-faa's TPcMonitor) that is also an
  IKafkaSignal: a request sent with the hub as its signal pulses it when
  its answer comes, so a waiter wakes for an answer, a state change or Stop,
  whichever comes first. It is reference-counted because a request's future
  holds its signal: an abandoned request may outlive the membership that
  sent it, and its late Signal must find a live object. Stop is for good:
  every wait on the hub ends at once from then on (Close).

  Delphi x FPC: TKafkaGroupEventHandler is an "of object" method, not a
  closure (FPC 3.2.2 has no anonymous methods). }

interface

uses
  SysUtils,
  PascalCommon.ThreadPool,
  Kafka.Types,
  Kafka.Connection;

const
  /// Java's retry.backoff.max.ms: the longest wait between two attempts when
  /// they keep failing (each failure doubles the wait from RetryBackoffMs).
  KAFKA_DEFAULT_RETRY_BACKOFF_MAX_MS = 1000;
  /// Java's max.poll.interval.ms, which KIP-848 also sends as the member's
  /// rebalance timeout.
  KAFKA_DEFAULT_MAX_POLL_INTERVAL_MS = 300000;

type
  /// Revoked: the partitions go away; commit what was processed first.
  /// Assigned: new partitions (the list may be empty: an assignment with
  /// nothing new still passes through the consumer before it is
  /// acknowledged). Lost: the member is no longer in the group (fenced,
  /// expired) and the partitions may already belong to someone else: don't
  /// commit them.
  TKafkaGroupEventKind = (kgeRevoked, kgeAssigned, kgeLost);

  TKafkaGroupEventHandler = procedure(AKind: TKafkaGroupEventKind;
    const APartitions: TKafkaTopicPartitionArray) of object;

  { An offset to commit, or a committed one (OffsetCommit, OffsetFetch). }
  TKafkaCommittedOffset = record
    Topic: string;
    Partition: Int32;
    /// The next record to read. -1 in Committed's answer: none committed.
    Offset: Int64;
    /// The leader epoch of the record before Offset; -1 when unknown.
    LeaderEpoch: Int32;
    Metadata: string;
  end;
  TKafkaCommittedOffsetArray = array of TKafkaCommittedOffset;

  /// Java's MemberState (KIP-848), for diagnostics and tests.
  TKafkaMemberState = (kmsUnsubscribed, kmsJoining, kmsReconciling, kmsAcknowledging,
    kmsStable, kmsFenced, kmsPrepareLeaving, kmsLeaving, kmsFatal);

  { A consumer's membership in its group (the unit's header). The consumer
    calls every method from its own thread; the membership's own threads
    never call the consumer. }
  IKafkaGroupMembership = interface
    ['{5B1C3E7A-90D2-4F6B-8A41-2E7C9D0B6F35}']
    function GroupId: string;
    /// The member id, fixed for the membership's life ('' if the protocol
    /// gets it from the coordinator and hasn't yet).
    function MemberId: string;
    /// The current member epoch: 0 while joining, -1 after leaving.
    function MemberEpoch: Int32;
    function State: TKafkaMemberState;
    /// The topics to consume (names, at least one). The first call joins
    /// the group; a later one changes the subscription.
    procedure Subscribe(const ATopics: array of string);
    /// Hands the queued events to AHandler, in order, on the caller's
    /// thread (nil: they are taken without a call). Raises, once, an error
    /// that ended the membership (a GROUP_AUTHORIZATION_FAILED, a broker
    /// without the protocol). Returns the events handled.
    function Poll(AHandler: TKafkaGroupEventHandler): Integer;
    /// The partitions the handled events gave the consumer.
    function Assignment: TKafkaTopicPartitionArray;
    /// Commits AOffsets for the member, retrying what a retry can cure
    /// until ATimeoutMs runs out (EKafkaTimeout); EKafkaBrokerError for
    /// the rest.
    procedure Commit(const AOffsets: TKafkaCommittedOffsetArray; ATimeoutMs: Cardinal);
    /// The group's committed offsets for APartitions, in their order.
    function Committed(const APartitions: array of TKafkaTopicPartition;
      ATimeoutMs: Cardinal): TKafkaCommittedOffsetArray;
    /// Leaves the group: the partitions still held are revoked through
    /// AHandler (lost, if the member was no longer in the group), then
    /// the coordinator is told, waiting up to ATimeoutMs for its answer.
    /// The membership can't join again; Close follows.
    procedure Leave(AHandler: TKafkaGroupEventHandler; ATimeoutMs: Cardinal);
    /// Stops every thread without telling the coordinator (it removes the
    /// member when the session times out). Idempotent.
    procedure Close;
  end;

  { The wait hub (the unit's header). Enter/Leave/Wait/PulseAll are the
    monitor's; Signal is Enter + PulseAll + Leave. }
  TKafkaWaitHub = class(TInterfacedObject, IKafkaSignal)
  private
    FMonitor: TPcMonitor;
    FStopped: Boolean;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Signal;
    procedure Enter;
    procedure Leave;
    /// Holding the lock: releases it, waits for a pulse or the timeout,
    /// takes it again (it may wake early: re-check in a loop).
    procedure Wait(ATimeoutMs: Cardinal);
    /// Holding the lock.
    procedure PulseAll;
    /// Ends every wait, for good.
    procedure Stop;
    function Stopped: Boolean;
    /// Stopped, for a caller holding the lock.
    property StopRequested: Boolean read FStopped;
    /// Not holding the lock: waits until AFuture is done (True) or ADeadline
    /// (PcTickMs) passes (False). EKafkaConnection once stopped. AFuture
    /// must have been sent with this hub as its signal.
    function WaitFuture(const AFuture: IKafkaResponseFuture; ADeadline: UInt64): Boolean;
    /// Not holding the lock: waits until ADeadline or a pulse; False once
    /// stopped. A backoff that a state change may cut short.
    function Pause(ADeadline: UInt64): Boolean;
  end;

/// The wait after AFailures failures in a row: ABaseMs doubled per failure
/// up to AMaxMs, then +-20% at random (Java's ExponentialBackoff, factor 2,
/// jitter 0.2). 0 failures: ABaseMs, with the jitter.
function KafkaBackoffMs(AFailures: Integer; ABaseMs, AMaxMs: Cardinal): Cardinal;

implementation

uses
  PascalCommon.Threading,
  PascalCommon.Random;

function KafkaBackoffMs(AFailures: Integer; ABaseMs, AMaxMs: Cardinal): Cardinal;
var
  LWait: UInt64;
  I: Integer;
  LRandom: TBytes;
  LFactor: Double;
begin
  if AMaxMs < ABaseMs then
    AMaxMs := ABaseMs;
  LWait := ABaseMs;
  I := 0;
  while (I < AFailures) and (LWait < AMaxMs) do
  begin
    LWait := LWait * 2;
    Inc(I);
  end;
  if LWait > AMaxMs then
    LWait := AMaxMs;
  // 0.8 to 1.2, from one random byte.
  LRandom := PcRandomBytes(1);
  LFactor := 0.8 + 0.4 * LRandom[0] / 255;
  Result := Cardinal(Round(LWait * LFactor));
end;

{ TKafkaWaitHub }

constructor TKafkaWaitHub.Create;
begin
  inherited Create;
  FMonitor := TPcMonitor.Create;
end;

destructor TKafkaWaitHub.Destroy;
begin
  FMonitor.Free;
  inherited;
end;

procedure TKafkaWaitHub.Signal;
begin
  FMonitor.Enter;
  try
    FMonitor.PulseAll;
  finally
    FMonitor.Leave;
  end;
end;

procedure TKafkaWaitHub.Enter;
begin
  FMonitor.Enter;
end;

procedure TKafkaWaitHub.Leave;
begin
  FMonitor.Leave;
end;

procedure TKafkaWaitHub.Wait(ATimeoutMs: Cardinal);
begin
  FMonitor.Wait(ATimeoutMs);
end;

procedure TKafkaWaitHub.PulseAll;
begin
  FMonitor.PulseAll;
end;

procedure TKafkaWaitHub.Stop;
begin
  FMonitor.Enter;
  try
    FStopped := True;
    FMonitor.PulseAll;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaWaitHub.Stopped: Boolean;
begin
  FMonitor.Enter;
  try
    Result := FStopped;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaWaitHub.WaitFuture(const AFuture: IKafkaResponseFuture; ADeadline: UInt64): Boolean;
var
  LNow: UInt64;
begin
  FMonitor.Enter;
  try
    while True do
    begin
      // The future signals after it is done, and Signal needs this lock: a
      // check made holding it can't miss the pulse.
      if AFuture.IsDone then
        Exit(True);
      if FStopped then
        raise EKafkaConnection.Create('The group membership is closed');
      LNow := PcTickMs;
      if LNow >= ADeadline then
        Exit(False);
      FMonitor.Wait(Cardinal(ADeadline - LNow));
    end;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaWaitHub.Pause(ADeadline: UInt64): Boolean;
var
  LNow: UInt64;
begin
  FMonitor.Enter;
  try
    if not FStopped then
    begin
      LNow := PcTickMs;
      if LNow < ADeadline then
        FMonitor.Wait(Cardinal(ADeadline - LNow));
    end;
    Result := not FStopped;
  finally
    FMonitor.Leave;
  end;
end;

end.
