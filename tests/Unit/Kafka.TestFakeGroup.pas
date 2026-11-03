unit Kafka.TestFakeGroup;

{ The fake broker's group coordinator (Kafka.TestFakeBroker): consumer groups
  under KIP-848, small enough to read, strict enough to catch a client that
  gets the protocol wrong.

  Heartbeats. Epoch 0 joins (the subscription is required then), -1 leaves,
  anything else must be the member's current epoch or the one before it
  (FENCED_MEMBER_EPOCH otherwise; UNKNOWN_MEMBER_ID for a member the group
  doesn't have). A join, a leave, a removed member or a new subscription
  bumps the group epoch and recomputes every member's target with a simple
  assignor: each topic's partitions dealt round robin to the members that
  subscribe to it, in member id order.

  Revoke before assign. A member is given its target minus what another
  member still holds: what another acknowledged owning (the TopicPartitions
  of its heartbeats) or was given and hasn't acknowledged letting go. So a
  partition moves only after its previous owner's heartbeat says it let go,
  as a real coordinator does it. A member's epoch becomes the group's when
  what it is given changes. The assignment goes in the answer when it
  changed, on a join, and when the member's acknowledged partitions differ
  from what it was given (the client resent its state after a failure).

  Passive members (AddMember): the test's stand-in for another consumer.
  They never heartbeat, and take what they are given at once (given and
  acknowledged at the same moment), so a rebalance against them is
  deterministic.

  Commits. OffsetCommit checks the member and its epoch (UNKNOWN_MEMBER_ID,
  STALE_MEMBER_EPOCH for an older one, FENCED_MEMBER_EPOCH for a newer one;
  an empty member id with -1 commits as a simple consumer); topics by name
  or, from v10, by id through the cluster view. OffsetFetch (v8+, the
  groups form) checks a member id and epoch when given, and answers -1 for
  a partition with nothing committed.

  Test controls (fields, set before or between requests): the heartbeat
  interval to give; the next N heartbeats answered with an error code
  (the member left untouched); the next N heartbeats of a member in the
  group fenced (FENCED_MEMBER_EPOCH, the member removed); the next N
  OffsetCommit or OffsetFetch answered with an error code.

  Thread-safe: every method takes the broker's monitor (given at Create)
  and pulses it after a change, so a test can wait on a condition with a
  deadline (WaitMemberOwns, WaitMemberCount, WaitHeartbeats) instead of
  sleeping.

  Not a test unit (its name doesn't end in Tests): shared, not mirrored. }

interface

uses
  SysUtils,
  Classes,
  Generics.Collections,
  PascalCommon.ThreadPool,
  Kafka.Types,
  Kafka.Protocol.Primitives,
  Kafka.Messages.Metadata,
  Kafka.Messages.ConsumerGroupHeartbeat,
  Kafka.Messages.OffsetCommit,
  Kafka.Messages.OffsetFetch;

type
  TKafkaFakeMember = class
  public
    MemberId: string;
    Epoch: Int32;
    PreviousEpoch: Int32;
    Topics: TKafkaStringArray;
    /// Keys TopicName#0Partition, sorted.
    Owned: TStringList;
    Given: TStringList;
    Target: TStringList;
    Passive: Boolean;
    HeartbeatCount: Integer;
    JoinRequest: TKafkaConsumerGroupHeartbeatRequest;
    LastRequest: TKafkaConsumerGroupHeartbeatRequest;
    constructor Create(const AMemberId: string);
    destructor Destroy; override;
  end;

  TKafkaFakeCommit = record
    Offset: Int64;
    LeaderEpoch: Int32;
    Metadata: string;
  end;

  TKafkaFakeGroup = class
  public
    GroupId: string;
    GroupEpoch: Int32;
    Members: TObjectList<TKafkaFakeMember>;
    /// Keys TopicName#0Partition.
    Commits: TDictionary<string, TKafkaFakeCommit>;
    constructor Create(const AGroupId: string);
    destructor Destroy; override;
    function Find(const AMemberId: string): TKafkaFakeMember;
  end;

  TKafkaFakeGroups = class
  private
    FMonitor: TPcMonitor;
    FGroups: TObjectDictionary<string, TKafkaFakeGroup>;
    FHeartbeatCount: Integer;
    FJoinCount: Integer;
    FLeaveCount: Integer;
    FCommitCount: Integer;
    FOffsetFetchCount: Integer;
    function Group(const AGroupId: string): TKafkaFakeGroup;
    procedure Rebalance(AGroup: TKafkaFakeGroup; const AView: TKafkaMetadataResponse);
    function Available(AGroup: TKafkaFakeGroup; AMember: TKafkaFakeMember; const AKey: string): Boolean;
    function Give(AGroup: TKafkaFakeGroup; AMember: TKafkaFakeMember): Boolean;
    procedure SettlePassive(AGroup: TKafkaFakeGroup);
    procedure RemoveMemberOf(AGroup: TKafkaFakeGroup; AMember: TKafkaFakeMember;
      const AView: TKafkaMetadataResponse);
    function MemberOf(const AGroupId, AMemberId: string): TKafkaFakeMember;
  public
    /// What heartbeat answers give as the interval. Default 100.
    HeartbeatIntervalMs: Int32;
    HeartbeatErrors: Integer;
    HeartbeatErrorCode: Int16;
    FenceNext: Integer;
    CommitErrors: Integer;
    CommitErrorCode: Int16;
    OffsetFetchErrors: Integer;
    OffsetFetchErrorCode: Int16;

    /// AMonitor is the broker's (it outlives this).
    constructor Create(AMonitor: TPcMonitor);
    destructor Destroy; override;

    function Heartbeat(const AView: TKafkaMetadataResponse;
      const ARequest: TKafkaConsumerGroupHeartbeatRequest): TKafkaConsumerGroupHeartbeatResponse;
    function OffsetCommit(const AView: TKafkaMetadataResponse; AVersion: Int16;
      const ARequest: TKafkaOffsetCommitRequest): TKafkaOffsetCommitResponse;
    function OffsetFetch(const AView: TKafkaMetadataResponse; AVersion: Int16;
      const ARequest: TKafkaOffsetFetchRequest): TKafkaOffsetFetchResponse;

    /// A passive member subscribed to ATopics (the unit's header).
    procedure AddMember(const AGroupId, AMemberId: string; const ATopics: array of string;
      const AView: TKafkaMetadataResponse);
    /// The member is gone, as when its session expires: its next
    /// heartbeat gets UNKNOWN_MEMBER_ID.
    procedure RemoveMember(const AGroupId, AMemberId: string; const AView: TKafkaMetadataResponse);
    function MemberCount(const AGroupId: string): Integer;
    function MemberIds(const AGroupId: string): TKafkaStringArray;
    /// What the member acknowledged owning (empty when it isn't a member).
    function MemberOwned(const AGroupId, AMemberId: string): TKafkaTopicPartitionArray;
    /// What the member was last given.
    function MemberGiven(const AGroupId, AMemberId: string): TKafkaTopicPartitionArray;
    /// -1 when it isn't a member.
    function MemberEpoch(const AGroupId, AMemberId: string): Int32;
    function MemberHeartbeats(const AGroupId, AMemberId: string): Integer;
    /// The member's last join and last heartbeat (False when it isn't one).
    function MemberRequests(const AGroupId, AMemberId: string;
      out AJoin, ALast: TKafkaConsumerGroupHeartbeatRequest): Boolean;
    /// -1 when nothing is committed.
    function CommittedOffset(const AGroupId, ATopic: string; APartition: Int32): Int64;
    function CommittedMetadata(const AGroupId, ATopic: string; APartition: Int32): string;
    procedure SetCommitted(const AGroupId, ATopic: string; APartition: Int32; AOffset: Int64);
    function HeartbeatCount: Integer;
    function JoinCount: Integer;
    function LeaveCount: Integer;
    function CommitCount: Integer;
    function OffsetFetchCount: Integer;

    /// Waits up to ATimeoutMs until the member acknowledged owning exactly
    /// ACount partitions.
    function WaitMemberOwns(const AGroupId, AMemberId: string; ACount: Integer;
      ATimeoutMs: Cardinal): Boolean;
    /// ... until the group has ACount members.
    function WaitMemberCount(const AGroupId: string; ACount: Integer; ATimeoutMs: Cardinal): Boolean;
    /// ... until ACount heartbeats came (any group).
    function WaitHeartbeats(ACount: Integer; ATimeoutMs: Cardinal): Boolean;
    /// ... until the member sent ACount heartbeats.
    function WaitMemberHeartbeats(const AGroupId, AMemberId: string; ACount: Integer;
      ATimeoutMs: Cardinal): Boolean;
  end;

/// TopicName#0Partition.
function KafkaFakePartitionKey(const ATopic: string; APartition: Int32): string;

implementation

uses
  PascalCommon.Threading,
  Kafka.Errors;

function KafkaFakePartitionKey(const ATopic: string; APartition: Int32): string;
begin
  // Zero-padded, so that sorted keys put partition 2 before 10.
  Result := ATopic + #0 + Format('%.6d', [APartition]);
end;

function KeyTopic(const AKey: string): string;
begin
  Result := Copy(AKey, 1, Pos(#0, AKey) - 1);
end;

function KeyPartition(const AKey: string): Int32;
begin
  Result := StrToInt(Copy(AKey, Pos(#0, AKey) + 1, MaxInt));
end;

function NewKeyList: TStringList;
begin
  Result := TStringList.Create;
  Result.CaseSensitive := True;
  Result.Sorted := True;
  Result.Duplicates := dupIgnore;
end;

function SameKeys(A, B: TStringList): Boolean;
var
  I: Integer;
begin
  if A.Count <> B.Count then
    Exit(False);
  for I := 0 to A.Count - 1 do
    if A[I] <> B[I] then
      Exit(False);
  Result := True;
end;

function KeysToPartitions(AKeys: TStringList): TKafkaTopicPartitionArray;
var
  I: Integer;
begin
  Result := nil;
  SetLength(Result, AKeys.Count);
  for I := 0 to AKeys.Count - 1 do
    Result[I] := KafkaTopicPartition(KeyTopic(AKeys[I]), KeyPartition(AKeys[I]));
end;

function TopicNameById(const AView: TKafkaMetadataResponse; const AId: TKafkaUuid): string;
var
  I: Integer;
begin
  for I := 0 to High(AView.Topics) do
    if AView.Topics[I].TopicId.Equals(AId) then
      Exit(AView.Topics[I].Name);
  Result := '';
end;

function TopicIdByName(const AView: TKafkaMetadataResponse; const AName: string): TKafkaUuid;
var
  I: Integer;
begin
  for I := 0 to High(AView.Topics) do
    if AView.Topics[I].Name = AName then
      Exit(AView.Topics[I].TopicId);
  Result := TKafkaUuid.Zero;
end;

{ TKafkaFakeMember }

constructor TKafkaFakeMember.Create(const AMemberId: string);
begin
  inherited Create;
  MemberId := AMemberId;
  Owned := NewKeyList;
  Given := NewKeyList;
  Target := NewKeyList;
  JoinRequest.Init;
  LastRequest.Init;
end;

destructor TKafkaFakeMember.Destroy;
begin
  Owned.Free;
  Given.Free;
  Target.Free;
  inherited;
end;

{ TKafkaFakeGroup }

constructor TKafkaFakeGroup.Create(const AGroupId: string);
begin
  inherited Create;
  GroupId := AGroupId;
  Members := TObjectList<TKafkaFakeMember>.Create(True);
  Commits := TDictionary<string, TKafkaFakeCommit>.Create;
end;

destructor TKafkaFakeGroup.Destroy;
begin
  Commits.Free;
  Members.Free;
  inherited;
end;

function TKafkaFakeGroup.Find(const AMemberId: string): TKafkaFakeMember;
var
  I: Integer;
begin
  for I := 0 to Members.Count - 1 do
    if Members[I].MemberId = AMemberId then
      Exit(Members[I]);
  Result := nil;
end;

{ TKafkaFakeGroups }

constructor TKafkaFakeGroups.Create(AMonitor: TPcMonitor);
begin
  inherited Create;
  FMonitor := AMonitor;
  FGroups := TObjectDictionary<string, TKafkaFakeGroup>.Create([doOwnsValues]);
  HeartbeatIntervalMs := 100;
end;

destructor TKafkaFakeGroups.Destroy;
begin
  FGroups.Free;
  inherited;
end;

function TKafkaFakeGroups.Group(const AGroupId: string): TKafkaFakeGroup;
begin
  if not FGroups.TryGetValue(AGroupId, Result) then
  begin
    Result := TKafkaFakeGroup.Create(AGroupId);
    FGroups.Add(AGroupId, Result);
  end;
end;

function TKafkaFakeGroups.MemberOf(const AGroupId, AMemberId: string): TKafkaFakeMember;
var
  G: TKafkaFakeGroup;
begin
  if FGroups.TryGetValue(AGroupId, G) then
    Result := G.Find(AMemberId)
  else
    Result := nil;
end;

procedure TKafkaFakeGroups.Rebalance(AGroup: TKafkaFakeGroup; const AView: TKafkaMetadataResponse);
var
  LIds: TStringList;
  LEligible: TList<TKafkaFakeMember>;
  M: TKafkaFakeMember;
  I, T, K, P: Integer;
  LSubscribed: Boolean;
begin
  Inc(AGroup.GroupEpoch);
  LIds := NewKeyList;
  LEligible := TList<TKafkaFakeMember>.Create;
  try
    for I := 0 to AGroup.Members.Count - 1 do
    begin
      AGroup.Members[I].Target.Clear;
      LIds.Add(AGroup.Members[I].MemberId);
    end;
    for T := 0 to High(AView.Topics) do
    begin
      LEligible.Clear;
      for I := 0 to LIds.Count - 1 do
      begin
        M := AGroup.Find(LIds[I]);
        LSubscribed := False;
        for K := 0 to High(M.Topics) do
          if M.Topics[K] = AView.Topics[T].Name then
            LSubscribed := True;
        if LSubscribed then
          LEligible.Add(M);
      end;
      if LEligible.Count = 0 then
        Continue;
      // The view lists partitions in any order; deal them by index.
      for P := 0 to Length(AView.Topics[T].Partitions) - 1 do
        LEligible[P mod LEligible.Count].Target.Add(
          KafkaFakePartitionKey(AView.Topics[T].Name, P));
    end;
  finally
    LEligible.Free;
    LIds.Free;
  end;
  // Passive members let go of what moved at once.
  SettlePassive(AGroup);
end;

function TKafkaFakeGroups.Available(AGroup: TKafkaFakeGroup; AMember: TKafkaFakeMember;
  const AKey: string): Boolean;
var
  I: Integer;
  O: TKafkaFakeMember;
begin
  for I := 0 to AGroup.Members.Count - 1 do
  begin
    O := AGroup.Members[I];
    if O = AMember then
      Continue;
    if (O.Owned.IndexOf(AKey) >= 0) or (O.Given.IndexOf(AKey) >= 0) then
      Exit(False);
  end;
  Result := True;
end;

function TKafkaFakeGroups.Give(AGroup: TKafkaFakeGroup; AMember: TKafkaFakeMember): Boolean;
var
  LNew: TStringList;
  I: Integer;
begin
  LNew := NewKeyList;
  try
    for I := 0 to AMember.Target.Count - 1 do
      if Available(AGroup, AMember, AMember.Target[I]) then
        LNew.Add(AMember.Target[I]);
    Result := not SameKeys(LNew, AMember.Given);
    if Result then
    begin
      AMember.Given.Assign(LNew);
      if AGroup.GroupEpoch > AMember.Epoch then
      begin
        AMember.PreviousEpoch := AMember.Epoch;
        AMember.Epoch := AGroup.GroupEpoch;
      end;
    end;
  finally
    LNew.Free;
  end;
end;

procedure TKafkaFakeGroups.SettlePassive(AGroup: TKafkaFakeGroup);
var
  I, LRound: Integer;
  LChanged: Boolean;
  M: TKafkaFakeMember;
begin
  // A passive member lets go and takes at once; what it frees may let
  // another take, so a few rounds.
  for LRound := 1 to 4 do
  begin
    LChanged := False;
    for I := 0 to AGroup.Members.Count - 1 do
    begin
      M := AGroup.Members[I];
      if not M.Passive then
        Continue;
      if Give(AGroup, M) then
        LChanged := True;
      if not SameKeys(M.Owned, M.Given) then
      begin
        M.Owned.Assign(M.Given);
        LChanged := True;
      end;
    end;
    if not LChanged then
      Break;
  end;
end;

procedure TKafkaFakeGroups.RemoveMemberOf(AGroup: TKafkaFakeGroup; AMember: TKafkaFakeMember;
  const AView: TKafkaMetadataResponse);
begin
  AGroup.Members.Remove(AMember);
  Rebalance(AGroup, AView);
end;

function TKafkaFakeGroups.Heartbeat(const AView: TKafkaMetadataResponse;
  const ARequest: TKafkaConsumerGroupHeartbeatRequest): TKafkaConsumerGroupHeartbeatResponse;
var
  G: TKafkaFakeGroup;
  M: TKafkaFakeMember;
  LForce, LChanged, LNewSubscription: Boolean;
  LKeys: TStringList;
  I, K, T, N: Integer;
  LName: string;
  LId: TKafkaUuid;
begin
  FMonitor.Enter;
  try
    Inc(FHeartbeatCount);
    Result.Init;
    Result.HeartbeatIntervalMs := HeartbeatIntervalMs;
    Result.MemberId := ARequest.MemberId;
    Result.MemberIdIsNull := False;
    G := Group(ARequest.GroupId);
    M := G.Find(ARequest.MemberId);
    if HeartbeatErrors > 0 then
    begin
      Dec(HeartbeatErrors);
      Result.ErrorCode := HeartbeatErrorCode;
      Exit;
    end;
    LForce := False;
    if ARequest.MemberEpoch = -1 then
    begin
      Inc(FLeaveCount);
      if M = nil then
      begin
        Result.ErrorCode := KAFKA_ERROR_UNKNOWN_MEMBER_ID;
        Exit;
      end;
      RemoveMemberOf(G, M, AView);
      Result.MemberEpoch := -1;
      Exit;
    end;
    if ARequest.MemberEpoch = 0 then
    begin
      Inc(FJoinCount);
      if ARequest.SubscribedTopicNamesIsNull or (Length(ARequest.SubscribedTopicNames) = 0) then
      begin
        Result.ErrorCode := KAFKA_ERROR_INVALID_REQUEST;
        Result.ErrorMessage := 'SubscribedTopicNames must be set when joining';
        Result.ErrorMessageIsNull := False;
        Exit;
      end;
      if M = nil then
      begin
        M := TKafkaFakeMember.Create(ARequest.MemberId);
        G.Members.Add(M);
      end
      else
      begin
        M.Owned.Clear;
        M.Given.Clear;
      end;
      M.Topics := Copy(ARequest.SubscribedTopicNames, 0, Length(ARequest.SubscribedTopicNames));
      M.JoinRequest := ARequest;
      Rebalance(G, AView);
      M.PreviousEpoch := M.Epoch;
      M.Epoch := G.GroupEpoch;
      LForce := True;
    end
    else
    begin
      if M = nil then
      begin
        Result.ErrorCode := KAFKA_ERROR_UNKNOWN_MEMBER_ID;
        Exit;
      end;
      if (FenceNext > 0) or ((ARequest.MemberEpoch <> M.Epoch) and
        (ARequest.MemberEpoch <> M.PreviousEpoch)) then
      begin
        if FenceNext > 0 then
          Dec(FenceNext);
        RemoveMemberOf(G, M, AView);
        Result.ErrorCode := KAFKA_ERROR_FENCED_MEMBER_EPOCH;
        Exit;
      end;
      if not ARequest.SubscribedTopicNamesIsNull then
      begin
        LNewSubscription := Length(ARequest.SubscribedTopicNames) <> Length(M.Topics);
        if not LNewSubscription then
          for I := 0 to High(M.Topics) do
            if M.Topics[I] <> ARequest.SubscribedTopicNames[I] then
              LNewSubscription := True;
        if LNewSubscription then
        begin
          M.Topics := Copy(ARequest.SubscribedTopicNames, 0,
            Length(ARequest.SubscribedTopicNames));
          Rebalance(G, AView);
        end;
      end;
    end;
    M.LastRequest := ARequest;
    Inc(M.HeartbeatCount);
    if not ARequest.TopicPartitionsIsNull then
    begin
      M.Owned.Clear;
      for T := 0 to High(ARequest.TopicPartitions) do
      begin
        LName := TopicNameById(AView, ARequest.TopicPartitions[T].TopicId);
        for K := 0 to High(ARequest.TopicPartitions[T].Partitions) do
          M.Owned.Add(KafkaFakePartitionKey(LName, ARequest.TopicPartitions[T].Partitions[K]));
      end;
    end;
    LChanged := Give(G, M);
    SettlePassive(G);
    Result.MemberEpoch := M.Epoch;
    if LChanged or LForce or (not ARequest.TopicPartitionsIsNull and
      not SameKeys(M.Owned, M.Given)) then
    begin
      Result.AssignmentIsNull := False;
      Result.Assignment.Init;
      LKeys := M.Given;
      for I := 0 to LKeys.Count - 1 do
      begin
        LName := KeyTopic(LKeys[I]);
        LId := TopicIdByName(AView, LName);
        T := 0;
        while (T < Length(Result.Assignment.TopicPartitions)) and
          not Result.Assignment.TopicPartitions[T].TopicId.Equals(LId) do
          Inc(T);
        if T = Length(Result.Assignment.TopicPartitions) then
        begin
          SetLength(Result.Assignment.TopicPartitions, T + 1);
          Result.Assignment.TopicPartitions[T].Init;
          Result.Assignment.TopicPartitions[T].TopicId := LId;
        end;
        N := Length(Result.Assignment.TopicPartitions[T].Partitions);
        SetLength(Result.Assignment.TopicPartitions[T].Partitions, N + 1);
        Result.Assignment.TopicPartitions[T].Partitions[N] := KeyPartition(LKeys[I]);
      end;
    end;
  finally
    FMonitor.PulseAll;
    FMonitor.Leave;
  end;
end;

function TKafkaFakeGroups.OffsetCommit(const AView: TKafkaMetadataResponse; AVersion: Int16;
  const ARequest: TKafkaOffsetCommitRequest): TKafkaOffsetCommitResponse;
var
  G: TKafkaFakeGroup;
  M: TKafkaFakeMember;
  LError, LCode: Int16;
  T, P: Integer;
  LName: string;
  C: TKafkaFakeCommit;
  LPart: TKafkaOffsetCommitRequestPartition;
begin
  FMonitor.Enter;
  try
    Inc(FCommitCount);
    Result.Init;
    G := Group(ARequest.GroupId);
    LError := 0;
    if CommitErrors > 0 then
    begin
      Dec(CommitErrors);
      LError := CommitErrorCode;
    end
    else if (ARequest.MemberId <> '') or (ARequest.GenerationIdOrMemberEpoch <> -1) then
    begin
      M := G.Find(ARequest.MemberId);
      if M = nil then
        LError := KAFKA_ERROR_UNKNOWN_MEMBER_ID
      else if ARequest.GenerationIdOrMemberEpoch > M.Epoch then
        LError := KAFKA_ERROR_FENCED_MEMBER_EPOCH
      else if ARequest.GenerationIdOrMemberEpoch < M.Epoch then
        LError := KAFKA_ERROR_STALE_MEMBER_EPOCH;
    end;
    SetLength(Result.Topics, Length(ARequest.Topics));
    for T := 0 to High(ARequest.Topics) do
    begin
      Result.Topics[T].Init;
      Result.Topics[T].Name := ARequest.Topics[T].Name;
      Result.Topics[T].TopicId := ARequest.Topics[T].TopicId;
      if AVersion >= 10 then
        LName := TopicNameById(AView, ARequest.Topics[T].TopicId)
      else
        LName := ARequest.Topics[T].Name;
      SetLength(Result.Topics[T].Partitions, Length(ARequest.Topics[T].Partitions));
      for P := 0 to High(ARequest.Topics[T].Partitions) do
      begin
        LPart := ARequest.Topics[T].Partitions[P];
        LCode := LError;
        if (LCode = 0) and (LName = '') then
          LCode := KAFKA_ERROR_UNKNOWN_TOPIC_ID;
        Result.Topics[T].Partitions[P].Init;
        Result.Topics[T].Partitions[P].PartitionIndex := LPart.PartitionIndex;
        Result.Topics[T].Partitions[P].ErrorCode := LCode;
        if LCode = 0 then
        begin
          C.Offset := LPart.CommittedOffset;
          C.LeaderEpoch := LPart.CommittedLeaderEpoch;
          if LPart.CommittedMetadataIsNull then
            C.Metadata := ''
          else
            C.Metadata := LPart.CommittedMetadata;
          G.Commits.AddOrSetValue(KafkaFakePartitionKey(LName, LPart.PartitionIndex), C);
        end;
      end;
    end;
  finally
    FMonitor.PulseAll;
    FMonitor.Leave;
  end;
end;

function TKafkaFakeGroups.OffsetFetch(const AView: TKafkaMetadataResponse; AVersion: Int16;
  const ARequest: TKafkaOffsetFetchRequest): TKafkaOffsetFetchResponse;
var
  I, T, P: Integer;
  G: TKafkaFakeGroup;
  M: TKafkaFakeMember;
  LCode: Int16;
  LName: string;
  C: TKafkaFakeCommit;
  LReq: TKafkaOffsetFetchRequestGroup;
begin
  FMonitor.Enter;
  try
    Inc(FOffsetFetchCount);
    Result.Init;
    SetLength(Result.Groups, Length(ARequest.Groups));
    for I := 0 to High(ARequest.Groups) do
    begin
      LReq := ARequest.Groups[I];
      Result.Groups[I].Init;
      Result.Groups[I].GroupId := LReq.GroupId;
      G := Group(LReq.GroupId);
      LCode := 0;
      if OffsetFetchErrors > 0 then
      begin
        Dec(OffsetFetchErrors);
        LCode := OffsetFetchErrorCode;
      end
      else if not LReq.MemberIdIsNull and (LReq.MemberEpoch >= 0) then
      begin
        M := G.Find(LReq.MemberId);
        if M = nil then
          LCode := KAFKA_ERROR_UNKNOWN_MEMBER_ID
        else if LReq.MemberEpoch <> M.Epoch then
          LCode := KAFKA_ERROR_STALE_MEMBER_EPOCH;
      end;
      Result.Groups[I].ErrorCode := LCode;
      if (LCode <> 0) or LReq.TopicsIsNull then
        Continue; // every committed offset: not needed by these tests
      SetLength(Result.Groups[I].Topics, Length(LReq.Topics));
      for T := 0 to High(LReq.Topics) do
      begin
        Result.Groups[I].Topics[T].Init;
        Result.Groups[I].Topics[T].Name := LReq.Topics[T].Name;
        Result.Groups[I].Topics[T].TopicId := LReq.Topics[T].TopicId;
        if AVersion >= 10 then
          LName := TopicNameById(AView, LReq.Topics[T].TopicId)
        else
          LName := LReq.Topics[T].Name;
        SetLength(Result.Groups[I].Topics[T].Partitions, Length(LReq.Topics[T].PartitionIndexes));
        for P := 0 to High(LReq.Topics[T].PartitionIndexes) do
        begin
          Result.Groups[I].Topics[T].Partitions[P].Init;
          Result.Groups[I].Topics[T].Partitions[P].PartitionIndex := LReq.Topics[T].PartitionIndexes[P];
          Result.Groups[I].Topics[T].Partitions[P].Metadata := '';
          Result.Groups[I].Topics[T].Partitions[P].MetadataIsNull := False;
          if LName = '' then
            Result.Groups[I].Topics[T].Partitions[P].ErrorCode := KAFKA_ERROR_UNKNOWN_TOPIC_ID
          else if G.Commits.TryGetValue(KafkaFakePartitionKey(LName,
            LReq.Topics[T].PartitionIndexes[P]), C) then
          begin
            Result.Groups[I].Topics[T].Partitions[P].CommittedOffset := C.Offset;
            Result.Groups[I].Topics[T].Partitions[P].CommittedLeaderEpoch := C.LeaderEpoch;
            Result.Groups[I].Topics[T].Partitions[P].Metadata := C.Metadata;
          end
          else
            Result.Groups[I].Topics[T].Partitions[P].CommittedOffset := -1;
        end;
      end;
    end;
  finally
    FMonitor.Leave;
  end;
end;

procedure TKafkaFakeGroups.AddMember(const AGroupId, AMemberId: string;
  const ATopics: array of string; const AView: TKafkaMetadataResponse);
var
  G: TKafkaFakeGroup;
  M: TKafkaFakeMember;
  I: Integer;
begin
  FMonitor.Enter;
  try
    G := Group(AGroupId);
    M := TKafkaFakeMember.Create(AMemberId);
    M.Passive := True;
    SetLength(M.Topics, Length(ATopics));
    for I := 0 to High(ATopics) do
      M.Topics[I] := ATopics[I];
    G.Members.Add(M);
    M.Epoch := G.GroupEpoch + 1;
    Rebalance(G, AView);
    FMonitor.PulseAll;
  finally
    FMonitor.Leave;
  end;
end;

procedure TKafkaFakeGroups.RemoveMember(const AGroupId, AMemberId: string;
  const AView: TKafkaMetadataResponse);
var
  G: TKafkaFakeGroup;
  M: TKafkaFakeMember;
begin
  FMonitor.Enter;
  try
    G := Group(AGroupId);
    M := G.Find(AMemberId);
    if M <> nil then
      RemoveMemberOf(G, M, AView);
    FMonitor.PulseAll;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeGroups.MemberCount(const AGroupId: string): Integer;
var
  G: TKafkaFakeGroup;
begin
  FMonitor.Enter;
  try
    if FGroups.TryGetValue(AGroupId, G) then
      Result := G.Members.Count
    else
      Result := 0;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeGroups.MemberIds(const AGroupId: string): TKafkaStringArray;
var
  G: TKafkaFakeGroup;
  I: Integer;
begin
  Result := nil;
  FMonitor.Enter;
  try
    if FGroups.TryGetValue(AGroupId, G) then
    begin
      SetLength(Result, G.Members.Count);
      for I := 0 to G.Members.Count - 1 do
        Result[I] := G.Members[I].MemberId;
    end;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeGroups.MemberOwned(const AGroupId, AMemberId: string): TKafkaTopicPartitionArray;
var
  M: TKafkaFakeMember;
begin
  Result := nil;
  FMonitor.Enter;
  try
    M := MemberOf(AGroupId, AMemberId);
    if M <> nil then
      Result := KeysToPartitions(M.Owned);
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeGroups.MemberGiven(const AGroupId, AMemberId: string): TKafkaTopicPartitionArray;
var
  M: TKafkaFakeMember;
begin
  Result := nil;
  FMonitor.Enter;
  try
    M := MemberOf(AGroupId, AMemberId);
    if M <> nil then
      Result := KeysToPartitions(M.Given);
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeGroups.MemberEpoch(const AGroupId, AMemberId: string): Int32;
var
  M: TKafkaFakeMember;
begin
  FMonitor.Enter;
  try
    M := MemberOf(AGroupId, AMemberId);
    if M <> nil then
      Result := M.Epoch
    else
      Result := -1;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeGroups.MemberHeartbeats(const AGroupId, AMemberId: string): Integer;
var
  M: TKafkaFakeMember;
begin
  FMonitor.Enter;
  try
    M := MemberOf(AGroupId, AMemberId);
    if M <> nil then
      Result := M.HeartbeatCount
    else
      Result := 0;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeGroups.MemberRequests(const AGroupId, AMemberId: string;
  out AJoin, ALast: TKafkaConsumerGroupHeartbeatRequest): Boolean;
var
  M: TKafkaFakeMember;
begin
  FMonitor.Enter;
  try
    M := MemberOf(AGroupId, AMemberId);
    Result := M <> nil;
    if Result then
    begin
      AJoin := M.JoinRequest;
      ALast := M.LastRequest;
    end
    else
    begin
      AJoin.Init;
      ALast.Init;
    end;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeGroups.CommittedOffset(const AGroupId, ATopic: string; APartition: Int32): Int64;
var
  G: TKafkaFakeGroup;
  C: TKafkaFakeCommit;
begin
  Result := -1;
  FMonitor.Enter;
  try
    if FGroups.TryGetValue(AGroupId, G) and
      G.Commits.TryGetValue(KafkaFakePartitionKey(ATopic, APartition), C) then
      Result := C.Offset;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeGroups.CommittedMetadata(const AGroupId, ATopic: string;
  APartition: Int32): string;
var
  G: TKafkaFakeGroup;
  C: TKafkaFakeCommit;
begin
  Result := '';
  FMonitor.Enter;
  try
    if FGroups.TryGetValue(AGroupId, G) and
      G.Commits.TryGetValue(KafkaFakePartitionKey(ATopic, APartition), C) then
      Result := C.Metadata;
  finally
    FMonitor.Leave;
  end;
end;

procedure TKafkaFakeGroups.SetCommitted(const AGroupId, ATopic: string; APartition: Int32;
  AOffset: Int64);
var
  C: TKafkaFakeCommit;
begin
  FMonitor.Enter;
  try
    C.Offset := AOffset;
    C.LeaderEpoch := -1;
    C.Metadata := '';
    Group(AGroupId).Commits.AddOrSetValue(KafkaFakePartitionKey(ATopic, APartition), C);
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeGroups.HeartbeatCount: Integer;
begin
  FMonitor.Enter;
  Result := FHeartbeatCount;
  FMonitor.Leave;
end;

function TKafkaFakeGroups.JoinCount: Integer;
begin
  FMonitor.Enter;
  Result := FJoinCount;
  FMonitor.Leave;
end;

function TKafkaFakeGroups.LeaveCount: Integer;
begin
  FMonitor.Enter;
  Result := FLeaveCount;
  FMonitor.Leave;
end;

function TKafkaFakeGroups.CommitCount: Integer;
begin
  FMonitor.Enter;
  Result := FCommitCount;
  FMonitor.Leave;
end;

function TKafkaFakeGroups.OffsetFetchCount: Integer;
begin
  FMonitor.Enter;
  Result := FOffsetFetchCount;
  FMonitor.Leave;
end;

function TKafkaFakeGroups.WaitMemberOwns(const AGroupId, AMemberId: string; ACount: Integer;
  ATimeoutMs: Cardinal): Boolean;
var
  LDeadline, LNow: UInt64;
  M: TKafkaFakeMember;
begin
  LDeadline := PcTickMs + ATimeoutMs;
  FMonitor.Enter;
  try
    while True do
    begin
      M := MemberOf(AGroupId, AMemberId);
      if (M <> nil) and (M.Owned.Count = ACount) then
        Exit(True);
      LNow := PcTickMs;
      if LNow >= LDeadline then
        Exit(False);
      FMonitor.Wait(Cardinal(LDeadline - LNow));
    end;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeGroups.WaitMemberCount(const AGroupId: string; ACount: Integer;
  ATimeoutMs: Cardinal): Boolean;
var
  LDeadline, LNow: UInt64;
  G: TKafkaFakeGroup;
  N: Integer;
begin
  LDeadline := PcTickMs + ATimeoutMs;
  FMonitor.Enter;
  try
    while True do
    begin
      N := 0;
      if FGroups.TryGetValue(AGroupId, G) then
        N := G.Members.Count;
      if N = ACount then
        Exit(True);
      LNow := PcTickMs;
      if LNow >= LDeadline then
        Exit(False);
      FMonitor.Wait(Cardinal(LDeadline - LNow));
    end;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeGroups.WaitHeartbeats(ACount: Integer; ATimeoutMs: Cardinal): Boolean;
var
  LDeadline, LNow: UInt64;
begin
  LDeadline := PcTickMs + ATimeoutMs;
  FMonitor.Enter;
  try
    while FHeartbeatCount < ACount do
    begin
      LNow := PcTickMs;
      if LNow >= LDeadline then
        Exit(False);
      FMonitor.Wait(Cardinal(LDeadline - LNow));
    end;
    Result := True;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeGroups.WaitMemberHeartbeats(const AGroupId, AMemberId: string; ACount: Integer;
  ATimeoutMs: Cardinal): Boolean;
var
  LDeadline, LNow: UInt64;
  M: TKafkaFakeMember;
begin
  LDeadline := PcTickMs + ATimeoutMs;
  FMonitor.Enter;
  try
    while True do
    begin
      M := MemberOf(AGroupId, AMemberId);
      if (M <> nil) and (M.HeartbeatCount >= ACount) then
        Exit(True);
      LNow := PcTickMs;
      if LNow >= LDeadline then
        Exit(False);
      FMonitor.Wait(Cardinal(LDeadline - LNow));
    end;
  finally
    FMonitor.Leave;
  end;
end;

end.
