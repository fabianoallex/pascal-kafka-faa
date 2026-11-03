unit Kafka.Group.Coordinator;

{$I kafka.inc}

{ Finding a consumer group's coordinator and keeping a connection to it, as
  Java's CoordinatorRequestManager does.

  FindCoordinator goes to any broker (TKafkaCluster.AnyBroker). From v4 the
  request carries a list of keys and the answer a coordinator per key
  (KIP-699); v0 to v3, which a broker older than 3.0 speaks, carry one key
  and answer at the top level. The coordinator's connection is a separate
  one (TKafkaCluster.Coordinator: a heartbeat must not wait behind a Fetch
  on the broker's side).

  Rediscovery. The coordinator is forgotten (MarkUnknown) when a request
  to it says NOT_COORDINATOR or COORDINATOR_NOT_AVAILABLE, or its connection
  fails; the next request looks it up again. A broker that just started
  answers FindCoordinator with COORDINATOR_NOT_AVAILABLE, and heartbeats
  with NOT_COORDINATOR, for several seconds while its coordinator loads (the
  captures recorded ten in a row, docs/design.md "K2 progress"), so lookups
  are spaced: at least RetryBackoffMs after the previous one, and after
  failures in a row RetryBackoffMs doubled per failure up to
  RetryBackoffMaxMs, with Java's +-20% jitter (KafkaBackoffMs). One lookup
  at a time: a second thread that needs the coordinator meanwhile waits
  for the first's answer.

  Errors. A retriable one (KafkaErrorIsRetriable: COORDINATOR_NOT_AVAILABLE,
  COORDINATOR_LOAD_IN_PROGRESS, NOT_COORDINATOR; a broker that can't be
  reached; a request that timed out) is retried; anything else raises
  EKafkaBrokerError at once (GROUP_AUTHORIZATION_FAILED: Java's
  GroupAuthorizationException), and so does EKafkaAuthentication.

  Waiting happens on a TKafkaWaitHub (Kafka.Group), the membership's when it
  gives one, so that closing the membership ends a lookup in progress.
  Thread-safe. }

interface

uses
  SysUtils,
  SyncObjs,
  Kafka.Types,
  Kafka.Connection,
  Kafka.Cluster,
  Kafka.Group;

type
  TKafkaGroupCoordinator = class
  private
    FCluster: TKafkaCluster;
    FGroupId: string;
    FRetryBackoffMs: Cardinal;
    FRetryBackoffMaxMs: Cardinal;
    FRequestTimeoutMs: Cardinal;
    FHub: TKafkaWaitHub;
    FHubRef: IKafkaSignal;
    FOwnHub: Boolean;
    FLock: TCriticalSection;
    // Under FLock.
    FKnown: Boolean;
    FNodeId: Int32;
    FHost: string;
    FPort: Int32;
    FFailures: Integer;
    FNextLookupAt: UInt64;
    FLookingUp: Boolean;
    FLookupCount: Integer;
    FLastVersion: Int16;
    FLastError: string;
    procedure Lookup(out ANodeId: Int32; out AHost: string; out APort: Int32);
    procedure LookupFailed(const AReason: string);
    function WaitLeft: Cardinal;
  public
    /// AHub nil: one of its own. The cluster must outlive the coordinator.
    constructor Create(ACluster: TKafkaCluster; const AGroupId: string;
      ARetryBackoffMs, ARetryBackoffMaxMs: Cardinal; AHub: TKafkaWaitHub = nil);
    destructor Destroy; override;
    /// One step, no waiting for a backoff: True with the coordinator's
    /// connection (looking it up first when unknown); False when a lookup
    /// failed or must wait, with AWaitMs until the next may go. Raises
    /// what a retry won't cure, and EKafkaConnection once closed.
    function TryConnection(out AConnection: IKafkaBrokerConnection; out AWaitMs: Cardinal): Boolean;
    /// TryConnection until it succeeds or ATimeoutMs passes (EKafkaTimeout,
    /// with the last failure).
    function Connection(ATimeoutMs: Cardinal): IKafkaBrokerConnection;
    /// The known coordinator's connection, with no lookup (False when it
    /// isn't known or can't be reached). What leaving the group uses.
    function KnownConnection(out AConnection: IKafkaBrokerConnection): Boolean;
    /// Forgets the coordinator; the next TryConnection looks it up again.
    procedure MarkUnknown(const AReason: string);
    /// Ends the waits on its own hub (not on one it was given).
    procedure Close;
    function IsKnown: Boolean;
    /// The coordinator's node id; -1 when unknown.
    function NodeId: Int32;
    /// FindCoordinator requests sent (for tests and diagnostics).
    function LookupCount: Integer;
    /// The FindCoordinator version of the last lookup.
    function LastVersion: Int16;
    property GroupId: string read FGroupId;
  end;

implementation

uses
  PascalCommon.Threading,
  Kafka.Errors,
  Kafka.Messages.Apis,
  Kafka.Messages.FindCoordinator;

const
  /// FindCoordinator's key type for a consumer group (1 is a transaction).
  KEY_TYPE_GROUP = 0;

constructor TKafkaGroupCoordinator.Create(ACluster: TKafkaCluster; const AGroupId: string;
  ARetryBackoffMs, ARetryBackoffMaxMs: Cardinal; AHub: TKafkaWaitHub);
begin
  inherited Create;
  if AGroupId = '' then
    raise EArgumentException.Create('A group id is required');
  FCluster := ACluster;
  FGroupId := AGroupId;
  FRetryBackoffMs := ARetryBackoffMs;
  if FRetryBackoffMs = 0 then
    FRetryBackoffMs := KAFKA_DEFAULT_RETRY_BACKOFF_MS;
  FRetryBackoffMaxMs := ARetryBackoffMaxMs;
  if FRetryBackoffMaxMs = 0 then
    FRetryBackoffMaxMs := KAFKA_DEFAULT_RETRY_BACKOFF_MAX_MS;
  if FRetryBackoffMaxMs < FRetryBackoffMs then
    FRetryBackoffMaxMs := FRetryBackoffMs;
  FRequestTimeoutMs := ACluster.Params.Connection.RequestTimeoutMs;
  if FRequestTimeoutMs = 0 then
    FRequestTimeoutMs := KAFKA_DEFAULT_REQUEST_TIMEOUT_MS;
  FOwnHub := AHub = nil;
  if AHub = nil then
    AHub := TKafkaWaitHub.Create;
  FHub := AHub;
  FHubRef := AHub;
  FLock := TCriticalSection.Create;
  FNodeId := -1;
  FLastVersion := -1;
end;

destructor TKafkaGroupCoordinator.Destroy;
begin
  FLock.Free;
  FHubRef := nil;
  inherited;
end;

procedure TKafkaGroupCoordinator.Close;
begin
  // Its own hub only: a hub it was given is its owner's to stop.
  if FOwnHub then
    FHub.Stop;
end;

function TKafkaGroupCoordinator.IsKnown: Boolean;
begin
  FLock.Enter;
  try
    Result := FKnown;
  finally
    FLock.Leave;
  end;
end;

function TKafkaGroupCoordinator.NodeId: Int32;
begin
  FLock.Enter;
  try
    if FKnown then
      Result := FNodeId
    else
      Result := -1;
  finally
    FLock.Leave;
  end;
end;

function TKafkaGroupCoordinator.LookupCount: Integer;
begin
  FLock.Enter;
  try
    Result := FLookupCount;
  finally
    FLock.Leave;
  end;
end;

function TKafkaGroupCoordinator.LastVersion: Int16;
begin
  FLock.Enter;
  try
    Result := FLastVersion;
  finally
    FLock.Leave;
  end;
end;

procedure TKafkaGroupCoordinator.MarkUnknown(const AReason: string);
begin
  FLock.Enter;
  try
    if FKnown then
      FLastError := AReason;
    FKnown := False;
  finally
    FLock.Leave;
  end;
end;

procedure TKafkaGroupCoordinator.LookupFailed(const AReason: string);
begin
  FLock.Enter;
  try
    FLookingUp := False;
    Inc(FFailures);
    FLastError := AReason;
    FNextLookupAt := PcTickMs + KafkaBackoffMs(FFailures, FRetryBackoffMs, FRetryBackoffMaxMs);
  finally
    FLock.Leave;
  end;
  // A thread waiting for this lookup tries its own.
  FHub.Signal;
end;

function TKafkaGroupCoordinator.WaitLeft: Cardinal;
var
  LNow: UInt64;
begin
  FLock.Enter;
  try
    LNow := PcTickMs;
    if FNextLookupAt > LNow then
      Result := Cardinal(FNextLookupAt - LNow)
    else
      Result := 0;
  finally
    FLock.Leave;
  end;
end;

procedure TKafkaGroupCoordinator.Lookup(out ANodeId: Int32; out AHost: string; out APort: Int32);
var
  LBroker: IKafkaBrokerConnection;
  LVersion: Int16;
  LRequest: TKafkaFindCoordinatorRequest;
  LResponse: TKafkaFindCoordinatorResponse;
  LFuture: IKafkaResponseFuture;
  LError: Int16;
  LMessage: string;
  LFound: Boolean;
  I: Integer;
begin
  LBroker := FCluster.AnyBroker;
  LVersion := LBroker.Connection.NegotiatedVersion(KAFKA_API_KEY_FIND_COORDINATOR);
  FLock.Enter;
  FLastVersion := LVersion;
  FLock.Leave;
  LRequest.Init;
  LRequest.KeyType := KEY_TYPE_GROUP;
  if LVersion >= 4 then
  begin
    SetLength(LRequest.CoordinatorKeys, 1);
    LRequest.CoordinatorKeys[0] := FGroupId;
  end
  else
    LRequest.Key := FGroupId;
  LFuture := LBroker.Connection.Send(KAFKA_API_KEY_FIND_COORDINATOR, LVersion,
    LRequest.ToBytes(LVersion), FHubRef);
  if not FHub.WaitFuture(LFuture, PcTickMs + FRequestTimeoutMs) then
  begin
    LBroker.Connection.Abandon(LFuture);
    raise EKafkaTimeout.CreateFmt('FindCoordinator to %s got no answer in %d ms',
      [LBroker.Connection.Address, FRequestTimeoutMs]);
  end;
  LResponse.FromBytes(LFuture.Body, LVersion);
  LMessage := '';
  if LVersion >= 4 then
  begin
    LFound := False;
    for I := 0 to High(LResponse.Coordinators) do
      if LResponse.Coordinators[I].Key = FGroupId then
      begin
        LFound := True;
        LError := LResponse.Coordinators[I].ErrorCode;
        if not LResponse.Coordinators[I].ErrorMessageIsNull then
          LMessage := LResponse.Coordinators[I].ErrorMessage;
        ANodeId := LResponse.Coordinators[I].NodeId;
        AHost := LResponse.Coordinators[I].Host;
        APort := LResponse.Coordinators[I].Port;
        Break;
      end;
    if not LFound then
      raise EKafkaProtocol.CreateFmt('FindCoordinator v%d from %s has no coordinator for group ''%s''',
        [LVersion, LBroker.Connection.Address, FGroupId]);
  end
  else
  begin
    LError := LResponse.ErrorCode;
    if not LResponse.ErrorMessageIsNull then
      LMessage := LResponse.ErrorMessage;
    ANodeId := LResponse.NodeId;
    AHost := LResponse.Host;
    APort := LResponse.Port;
  end;
  if LError <> 0 then
  begin
    if LMessage <> '' then
      LMessage := ' (' + LMessage + ')';
    raise EKafkaBrokerError.Create(LError, Format('FindCoordinator for group ''%s'' to %s: %s%s',
      [FGroupId, LBroker.Connection.Address, KafkaErrorText(LError), LMessage]));
  end;
end;

function TKafkaGroupCoordinator.TryConnection(out AConnection: IKafkaBrokerConnection;
  out AWaitMs: Cardinal): Boolean;
var
  LKnown: Boolean;
  LNodeId: Int32;
  LHost: string;
  LPort: Int32;
  LNow: UInt64;
begin
  AConnection := nil;
  AWaitMs := 0;
  if FHub.Stopped then
    raise EKafkaConnection.Create('The group membership is closed');
  FLock.Enter;
  try
    LKnown := FKnown;
    LNodeId := FNodeId;
    LHost := FHost;
    LPort := FPort;
    if not LKnown then
    begin
      LNow := PcTickMs;
      if FLookingUp then
      begin
        // Another thread's lookup: its end pulses the hub.
        AWaitMs := FRetryBackoffMs;
        Exit(False);
      end;
      if LNow < FNextLookupAt then
      begin
        AWaitMs := Cardinal(FNextLookupAt - LNow);
        Exit(False);
      end;
      FLookingUp := True;
      Inc(FLookupCount);
    end;
  finally
    FLock.Leave;
  end;
  if not LKnown then
  begin
    try
      Lookup(LNodeId, LHost, LPort);
    except
      on E: EKafkaBrokerError do
      begin
        if not KafkaErrorIsRetriable(E.ErrorCode) then
        begin
          LookupFailed(E.Message);
          raise;
        end;
        LookupFailed(E.Message);
        AWaitMs := WaitLeft;
        Exit(False);
      end;
      on E: EKafkaConnection do
      begin
        LookupFailed(E.Message);
        if FHub.Stopped then
          raise;
        AWaitMs := WaitLeft;
        Exit(False);
      end;
      on E: EKafkaTimeout do
      begin
        LookupFailed(E.Message);
        AWaitMs := WaitLeft;
        Exit(False);
      end;
      on E: Exception do
      begin
        LookupFailed(E.Message);
        raise;
      end;
    end;
    FLock.Enter;
    try
      FLookingUp := False;
      FKnown := True;
      FNodeId := LNodeId;
      FHost := LHost;
      FPort := LPort;
      FFailures := 0;
      // Even a lookup that worked isn't repeated sooner than this: a broker
      // still loading answers FindCoordinator with itself and heartbeats
      // with NOT_COORDINATOR.
      FNextLookupAt := PcTickMs + KafkaBackoffMs(0, FRetryBackoffMs, FRetryBackoffMaxMs);
    finally
      FLock.Leave;
    end;
    FHub.Signal;
  end;
  try
    AConnection := FCluster.Coordinator(LNodeId, LHost, LPort);
  except
    on E: EKafkaConnection do
    begin
      MarkUnknown(E.Message);
      if FHub.Stopped then
        raise;
      FLock.Enter;
      try
        Inc(FFailures);
        FLastError := E.Message;
        FNextLookupAt := PcTickMs + KafkaBackoffMs(FFailures, FRetryBackoffMs, FRetryBackoffMaxMs);
      finally
        FLock.Leave;
      end;
      AWaitMs := WaitLeft;
      Exit(False);
    end;
  end;
  Result := True;
end;

function TKafkaGroupCoordinator.Connection(ATimeoutMs: Cardinal): IKafkaBrokerConnection;
var
  LDeadline, LNow, LUntil: UInt64;
  LWait: Cardinal;
  LError: string;
begin
  LDeadline := PcTickMs + ATimeoutMs;
  while True do
  begin
    if TryConnection(Result, LWait) then
      Exit;
    LNow := PcTickMs;
    if LNow >= LDeadline then
    begin
      FLock.Enter;
      LError := FLastError;
      FLock.Leave;
      if LError <> '' then
        LError := ': ' + LError;
      raise EKafkaTimeout.CreateFmt('The coordinator of group ''%s'' was not found in %d ms%s',
        [FGroupId, ATimeoutMs, LError]);
    end;
    LUntil := LNow + LWait;
    if LUntil > LDeadline then
      LUntil := LDeadline;
    if not FHub.Pause(LUntil) then
      raise EKafkaConnection.Create('The group membership is closed');
  end;
end;

function TKafkaGroupCoordinator.KnownConnection(out AConnection: IKafkaBrokerConnection): Boolean;
var
  LNodeId: Int32;
  LHost: string;
  LPort: Int32;
begin
  AConnection := nil;
  FLock.Enter;
  try
    if not FKnown then
      Exit(False);
    LNodeId := FNodeId;
    LHost := FHost;
    LPort := FPort;
  finally
    FLock.Leave;
  end;
  try
    AConnection := FCluster.Coordinator(LNodeId, LHost, LPort);
    Result := True;
  except
    on EKafkaConnection do
      Result := False;
  end;
end;

end.
