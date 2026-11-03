unit Kafka.Cluster;

{$I kafka.inc}

{ The cluster as a client sees it: the bootstrap, the metadata (brokers,
  topics, partitions and their leaders), and one connection per broker,
  opened when first needed.

  A Kafka client is "smart" (docs/design.md, section 4): it learns the
  brokers from Metadata and sends each request to the broker that leads the
  partition. This unit is that knowledge, shared by the producer and the
  consumer; it sends no Produce or Fetch itself.

  Bootstrap. Open tries the addresses of BootstrapServers in order and asks
  the first that answers for Metadata; any failure (refused, TLS, not Kafka)
  moves on to the next, and the exception when none answers names each
  (EKafkaAuthentication when one of them refused the credentials, which a
  retry won't cure; EKafkaConnection otherwise). That connection is
  temporary: it is closed once the answer is in, and the brokers are
  reached later by the addresses they advertise (which may differ from the
  bootstrap's). When no
  known broker answers a later refresh, the bootstrap addresses are tried
  again.

  Metadata. A snapshot (IKafkaClusterMetadata), immutable, swapped whole on
  each refresh: a reader holds the interface and never sees a half-updated
  view. Only the tracked topics are asked for, as Java's client does: a topic
  becomes tracked the first time Topic, Leader or Refresh names it. The
  snapshot is refreshed when it is older than MetadataMaxAgeMs or after
  Invalidate (a caller saw NOT_LEADER_OR_FOLLOWER or the like:
  KafkaErrorIsInvalidMetadata, in Kafka.Errors); one refresh runs at a time.

  Waiting. Topic and Leader wait, up to MaxBlockMs (Java's max.block.ms), for
  a topic to exist with partitions and for a partition to have a leader,
  refreshing every RetryBackoffMs. The wait is on an event that Close sets,
  so closing the cluster ends it at once. An error that waiting won't cure
  (TOPIC_AUTHORIZATION_FAILED, INVALID_TOPIC_EXCEPTION) raises
  EKafkaBrokerError at once; time running out raises EKafkaTimeout.

  Connections. Broker(id) returns an IKafkaBrokerConnection: the connection
  lives as long as someone holds the interface, so a caller in the middle of
  a request is never left with a freed object when another thread replaces a
  broken connection, or when the cluster closes (Close closes every
  connection, which makes requests in flight fail with EKafkaConnection; the
  objects go when the last holder lets go). A broken connection is replaced
  on the next Broker call, and so is one whose SASL session is near its
  end (ReauthenticationDue, KIP-368: Kafka.Connection's header).
  Connections are opened one at a time (a lock), so
  two threads asking for the same broker open it once.

  Coordinators. A consumer group's coordinator gets a connection of its own
  (Coordinator), apart from the one Broker gives for the same node: a broker
  answers one connection's requests in order, so a heartbeat sent behind a
  Fetch that long-polls for half a second would wait that long. Java's client
  does the same (the coordinator is node Integer.MAX_VALUE - id, a separate
  connection). It is replaced as Broker's are, and also when FindCoordinator
  gives the node another address. No reconnect backoff
  yet: the producer's and consumer's retry backoff spaces the attempts.

  The connection parameters are a template (TKafkaClusterParams.Connection):
  Host and Port are each broker's; leave TlsServerName empty, so that each
  connection checks the certificate against its own broker's host. }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  Kafka.Types,
  Kafka.Protocol.Primitives,
  Kafka.Messages.Metadata,
  Kafka.Connection;

const
  KAFKA_DEFAULT_METADATA_MAX_AGE_MS = 300000;
  KAFKA_DEFAULT_MAX_BLOCK_MS = 60000;
  KAFKA_DEFAULT_RETRY_BACKOFF_MS = 100;
  /// The node id of a bootstrap connection, which isn't a known broker yet.
  KAFKA_BOOTSTRAP_NODE_ID = -1;

type
  TKafkaClusterParams = record
    /// 'host:port,host:port'; '[::1]:9092' for IPv6; the port defaults to
    /// 9092.
    BootstrapServers: string;
    /// Every connection's parameters but Host and Port (TLS, client id,
    /// timeouts).
    Connection: TKafkaConnectionParams;
    /// Metadata asks the broker to create a tracked topic that doesn't exist
    /// (when the broker allows it). Java's default: True.
    AllowAutoTopicCreation: Boolean;
    /// 0 = KAFKA_DEFAULT_METADATA_MAX_AGE_MS (Java's metadata.max.age.ms).
    MetadataMaxAgeMs: Cardinal;
    /// 0 = KAFKA_DEFAULT_MAX_BLOCK_MS (Java's max.block.ms).
    MaxBlockMs: Cardinal;
    /// 0 = KAFKA_DEFAULT_RETRY_BACKOFF_MS (Java's retry.backoff.ms).
    RetryBackoffMs: Cardinal;
  end;

  TKafkaBrokerInfo = record
    NodeId: Int32;
    Host: string;
    Port: Int32;
    /// '' when the broker has none.
    Rack: string;
  end;
  TKafkaBrokerInfoArray = array of TKafkaBrokerInfo;

  TKafkaPartitionInfo = record
    Partition: Int32;
    ErrorCode: Int16;
    /// -1 while the partition has no leader.
    LeaderId: Int32;
    LeaderEpoch: Int32;
    Replicas: TKafkaInt32Array;
    Isr: TKafkaInt32Array;
  end;
  TKafkaPartitionInfoArray = array of TKafkaPartitionInfo;

  TKafkaTopicInfo = record
    Name: string;
    TopicId: TKafkaUuid;
    ErrorCode: Int16;
    IsInternal: Boolean;
    /// In partition order (Partitions[I].Partition = I).
    Partitions: TKafkaPartitionInfoArray;
    /// No error and at least one partition.
    function IsReady: Boolean;
  end;

  /// What the cluster looked like at one refresh. Immutable.
  IKafkaClusterMetadata = interface
    ['{6E2B1B4E-7C1D-4A8E-9F27-5C3E0B8D21A1}']
    function ClusterId: string;
    function ControllerId: Int32;
    function Brokers: TKafkaBrokerInfoArray;
    function FindBroker(ANodeId: Int32; out ABroker: TKafkaBrokerInfo): Boolean;
    /// False for a topic the refresh didn't ask for (not tracked yet).
    function FindTopic(const AName: string; out ATopic: TKafkaTopicInfo): Boolean;
    function TopicCount: Integer;
    /// Milliseconds since the refresh (PcTickMs).
    function AgeMs: UInt64;
  end;

  /// A connection to one broker, alive while the interface is held.
  IKafkaBrokerConnection = interface
    ['{0F5C9A63-2D8B-4E71-B3A6-91E4C7D2F058}']
    function NodeId: Int32;
    function Connection: TKafkaConnection;
  end;

  TKafkaCluster = class
  private type
    TBootstrapAddress = record
      Host: string;
      Port: Word;
    end;
    TBootstrapAddressArray = array of TBootstrapAddress;
  private
    FParams: TKafkaClusterParams;
    FBootstrap: TBootstrapAddressArray;
    FLock: TCriticalSection;
    FRefreshLock: TCriticalSection;
    FConnectLock: TCriticalSection;
    FClosedEvent: TEvent;
    FMetadata: IKafkaClusterMetadata;
    FTracked: TStringList;
    FBrokers: TDictionary<Int32, IKafkaBrokerConnection>;
    FCoordinators: TDictionary<Int32, IKafkaBrokerConnection>;
    FInvalid: Boolean;
    FOpened: Boolean;
    FClosed: Boolean;
    FRefreshCount: Integer;
    procedure CheckUsable;
    function Track(const ATopic: string): Boolean;
    function TrackedTopics: TKafkaStringArray;
    function OpenConnection(ANodeId: Int32; const AHost: string; APort: Int32): IKafkaBrokerConnection;
    function TryMetadata(const ABroker: IKafkaBrokerConnection; const ARequest: TKafkaMetadataRequest;
      var AResponse: TKafkaMetadataResponse; var AErrors: string): Boolean;
    function DoRefresh: IKafkaClusterMetadata;
    function WaitBackoff(ADeadline: UInt64): Boolean;
    function MaxBlockMs: Cardinal;
  public
    /// Parses BootstrapServers (EArgumentException when it holds no address).
    constructor Create(const AParams: TKafkaClusterParams);
    /// Close, then frees.
    destructor Destroy; override;
    /// The bootstrap: the first address that answers Metadata. Raises
    /// EKafkaConnection with every address's failure when none does. Once
    /// only.
    procedure Open;
    /// Closes every broker connection and ends every wait. Idempotent; the
    /// cluster can't be used again.
    procedure Close;

    /// The current snapshot (nil before Open).
    function Metadata: IKafkaClusterMetadata;
    /// Tracks ATopics and refreshes now, whatever the snapshot's age.
    function Refresh(const ATopics: array of string): IKafkaClusterMetadata;
    /// The next Topic or Leader refreshes first.
    procedure Invalidate;
    /// Invalidated, or the snapshot is older than MetadataMaxAgeMs (or there
    /// is none): what Topic and Leader check before using it. The
    /// producer's sender refreshes on it, in the background.
    function NeedsRefresh: Boolean;
    /// The topic once it exists with partitions (tracking it, refreshing
    /// when stale or invalidated), waiting up to MaxBlockMs. EKafkaTimeout,
    /// or EKafkaBrokerError for an error that waiting won't cure.
    function Topic(const AName: string): TKafkaTopicInfo;
    /// The connection to the partition's leader, waiting up to MaxBlockMs
    /// for the partition to have one. EArgumentOutOfRangeException for a
    /// partition the topic doesn't have.
    function Leader(const ATopic: string; APartition: Int32): IKafkaBrokerConnection;
    /// The connection to broker ANodeId, opened if there is none or it
    /// broke. EKafkaConnection when the broker isn't in the metadata or
    /// can't be reached.
    function Broker(ANodeId: Int32): IKafkaBrokerConnection;
    /// A connection to some broker, for a request any broker answers
    /// (FindCoordinator): one already open if there is one, otherwise the
    /// metadata's brokers in order. EKafkaConnection when none can be
    /// reached; EKafkaAuthentication at once when one refused the
    /// credentials.
    function AnyBroker: IKafkaBrokerConnection;
    /// The group coordinator's connection: node ANodeId at AHost:APort, as
    /// FindCoordinator gave it (the unit's header, "Coordinators").
    function Coordinator(ANodeId: Int32; const AHost: string; APort: Int32): IKafkaBrokerConnection;

    /// Metadata requests sent since Create (for tests and diagnostics).
    function RefreshCount: Integer;
    property Params: TKafkaClusterParams read FParams;
  end;

/// BootstrapServers given, every other field at its default (plain TCP).
function KafkaClusterParams(const ABootstrapServers: string): TKafkaClusterParams;

implementation

uses
  PascalCommon.Threading,
  Kafka.Errors;

type
  TClusterMetadata = class(TInterfacedObject, IKafkaClusterMetadata)
  private
    FClusterId: string;
    FControllerId: Int32;
    FBrokers: TKafkaBrokerInfoArray;
    FTopics: array of TKafkaTopicInfo;
    FCreatedAt: UInt64;
  public
    constructor Create(const AResponse: TKafkaMetadataResponse);
    function ClusterId: string;
    function ControllerId: Int32;
    function Brokers: TKafkaBrokerInfoArray;
    function FindBroker(ANodeId: Int32; out ABroker: TKafkaBrokerInfo): Boolean;
    function FindTopic(const AName: string; out ATopic: TKafkaTopicInfo): Boolean;
    function TopicCount: Integer;
    function AgeMs: UInt64;
  end;

  TBrokerConnection = class(TInterfacedObject, IKafkaBrokerConnection)
  private
    FNodeId: Int32;
    FConnection: TKafkaConnection;
  public
    constructor Create(ANodeId: Int32; AConnection: TKafkaConnection);
    destructor Destroy; override;
    function NodeId: Int32;
    function Connection: TKafkaConnection;
  end;

function KafkaClusterParams(const ABootstrapServers: string): TKafkaClusterParams;
begin
  Result := Default(TKafkaClusterParams);
  Result.BootstrapServers := ABootstrapServers;
  Result.Connection := KafkaConnectionParams('', 9092);
  Result.AllowAutoTopicCreation := True;
end;

{ TKafkaTopicInfo }

function TKafkaTopicInfo.IsReady: Boolean;
begin
  Result := (ErrorCode = 0) and (Length(Partitions) > 0);
end;

{ TClusterMetadata }

constructor TClusterMetadata.Create(const AResponse: TKafkaMetadataResponse);
var
  I, K, P, N: Integer;
  T: TKafkaTopicInfo;
begin
  inherited Create;
  FCreatedAt := PcTickMs;
  if not AResponse.ClusterIdIsNull then
    FClusterId := AResponse.ClusterId;
  FControllerId := AResponse.ControllerId;
  SetLength(FBrokers, Length(AResponse.Brokers));
  for I := 0 to High(AResponse.Brokers) do
  begin
    FBrokers[I].NodeId := AResponse.Brokers[I].NodeId;
    FBrokers[I].Host := AResponse.Brokers[I].Host;
    FBrokers[I].Port := AResponse.Brokers[I].Port;
    if not AResponse.Brokers[I].RackIsNull then
      FBrokers[I].Rack := AResponse.Brokers[I].Rack;
  end;
  SetLength(FTopics, Length(AResponse.Topics));
  N := 0;
  for I := 0 to High(AResponse.Topics) do
  begin
    if AResponse.Topics[I].NameIsNull then
      Continue; // a topic asked for by id only; this library asks by name
    T := Default(TKafkaTopicInfo);
    T.Name := AResponse.Topics[I].Name;
    T.TopicId := AResponse.Topics[I].TopicId;
    T.ErrorCode := AResponse.Topics[I].ErrorCode;
    T.IsInternal := AResponse.Topics[I].IsInternal;
    // The broker lists partitions in any order: place each at its index, so
    // that Partitions[I] is partition I. A gap stays as "no leader".
    P := 0;
    for K := 0 to High(AResponse.Topics[I].Partitions) do
      if AResponse.Topics[I].Partitions[K].PartitionIndex + 1 > P then
        P := AResponse.Topics[I].Partitions[K].PartitionIndex + 1;
    SetLength(T.Partitions, P);
    for K := 0 to P - 1 do
    begin
      T.Partitions[K].Partition := K;
      T.Partitions[K].LeaderId := -1;
      T.Partitions[K].LeaderEpoch := -1;
      T.Partitions[K].ErrorCode := KAFKA_ERROR_LEADER_NOT_AVAILABLE;
    end;
    for K := 0 to High(AResponse.Topics[I].Partitions) do
    begin
      P := AResponse.Topics[I].Partitions[K].PartitionIndex;
      if P < 0 then
        Continue;
      T.Partitions[P].ErrorCode := AResponse.Topics[I].Partitions[K].ErrorCode;
      T.Partitions[P].LeaderId := AResponse.Topics[I].Partitions[K].LeaderId;
      T.Partitions[P].LeaderEpoch := AResponse.Topics[I].Partitions[K].LeaderEpoch;
      T.Partitions[P].Replicas := AResponse.Topics[I].Partitions[K].ReplicaNodes;
      T.Partitions[P].Isr := AResponse.Topics[I].Partitions[K].IsrNodes;
    end;
    FTopics[N] := T;
    Inc(N);
  end;
  SetLength(FTopics, N);
end;

function TClusterMetadata.ClusterId: string;
begin
  Result := FClusterId;
end;

function TClusterMetadata.ControllerId: Int32;
begin
  Result := FControllerId;
end;

function TClusterMetadata.Brokers: TKafkaBrokerInfoArray;
begin
  Result := Copy(FBrokers, 0, Length(FBrokers));
end;

function TClusterMetadata.FindBroker(ANodeId: Int32; out ABroker: TKafkaBrokerInfo): Boolean;
var
  I: Integer;
begin
  for I := 0 to High(FBrokers) do
    if FBrokers[I].NodeId = ANodeId then
    begin
      ABroker := FBrokers[I];
      Exit(True);
    end;
  ABroker := Default(TKafkaBrokerInfo);
  Result := False;
end;

function TClusterMetadata.FindTopic(const AName: string; out ATopic: TKafkaTopicInfo): Boolean;
var
  I: Integer;
begin
  for I := 0 to High(FTopics) do
    if FTopics[I].Name = AName then
    begin
      ATopic := FTopics[I];
      Exit(True);
    end;
  ATopic := Default(TKafkaTopicInfo);
  Result := False;
end;

function TClusterMetadata.TopicCount: Integer;
begin
  Result := Length(FTopics);
end;

function TClusterMetadata.AgeMs: UInt64;
var
  LNow: UInt64;
begin
  LNow := PcTickMs;
  if LNow > FCreatedAt then
    Result := LNow - FCreatedAt
  else
    Result := 0;
end;

{ TBrokerConnection }

constructor TBrokerConnection.Create(ANodeId: Int32; AConnection: TKafkaConnection);
begin
  inherited Create;
  FNodeId := ANodeId;
  FConnection := AConnection;
end;

destructor TBrokerConnection.Destroy;
begin
  FConnection.Free; // closes it
  inherited Destroy;
end;

function TBrokerConnection.NodeId: Int32;
begin
  Result := FNodeId;
end;

function TBrokerConnection.Connection: TKafkaConnection;
begin
  Result := FConnection;
end;

{ TKafkaCluster }

function ParseBootstrap(const AText: string): TKafkaCluster.TBootstrapAddressArray;
var
  LParts: TStringList;
  I, N, P, LPort: Integer;
  S, LHost: string;
begin
  Result := nil;
  LParts := TStringList.Create;
  try
    LParts.StrictDelimiter := True;
    LParts.Delimiter := ',';
    LParts.DelimitedText := AText;
    N := 0;
    SetLength(Result, LParts.Count);
    for I := 0 to LParts.Count - 1 do
    begin
      S := Trim(LParts[I]);
      if S = '' then
        Continue;
      LPort := 9092;
      if (S[1] = '[') and (Pos(']', S) > 0) then
      begin
        // [IPv6]:port
        P := Pos(']', S);
        LHost := Copy(S, 2, P - 2);
        if (P < Length(S)) and (S[P + 1] = ':') then
          LPort := StrToIntDef(Copy(S, P + 2, MaxInt), -1);
      end
      else
      begin
        P := LastDelimiter(':', S);
        if P > 0 then
        begin
          LHost := Copy(S, 1, P - 1);
          LPort := StrToIntDef(Copy(S, P + 1, MaxInt), -1);
        end
        else
          LHost := S;
      end;
      if (LHost = '') or (LPort < 1) or (LPort > 65535) then
        raise EArgumentException.CreateFmt('Bootstrap server "%s" is not host:port', [S]);
      Result[N].Host := LHost;
      Result[N].Port := Word(LPort);
      Inc(N);
    end;
    SetLength(Result, N);
  finally
    LParts.Free;
  end;
  if N = 0 then
    raise EArgumentException.Create('BootstrapServers holds no address');
end;

constructor TKafkaCluster.Create(const AParams: TKafkaClusterParams);
begin
  inherited Create;
  FParams := AParams;
  FBootstrap := ParseBootstrap(AParams.BootstrapServers);
  FLock := TCriticalSection.Create;
  FRefreshLock := TCriticalSection.Create;
  FConnectLock := TCriticalSection.Create;
  FClosedEvent := TEvent.Create(nil, True, False, '');
  FTracked := TStringList.Create;
  FTracked.CaseSensitive := True; // topic names are
  FTracked.Sorted := True;
  FTracked.Duplicates := dupIgnore;
  FBrokers := TDictionary<Int32, IKafkaBrokerConnection>.Create;
  FCoordinators := TDictionary<Int32, IKafkaBrokerConnection>.Create;
end;

destructor TKafkaCluster.Destroy;
begin
  if FLock <> nil then
    Close;
  FBrokers.Free;
  FCoordinators.Free;
  FTracked.Free;
  FClosedEvent.Free;
  FConnectLock.Free;
  FRefreshLock.Free;
  FLock.Free;
  inherited Destroy;
end;

function TKafkaCluster.MaxBlockMs: Cardinal;
begin
  Result := FParams.MaxBlockMs;
  if Result = 0 then
    Result := KAFKA_DEFAULT_MAX_BLOCK_MS;
end;

procedure TKafkaCluster.CheckUsable;
begin
  FLock.Enter;
  try
    if FClosed then
      raise EKafkaConnection.Create('The cluster is closed');
    if not FOpened then
      raise EKafkaConnection.Create('The cluster is not open (call Open)');
  finally
    FLock.Leave;
  end;
end;

procedure TKafkaCluster.Open;
begin
  FLock.Enter;
  try
    if FClosed then
      raise EKafkaConnection.Create('The cluster is closed');
    if FOpened then
      raise EKafkaConnection.Create('The cluster is already open');
    FOpened := True;
  finally
    FLock.Leave;
  end;
  DoRefresh;
end;

procedure TKafkaCluster.Close;
var
  LAll: array of IKafkaBrokerConnection;
  LBroker: IKafkaBrokerConnection;
  I: Integer;
begin
  LAll := nil;
  FLock.Enter;
  try
    if FClosed then
      Exit;
    FClosed := True;
    FClosedEvent.SetEvent;
    SetLength(LAll, FBrokers.Count + FCoordinators.Count);
    I := 0;
    for LBroker in FBrokers.Values do
    begin
      LAll[I] := LBroker;
      Inc(I);
    end;
    for LBroker in FCoordinators.Values do
    begin
      LAll[I] := LBroker;
      Inc(I);
    end;
    FBrokers.Clear;
    FCoordinators.Clear;
    FMetadata := nil;
  finally
    FLock.Leave;
  end;
  // Outside the lock: Close joins each reader thread.
  for I := 0 to High(LAll) do
    LAll[I].Connection.Close;
end;

function TKafkaCluster.Metadata: IKafkaClusterMetadata;
begin
  FLock.Enter;
  try
    Result := FMetadata;
  finally
    FLock.Leave;
  end;
end;

function TKafkaCluster.RefreshCount: Integer;
begin
  FLock.Enter;
  try
    Result := FRefreshCount;
  finally
    FLock.Leave;
  end;
end;

procedure TKafkaCluster.Invalidate;
begin
  FLock.Enter;
  try
    FInvalid := True;
  finally
    FLock.Leave;
  end;
end;

function TKafkaCluster.Track(const ATopic: string): Boolean;
begin
  FLock.Enter;
  try
    Result := FTracked.IndexOf(ATopic) < 0;
    if Result then
      FTracked.Add(ATopic);
  finally
    FLock.Leave;
  end;
end;

function TKafkaCluster.TrackedTopics: TKafkaStringArray;
var
  I: Integer;
begin
  FLock.Enter;
  try
    Result := nil;
    SetLength(Result, FTracked.Count);
    for I := 0 to FTracked.Count - 1 do
      Result[I] := FTracked[I];
  finally
    FLock.Leave;
  end;
end;

function TKafkaCluster.NeedsRefresh: Boolean;
var
  LMaxAge: Cardinal;
begin
  LMaxAge := FParams.MetadataMaxAgeMs;
  if LMaxAge = 0 then
    LMaxAge := KAFKA_DEFAULT_METADATA_MAX_AGE_MS;
  FLock.Enter;
  try
    Result := FInvalid or (FMetadata = nil) or (FMetadata.AgeMs >= LMaxAge);
  finally
    FLock.Leave;
  end;
end;

function TKafkaCluster.OpenConnection(ANodeId: Int32; const AHost: string;
  APort: Int32): IKafkaBrokerConnection;
var
  P: TKafkaConnectionParams;
  C: TKafkaConnection;
begin
  if (APort < 1) or (APort > 65535) then
    raise EKafkaConnection.CreateFmt('Broker %d advertises port %d', [ANodeId, APort]);
  P := FParams.Connection;
  P.Host := AHost;
  P.Port := Word(APort);
  C := TKafkaConnection.Create(P);
  try
    C.Open;
  except
    C.Free;
    raise;
  end;
  Result := TBrokerConnection.Create(ANodeId, C);
end;

/// Open, not broken, and its SASL session not near its end (KIP-368,
/// Kafka.Connection's header): a connection Broker hands out as it is.
function Usable(const ABroker: IKafkaBrokerConnection): Boolean;
begin
  Result := ABroker.Connection.IsOpen and not ABroker.Connection.ReauthenticationDue;
end;

function TKafkaCluster.Broker(ANodeId: Int32): IKafkaBrokerConnection;
var
  LInfo: TKafkaBrokerInfo;
  LKnown: Boolean;
begin
  FLock.Enter;
  try
    if FClosed then
      raise EKafkaConnection.Create('The cluster is closed');
    if FBrokers.TryGetValue(ANodeId, Result) and Usable(Result) then
      Exit;
    LKnown := (FMetadata <> nil) and FMetadata.FindBroker(ANodeId, LInfo);
  finally
    FLock.Leave;
  end;
  if not LKnown then
    raise EKafkaConnection.CreateFmt('Broker %d is not in the cluster''s metadata', [ANodeId]);
  FConnectLock.Enter;
  try
    // Another thread may have opened it while this one waited for the lock.
    FLock.Enter;
    try
      if FClosed then
        raise EKafkaConnection.Create('The cluster is closed');
      if FBrokers.TryGetValue(ANodeId, Result) and Usable(Result) then
        Exit;
    finally
      FLock.Leave;
    end;
    Result := OpenConnection(ANodeId, LInfo.Host, LInfo.Port);
    FLock.Enter;
    try
      if FClosed then
      begin
        Result.Connection.Close;
        raise EKafkaConnection.Create('The cluster is closed');
      end;
      // A broken one it replaces goes when its last holder lets go.
      FBrokers.AddOrSetValue(ANodeId, Result);
    finally
      FLock.Leave;
    end;
  finally
    FConnectLock.Leave;
  end;
end;

function TKafkaCluster.AnyBroker: IKafkaBrokerConnection;
var
  LBroker: IKafkaBrokerConnection;
  LMetadata: IKafkaClusterMetadata;
  LInfos: TKafkaBrokerInfoArray;
  LErrors: string;
  I: Integer;
begin
  CheckUsable;
  FLock.Enter;
  try
    for LBroker in FBrokers.Values do
      if Usable(LBroker) then
        Exit(LBroker);
    LMetadata := FMetadata;
  finally
    FLock.Leave;
  end;
  if LMetadata = nil then
    raise EKafkaConnection.Create('The cluster has no metadata yet');
  LInfos := LMetadata.Brokers;
  LErrors := '';
  for I := 0 to High(LInfos) do
    try
      Exit(Broker(LInfos[I].NodeId));
    except
      on E: EKafkaConnection do
        LErrors := LErrors + Format('; %s:%d: %s', [LInfos[I].Host, LInfos[I].Port, E.Message]);
    end;
  raise EKafkaConnection.Create('No broker could be reached' + LErrors);
end;

function TKafkaCluster.Coordinator(ANodeId: Int32; const AHost: string;
  APort: Int32): IKafkaBrokerConnection;

  function Fits(const AConn: IKafkaBrokerConnection): Boolean;
  begin
    Result := Usable(AConn) and (AConn.Connection.Params.Host = AHost) and
      (AConn.Connection.Params.Port = APort);
  end;

begin
  FLock.Enter;
  try
    if FClosed then
      raise EKafkaConnection.Create('The cluster is closed');
    if FCoordinators.TryGetValue(ANodeId, Result) and Fits(Result) then
      Exit;
  finally
    FLock.Leave;
  end;
  FConnectLock.Enter;
  try
    FLock.Enter;
    try
      if FClosed then
        raise EKafkaConnection.Create('The cluster is closed');
      if FCoordinators.TryGetValue(ANodeId, Result) and Fits(Result) then
        Exit;
    finally
      FLock.Leave;
    end;
    Result := OpenConnection(ANodeId, AHost, APort);
    FLock.Enter;
    try
      if FClosed then
      begin
        Result.Connection.Close;
        raise EKafkaConnection.Create('The cluster is closed');
      end;
      FCoordinators.AddOrSetValue(ANodeId, Result);
    finally
      FLock.Leave;
    end;
  finally
    FConnectLock.Leave;
  end;
end;

function TKafkaCluster.TryMetadata(const ABroker: IKafkaBrokerConnection;
  const ARequest: TKafkaMetadataRequest; var AResponse: TKafkaMetadataResponse;
  var AErrors: string): Boolean;
begin
  try
    FLock.Enter;
    Inc(FRefreshCount);
    FLock.Leave;
    AResponse := ABroker.Connection.Metadata(ARequest);
    Result := True;
  except
    on E: EKafkaConnection do
    begin
      AErrors := AErrors + '; ' + ABroker.Connection.Address + ': ' + E.Message;
      Result := False;
    end;
    on E: EKafkaTimeout do
    begin
      AErrors := AErrors + '; ' + ABroker.Connection.Address + ': ' + E.Message;
      Result := False;
    end;
  end;
end;

function TKafkaCluster.DoRefresh: IKafkaClusterMetadata;
var
  LRequest: TKafkaMetadataRequest;
  LResponse: TKafkaMetadataResponse;
  LTopics: TKafkaStringArray;
  LKnown: TKafkaBrokerInfoArray;
  LBroker: IKafkaBrokerConnection;
  LErrors: string;
  LDone: Boolean;
  LAuthFailed: Boolean;
  I: Integer;
begin
  FRefreshLock.Enter;
  try
    LTopics := TrackedTopics;
    LRequest.Init;
    // No tracked topic: an empty list (not null), which asks for none.
    SetLength(LRequest.Topics, Length(LTopics));
    for I := 0 to High(LTopics) do
    begin
      LRequest.Topics[I].Init;
      LRequest.Topics[I].Name := LTopics[I];
    end;
    LRequest.AllowAutoTopicCreation := FParams.AllowAutoTopicCreation;
    LResponse.Init;
    LErrors := '';
    LDone := False;
    LAuthFailed := False;
    // The brokers already known, then the bootstrap addresses.
    LKnown := nil;
    FLock.Enter;
    try
      if FMetadata <> nil then
        LKnown := FMetadata.Brokers;
    finally
      FLock.Leave;
    end;
    for I := 0 to High(LKnown) do
    begin
      try
        LBroker := Broker(LKnown[I].NodeId);
      except
        // Any failure moves on to the next broker, as Java's client does
        // (a TLS port spoken to in plaintext is an EKafkaProtocol).
        on E: EKafka do
        begin
          CheckUsable;
          if E is EKafkaAuthentication then
            LAuthFailed := True;
          LErrors := LErrors + Format('; broker %d (%s:%d): %s',
            [LKnown[I].NodeId, LKnown[I].Host, LKnown[I].Port, E.Message]);
          Continue;
        end;
      end;
      LDone := TryMetadata(LBroker, LRequest, LResponse, LErrors);
      LBroker := nil;
      if LDone then
        Break;
    end;
    I := 0;
    while (not LDone) and (I <= High(FBootstrap)) do
    begin
      CheckUsable;
      try
        LBroker := OpenConnection(KAFKA_BOOTSTRAP_NODE_ID, FBootstrap[I].Host, FBootstrap[I].Port);
        LDone := TryMetadata(LBroker, LRequest, LResponse, LErrors);
      except
        on E: EKafka do
        begin
          if E is EKafkaAuthentication then
            LAuthFailed := True;
          LErrors := LErrors + Format('; %s:%d: %s', [FBootstrap[I].Host, FBootstrap[I].Port,
            E.Message]);
        end;
      end;
      LBroker := nil; // a bootstrap connection is closed here
      Inc(I);
    end;
    // Credentials a broker refused won't do better on a retry: the caller
    // gets EKafkaAuthentication at once (Topic doesn't wait it out).
    if not LDone and LAuthFailed then
      raise EKafkaAuthentication.Create('No broker answered Metadata' + LErrors);
    if not LDone then
      raise EKafkaConnection.Create('No broker answered Metadata' + LErrors);
    Result := TClusterMetadata.Create(LResponse);
    FLock.Enter;
    try
      if FClosed then
        raise EKafkaConnection.Create('The cluster is closed');
      FMetadata := Result;
      FInvalid := False;
    finally
      FLock.Leave;
    end;
  finally
    FRefreshLock.Leave;
  end;
end;

function TKafkaCluster.Refresh(const ATopics: array of string): IKafkaClusterMetadata;
var
  I: Integer;
begin
  CheckUsable;
  for I := 0 to High(ATopics) do
    Track(ATopics[I]);
  Result := DoRefresh;
end;

function TKafkaCluster.WaitBackoff(ADeadline: UInt64): Boolean;
var
  LNow: UInt64;
  LWait: Cardinal;
begin
  LNow := PcTickMs;
  if LNow >= ADeadline then
    Exit(False);
  LWait := FParams.RetryBackoffMs;
  if LWait = 0 then
    LWait := KAFKA_DEFAULT_RETRY_BACKOFF_MS;
  if ADeadline - LNow < LWait then
    LWait := Cardinal(ADeadline - LNow);
  if FClosedEvent.WaitFor(LWait) = wrSignaled then
    raise EKafkaConnection.Create('The cluster is closed');
  Result := True;
end;

function TKafkaCluster.Topic(const AName: string): TKafkaTopicInfo;
var
  LDeadline: UInt64;
  LMetadata: IKafkaClusterMetadata;
  LRefresh: Boolean;
  LLastError: string;
begin
  CheckUsable;
  LDeadline := PcTickMs + MaxBlockMs;
  LRefresh := Track(AName) or NeedsRefresh;
  Result := Default(TKafkaTopicInfo);
  LLastError := '';
  repeat
    LMetadata := nil;
    if LRefresh then
    begin
      // No broker answering is a reason to wait, like a topic not ready yet
      // (Java's max.block.ms covers both); a closed cluster is not.
      try
        LMetadata := DoRefresh;
      except
        on E: EKafkaConnection do
        begin
          CheckUsable;
          LLastError := E.Message;
        end;
      end;
    end
    else
      LMetadata := Metadata;
    if (LMetadata <> nil) and LMetadata.FindTopic(AName, Result) then
    begin
      LLastError := KafkaErrorText(Result.ErrorCode);
      if Result.IsReady then
        Exit;
      if (Result.ErrorCode <> 0) and not KafkaErrorIsInvalidMetadata(Result.ErrorCode) then
        raise EKafkaBrokerError.Create(Result.ErrorCode, Format('Topic %s: %s',
          [AName, KafkaErrorText(Result.ErrorCode)]));
    end;
    LRefresh := True;
  until not WaitBackoff(LDeadline);
  raise EKafkaTimeout.CreateFmt('Topic %s not ready in %d ms (%s)', [AName, MaxBlockMs, LLastError]);
end;

function TKafkaCluster.Leader(const ATopic: string; APartition: Int32): IKafkaBrokerConnection;
var
  LDeadline: UInt64;
  LTopic: TKafkaTopicInfo;
  P: TKafkaPartitionInfo;
begin
  LDeadline := PcTickMs + MaxBlockMs;
  P := Default(TKafkaPartitionInfo);
  repeat
    LTopic := Topic(ATopic);
    if (APartition < 0) or (APartition > High(LTopic.Partitions)) then
      raise EArgumentOutOfRangeException.CreateFmt('Topic %s has %d partitions; there is no %d',
        [ATopic, Length(LTopic.Partitions), APartition]);
    P := LTopic.Partitions[APartition];
    if (P.LeaderId >= 0) and not KafkaErrorIsInvalidMetadata(P.ErrorCode) then
      Exit(Broker(P.LeaderId));
    Invalidate;
  until not WaitBackoff(LDeadline);
  raise EKafkaTimeout.CreateFmt('Partition %d of %s has no leader after %d ms (%s)',
    [APartition, ATopic, MaxBlockMs, KafkaErrorText(P.ErrorCode)]);
end;

end.
