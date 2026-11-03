unit Kafka.TestFakeBroker;

{ An in-process broker for Kafka.ConnectionTests: it listens on 127.0.0.1,
  port 0, serves any number of connections at once (a thread each), and
  answers ApiVersions and Metadata with the library's own codecs, so the
  connection's behaviour can be tested without Docker and deterministically.

  Connections. Each has its socket, a lock its answers are written under,
  and its own SASL state; everything else (the log, the counters, the held
  and parked requests, the group) is the broker's, under one monitor. A held
  or parked request remembers its connection and is answered there, or not
  at all when that client left. A consumer in a group talks to the
  partition leaders and to the group coordinator on separate connections,
  to the same broker in a one-broker test: that is why. DropClient aborts
  every connection.

  What it can be told to do (set the fields before the client connects, or
  between requests):

  - offer a given range of ApiVersions (a request above it gets
    UNSUPPORTED_VERSION in the v0 format, as a real broker answers, KIP-511)
    and of Metadata, or not offer Metadata at all;
  - HOLD Metadata requests and answer them later, all at once, in reverse
    order (ReleaseHeld), to prove that responses reach their callers by
    correlation id;
  - drop the next N Metadata requests (never answer them);
  - answer a Metadata request with a size prefix larger than the client
    accepts;
  - drop the client's connection (DropClient);
  - describe a cluster (UseClusterView, ClusterView): Metadata answers with
    the brokers and topics of ClusterView, keeping only the topics asked
    for, and UNKNOWN_TOPIC_OR_PARTITION for one it doesn't have, as a real
    broker does. Several fake brokers given the same view make a cluster for
    Kafka.ClusterTests. NotReadyAnswers > 0: that many answers that name
    topics say LEADER_NOT_AVAILABLE for each, with no partitions (a topic
    being created).
  - answer Produce, Fetch and ListOffsets (OfferRecords). Produce: the records of each partition are
    decoded (gzip too) and appended to that partition's log, offsets from 0,
    and the answer gives each batch's base offset. With acks=0 nothing is
    answered. A topic is named (v3-12) or given by id (v13, resolved through
    ClusterView). The next ProduceErrors requests are answered with
    ProduceErrorCode for every partition, appending nothing; the next
    DropProduce requests get no answer at all (and append nothing);
    HoldProduce keeps requests unanswered and unappended until
    ReleaseHeldProduce, which handles them in the order they came.
    AppendBatch appends a batch as a producer would (a control batch
    too). Fetch returns the stored batches that hold the fetch offset or
    come after it, up to the partition's byte limit (the first one whole,
    as a broker does), and OFFSET_OUT_OF_RANGE below the log start
    (SetLogStart) or past the end. A Fetch with nothing to return and a
    MaxWaitMs is parked and answered when records arrive or the wait runs
    out, as a broker does (a thread of its own watches the deadlines). The
    next FetchErrors partitions answer FetchErrorCode. ListOffsets: -2 is
    the log start, -1 the end, any other timestamp the first record at or
    after it.

  - SASL (SaslMechanisms, e.g. 'PLAIN,SCRAM-SHA-256'; '' = a listener
    without SASL, which answers SaslHandshake with ILLEGAL_SASL_STATE, as a
    broker's PLAINTEXT listener does). One user (SaslUser, SaslPassword) for
    both mechanisms. Before a client authenticates, only ApiVersions,
    SaslHandshake and SaslAuthenticate are answered: anything else closes
    the connection, as a broker does. A mechanism not enabled gets
    UNSUPPORTED_SASL_MECHANISM with the list; wrong credentials get
    SASL_AUTHENTICATION_FAILED with Java's messages. SCRAM-SHA-256 is the
    server's side of RFC 5802, written here apart from Kafka.Sasl's client:
    the proof is checked as a server does (the client key recovered from
    the proof must hash to the stored key), with a fixed salt and
    SaslIterations rounds. SaslBadServerSignature sends a wrong server
    signature (a broker that doesn't know the password);
    SaslSessionLifetimeMs goes into SaslAuthenticate v1+ (KIP-368).

  - be every consumer group's coordinator (OfferGroups; KafkaFakeUseView
    turns it on): FindCoordinator answers this broker (CoordinatorNodeId,
    CoordinatorPort), in the list form or, up to FindCoordinatorMax 3, the
    single-key form, and the next FindCoordinatorErrors lookups answer
    FindCoordinatorErrorCode; ConsumerGroupHeartbeat (unless
    OfferConsumerGroupHeartbeat is False), OffsetCommit and OffsetFetch are
    Groups' (Kafka.TestFakeGroup), with ClusterView's topics.

  CreateTls listens with TLS instead, with the server certificate of
  pascal-common-faa's test PKI (CN=localhost, SAN localhost and 127.0.0.1):
  srv.pfx on SChannel (Windows' default backend), the PEM pair elsewhere
  (the common's transport-design.md, T7).

  A Metadata answer names, as its only topic, the topic the request asked for,
  so a test can tell which answer a caller got.

  Not a test unit (its name doesn't end in Tests): tools/gen_fpc_mirror.py
  leaves it alone and both suites compile this one copy. }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  PascalCommon.Transport,
  PascalCommon.ThreadPool,
  Kafka.Types,
  Kafka.Protocol.Primitives,
  Kafka.Protocol.Envelope,
  Kafka.Messages.Apis,
  Kafka.Messages.ApiVersions,
  Kafka.Messages.Metadata,
  Kafka.Messages.Produce,
  Kafka.Messages.Fetch,
  Kafka.Messages.ListOffsets,
  Kafka.Messages.SaslHandshake,
  Kafka.Messages.SaslAuthenticate,
  Kafka.Messages.FindCoordinator,
  Kafka.Messages.ConsumerGroupHeartbeat,
  Kafka.Messages.OffsetCommit,
  Kafka.Messages.OffsetFetch,
  Kafka.Protocol.RecordBatch,
  Kafka.TestFakeGroup,
  Generics.Collections;

type
  TKafkaFakeBroker = class;

  { One client connection: its socket, the lock its answers are written under,
    and its SASL state (under the broker's monitor). Freed with the broker,
    so a held or parked request may keep a reference after the client left
    (answering it then writes nothing). }
  TKafkaFakeConnection = class
  private type
    TServeThread = class(TThread)
    private
      FConnection: TKafkaFakeConnection;
    protected
      procedure Execute; override;
    public
      constructor Create(AConnection: TKafkaFakeConnection);
    end;
    TSaslState = (ssNone, ssHandshaken, ssScramFirst, ssAuthenticated);
  private
    FOwner: TKafkaFakeBroker;
    FEndpoint: TPcEndpoint;
    FWriteLock: TCriticalSection;
    FThread: TServeThread;
    FSaslState: TSaslState;
    FSaslChosen: string;
    FScramClientFirstBare: TBytes;
    FScramServerFirst: TBytes;
    FScramNonce: string;
  public
    constructor Create(AOwner: TKafkaFakeBroker; AEndpoint: TPcEndpoint);
    destructor Destroy; override;
    /// Writes a whole frame; nothing once the client is gone.
    procedure SendRaw(const AFrame: TBytes);
    /// Aborts the socket (the broker "goes away" for this client).
    procedure Abort;
  end;

  TKafkaFakeBroker = class
  private type
    THeld = record
      Connection: TKafkaFakeConnection;
      CorrelationId: Int32;
      Version: Int16;
      Topic: string;
    end;
    // Named: Delphi treats two separate 'array of THeld' declarations as
    // incompatible types (E2008); FPC accepts them.
    THeldArray = array of THeld;

    TLoggedRecord = record
      Topic: string;
      Partition: Int32;
      Offset: Int64;
      Rec: TKafkaRecord;
      Compression: Integer;
    end;
    TLoggedRecordArray = array of TLoggedRecord;

    THeldProduce = record
      Connection: TKafkaFakeConnection;
      CorrelationId: Int32;
      Version: Int16;
      Request: TKafkaProduceRequest;
    end;
    THeldProduceArray = array of THeldProduce;

    TStoredBatch = record
      Topic: string;
      Partition: Int32;
      BaseOffset: Int64;
      NextOffset: Int64;
      Data: TBytes;
    end;
    TStoredBatchArray = array of TStoredBatch;

    TParkedFetch = record
      Connection: TKafkaFakeConnection;
      CorrelationId: Int32;
      Version: Int16;
      Request: TKafkaFetchRequest;
      Deadline: UInt64;
    end;
    TParkedFetchArray = array of TParkedFetch;

    TParkThread = class(TThread)
    private
      FOwner: TKafkaFakeBroker;
    protected
      procedure Execute; override;
    public
      constructor Create(AOwner: TKafkaFakeBroker);
    end;

    TAcceptThread = class(TThread)
    private
      FOwner: TKafkaFakeBroker;
    protected
      procedure Execute; override;
    public
      constructor Create(AOwner: TKafkaFakeBroker);
    end;
  private
    FListener: TPcListener;
    FThread: TAcceptThread;
    // Every connection accepted, live or gone, under FMonitor; freed by
    // Destroy.
    FConnections: TObjectList<TKafkaFakeConnection>;
    FMonitor: TPcMonitor;
    FHeld: THeldArray;
    FRequestCount: Integer;
    FLastClientId: string;
    FLastHeaderVersion: Int16;
    FMetadataCount: Integer;
    FProduceCount: Integer;
    FLog: TLoggedRecordArray;
    FLogCount: Integer;
    FHeldProduce: THeldProduceArray;
    FBatches: TStoredBatchArray;
    FBatchCount: Integer;
    FLogStart: TDictionary<string, Int64>;
    FParked: TParkedFetchArray;
    FParkThread: TParkThread;
    FStopping: Boolean;
    FFetchCount: Integer;
    FListOffsetsCount: Integer;
    FConnectionCount: Integer;
    FSaslAuthenticatedCount: Integer;
    FLastSaslMechanism: string;
    FGroups: TKafkaFakeGroups;
    FFindCoordinatorCount: Integer;
    FLastFindCoordinatorVersion: Int16;
    function HandleGroup(AConn: TKafkaFakeConnection; AKey, AVersion: Int16;
      const AContent: TBytes): Boolean;
    function FindCoordinatorBody(AVersion: Int16; const ARequest: TKafkaFindCoordinatorRequest): TBytes;
    function HandleSasl(AConn: TKafkaFakeConnection; AKey, AVersion: Int16;
      const AContent: TBytes): Boolean;
    function SaslAuthenticateBody(AConn: TKafkaFakeConnection; AVersion: Int16;
      const AAuthBytes: TBytes): TBytes;
    procedure Accept;
    procedure StoreBatch(const ATopic: string; APartition: Int32; var ABatch: TKafkaRecordBatch);
    function LogStartOf(const ATopic: string; APartition: Int32): Int64;
    function FetchTopicName(AVersion: Int16; const ATopic: TKafkaFetchTopic): string;
    /// The answer, or False when it has no records and may be parked.
    function BuildFetch(AVersion: Int16; const ARequest: TKafkaFetchRequest; AForce: Boolean;
      out ABody: TBytes): Boolean;
    procedure HandleListOffsets(AConn: TKafkaFakeConnection; ACorrelationId: Int32;
      AVersion: Int16; const ARequest: TKafkaListOffsetsRequest);
    procedure ParkLoop;
    procedure HandleProduce(AConn: TKafkaFakeConnection; ACorrelationId: Int32; AVersion: Int16;
      const ARequest: TKafkaProduceRequest);
    function TopicNameOf(AVersion: Int16; const ATopic: TKafkaProduceRequestTopicProduceData): string;
    function NextOffset(const ATopic: string; APartition: Int32): Int64;
    procedure ServeClient(AConn: TKafkaFakeConnection);
    procedure ConnectionEnded(AConn: TKafkaFakeConnection);
    procedure HandleRequest(AConn: TKafkaFakeConnection; const AContent: TBytes);
    procedure SendResponse(AConn: TKafkaFakeConnection; ACorrelationId: Int32;
      AApiKey, AVersion: Int16; const ABody: TBytes);
    function ApiVersionsBody(AVersion: Int16): TBytes;
    function MetadataBody(AVersion: Int16; const ATopic: string): TBytes;
    function ClusterViewBody(AVersion: Int16; const ARequest: TKafkaMetadataRequest): TBytes;
  public
    ApiVersionsMin, ApiVersionsMax: Int16;
    MetadataMin, MetadataMax: Int16;
    OfferMetadata: Boolean;
    HoldMetadata: Boolean;
    DropNextMetadata: Integer;
    OversizedMetadata: Boolean;
    UseClusterView: Boolean;
    ClusterView: TKafkaMetadataResponse;
    NotReadyAnswers: Integer;
    OfferRecords: Boolean;
    ProduceErrors: Integer;
    ProduceErrorCode: Int16;
    DropProduce: Integer;
    HoldProduce: Boolean;
    FetchErrors: Integer;
    FetchErrorCode: Int16;
    SaslMechanisms: string;
    SaslUser: string;
    SaslPassword: string;
    SaslIterations: Integer;
    SaslBadServerSignature: Boolean;
    SaslSessionLifetimeMs: Int64;
    /// Offer and answer FindCoordinator (up to FindCoordinatorMax),
    /// ConsumerGroupHeartbeat (unless OfferConsumerGroupHeartbeat is False),
    /// OffsetCommit (v2-v10) and OffsetFetch (v8-v10): the broker is every
    /// group's coordinator (Groups, Kafka.TestFakeGroup), with ClusterView's
    /// topics. KafkaFakeUseView turns it on.
    OfferGroups: Boolean;
    OfferConsumerGroupHeartbeat: Boolean;
    FindCoordinatorMax: Int16;
    /// What FindCoordinator answers: node CoordinatorNodeId (1) at
    /// 127.0.0.1:CoordinatorPort (0: this broker's port).
    CoordinatorNodeId: Int32;
    CoordinatorPort: Integer;
    /// The next FindCoordinatorErrors lookups answer FindCoordinatorErrorCode.
    FindCoordinatorErrors: Integer;
    FindCoordinatorErrorCode: Int16;

    constructor Create;
    constructor CreateTls;
    destructor Destroy; override;
    function Port: Word;
    /// Waits up to ATimeoutMs until ACount Metadata requests are held.
    function WaitHeld(ACount: Integer; ATimeoutMs: Cardinal): Boolean;
    /// Answers every held request, last received first.
    procedure ReleaseHeld;
    /// Aborts every client connection (the broker "goes away").
    procedure DropClient;
    /// Connections accepted since Create.
    function ConnectionCount: Integer;
    /// Connections whose client is still there.
    function LiveConnectionCount: Integer;
    /// Requests received on any connection.
    function RequestCount: Integer;
    /// Metadata requests received (answered or not).
    function MetadataCount: Integer;
    function LastClientId: string;
    function LastHeaderVersion: Int16;
    /// Produce requests received (answered or not).
    function ProduceCount: Integer;
    /// Records appended, in the order they were.
    function LoggedCount: Integer;
    /// Every record appended to ATopic-APartition, in offset order.
    function Logged(const ATopic: string; APartition: Int32): TKafkaRecordArray;
    /// The partition and offset of the I-th record appended (any partition).
    procedure LoggedAt(AIndex: Integer; out ATopic: string; out APartition: Int32;
      out AOffset: Int64; out AValue: TBytes);
    /// The compression codec (attribute bits) the I-th record arrived with.
    function LoggedCompression(AIndex: Integer): Integer;
    /// Waits up to ATimeoutMs until ACount Produce requests came.
    function WaitProduceCount(ACount: Integer; ATimeoutMs: Cardinal): Boolean;
    /// Answers the held Produce requests, oldest first, and stops holding.
    procedure ReleaseHeldProduce;
    /// Appends ABatch to ATopic-APartition as a producer's would be (its
    /// base offset set to the partition's end); returns that base offset.
    function AppendBatch(const ATopic: string; APartition: Int32; ABatch: TKafkaRecordBatch): Int64;
    /// Records below AOffset are gone (a Fetch there: OFFSET_OUT_OF_RANGE).
    procedure SetLogStart(const ATopic: string; APartition: Int32; AOffset: Int64);
    /// Fetch requests received.
    function FetchCount: Integer;
    function ListOffsetsCount: Integer;
    /// Waits up to ATimeoutMs until ACount Fetch requests are parked.
    function WaitParked(ACount: Integer; ATimeoutMs: Cardinal): Boolean;
    /// Successful SASL exchanges, on any connection.
    function SaslAuthenticatedCount: Integer;
    /// The mechanism of the last SaslHandshake ('' before any).
    function LastSaslMechanism: string;
    /// FindCoordinator requests received, and the version of the last.
    function FindCoordinatorCount: Integer;
    function LastFindCoordinatorVersion: Int16;
    /// The groups this broker coordinates (Kafka.TestFakeGroup).
    property Groups: TKafkaFakeGroups read FGroups;
  end;
  TKafkaFakeBrokerArray = array of TKafkaFakeBroker;

/// A cluster of ABrokers (node ids 1..N, in order) with topic ATopic (topic id
/// ...07) whose partition P is led by node ALeaders[P] (-1: no leader); the
/// partitions listed in reverse order, which a client must not mind. ATopic
/// '' gives no topic.
function KafkaFakeClusterView(const ABrokers: TKafkaFakeBrokerArray; const ATopic: string;
  const ALeaders: array of Int32): TKafkaMetadataResponse;
/// Every broker of ABrokers answers with AView, and offers Produce.
procedure KafkaFakeUseView(const ABrokers: TKafkaFakeBrokerArray;
  const AView: TKafkaMetadataResponse);

implementation

uses
  PascalCommon.Threading,
  PascalCommon.Transport.Tcp,
  PascalCommon.Transport.Tls,
  PascalCommon.Crypto,
  PascalCommon.Random,
  PascalCommon.Utf8,
  Kafka.Sasl,
  Kafka.Errors,
  Kafka.Transport,
  Kafka.TestHelpers;

function KafkaFakeClusterView(const ABrokers: TKafkaFakeBrokerArray; const ATopic: string;
  const ALeaders: array of Int32): TKafkaMetadataResponse;
var
  I, P: Integer;
begin
  Result.Init;
  Result.ClusterId := 'fake-cluster';
  Result.ClusterIdIsNull := False;
  Result.ControllerId := 1;
  SetLength(Result.Brokers, Length(ABrokers));
  for I := 0 to High(ABrokers) do
  begin
    Result.Brokers[I].Init;
    Result.Brokers[I].NodeId := I + 1;
    Result.Brokers[I].Host := '127.0.0.1';
    Result.Brokers[I].Port := ABrokers[I].Port;
  end;
  if ATopic = '' then
    Exit;
  SetLength(Result.Topics, 1);
  Result.Topics[0].Init;
  Result.Topics[0].Name := ATopic;
  Result.Topics[0].TopicId.Bytes[15] := 7;
  SetLength(Result.Topics[0].Partitions, Length(ALeaders));
  for I := 0 to High(ALeaders) do
  begin
    P := High(ALeaders) - I;
    Result.Topics[0].Partitions[I].Init;
    Result.Topics[0].Partitions[I].PartitionIndex := P;
    Result.Topics[0].Partitions[I].LeaderId := ALeaders[P];
    Result.Topics[0].Partitions[I].LeaderEpoch := 3;
  end;
end;

procedure KafkaFakeUseView(const ABrokers: TKafkaFakeBrokerArray;
  const AView: TKafkaMetadataResponse);
var
  I: Integer;
begin
  for I := 0 to High(ABrokers) do
  begin
    ABrokers[I].ClusterView := AView;
    ABrokers[I].UseClusterView := True;
    ABrokers[I].OfferRecords := True;
    ABrokers[I].OfferGroups := True;
  end;
end;

{ TKafkaFakeConnection.TServeThread }

constructor TKafkaFakeConnection.TServeThread.Create(AConnection: TKafkaFakeConnection);
begin
  inherited Create(True);
  FConnection := AConnection;
  FreeOnTerminate := False;
end;

procedure TKafkaFakeConnection.TServeThread.Execute;
begin
  FConnection.FOwner.ServeClient(FConnection);
end;

{ TKafkaFakeConnection }

constructor TKafkaFakeConnection.Create(AOwner: TKafkaFakeBroker; AEndpoint: TPcEndpoint);
begin
  inherited Create;
  FOwner := AOwner;
  FEndpoint := AEndpoint;
  FWriteLock := TCriticalSection.Create;
  FThread := TServeThread.Create(Self);
end;

destructor TKafkaFakeConnection.Destroy;
begin
  // The broker joined the thread already (it aborts the socket first).
  FThread.Free;
  FEndpoint.Free;
  FWriteLock.Free;
  inherited;
end;

procedure TKafkaFakeConnection.SendRaw(const AFrame: TBytes);
begin
  FWriteLock.Enter;
  try
    if FEndpoint <> nil then
      KafkaWriteExactly(FEndpoint, AFrame[0], Length(AFrame), 'fake broker');
  finally
    FWriteLock.Leave;
  end;
end;

procedure TKafkaFakeConnection.Abort;
begin
  FWriteLock.Enter;
  try
    if FEndpoint <> nil then
      FEndpoint.CloseAbort;
  finally
    FWriteLock.Leave;
  end;
end;

{ TKafkaFakeBroker.TAcceptThread }

constructor TKafkaFakeBroker.TAcceptThread.Create(AOwner: TKafkaFakeBroker);
begin
  inherited Create(True);
  FOwner := AOwner;
  FreeOnTerminate := False;
end;

procedure TKafkaFakeBroker.TAcceptThread.Execute;
begin
  FOwner.Accept;
end;

{ TKafkaFakeBroker }

constructor TKafkaFakeBroker.CreateTls;
var
  LTls: TPcTlsOptions;
  LPki: string;
begin
  LPki := KafkaTestPkiDir;
  LTls := Default(TPcTlsOptions);
  {$IFDEF MSWINDOWS}
  LTls.CertFile := LPki + 'srv.pfx';
  LTls.CertPassword := 'pipestest';
  {$ELSE}
  LTls.CertFile := LPki + 'srv_cert.pem';
  LTls.KeyFile := LPki + 'srv_key.pem';
  {$ENDIF}
  FListener := PcTlsListen('127.0.0.1', 0, Default(TPcTcpOptions), LTls);
  Create;
end;

constructor TKafkaFakeBroker.Create;
begin
  inherited Create;
  ApiVersionsMin := 0;
  ApiVersionsMax := 5;
  MetadataMin := 0;
  MetadataMax := 13;
  OfferMetadata := True;
  SaslIterations := 4096;
  OfferConsumerGroupHeartbeat := True;
  FindCoordinatorMax := 6;
  CoordinatorNodeId := 1;
  FLastFindCoordinatorVersion := -1;
  FMonitor := TPcMonitor.Create;
  FGroups := TKafkaFakeGroups.Create(FMonitor);
  FConnections := TObjectList<TKafkaFakeConnection>.Create(True);
  if FListener = nil then
    FListener := PcTcpListen('127.0.0.1', 0, Default(TPcTcpOptions));
  FLogStart := TDictionary<string, Int64>.Create;
  FThread := TAcceptThread.Create(Self);
  FThread.Start;
  FParkThread := TParkThread.Create(Self);
  FParkThread.Start;
end;

destructor TKafkaFakeBroker.Destroy;
var
  I: Integer;
begin
  // No connection is accepted once the accept thread is gone, so DropClient
  // then reaches every one.
  FListener.Close;
  FThread.WaitFor;
  FThread.Free;
  DropClient;
  for I := 0 to FConnections.Count - 1 do
    FConnections[I].FThread.WaitFor;
  FMonitor.Enter;
  FStopping := True;
  FMonitor.PulseAll;
  FMonitor.Leave;
  FParkThread.WaitFor;
  FParkThread.Free;
  FConnections.Free;
  FLogStart.Free;
  FListener.Free;
  FGroups.Free;
  FMonitor.Free;
  inherited;
end;

function TKafkaFakeBroker.Port: Word;
begin
  Result := FListener.Port;
end;

procedure TKafkaFakeBroker.Accept;
var
  LClient: TPcEndpoint;
  LConn: TKafkaFakeConnection;
begin
  try
    while True do
    begin
      LClient := FListener.Accept;
      if LClient = nil then
        Exit; // listener closed
      LConn := TKafkaFakeConnection.Create(Self, LClient);
      FMonitor.Enter;
      try
        FConnections.Add(LConn);
        Inc(FConnectionCount);
      finally
        FMonitor.Leave;
      end;
      LConn.FThread.Start;
    end;
  except
    // A test broker never takes the test runner down.
  end;
end;

procedure TKafkaFakeBroker.ConnectionEnded(AConn: TKafkaFakeConnection);
var
  LKeep: TParkedFetchArray;
  I, K: Integer;
  LEndpoint: TPcEndpoint;
begin
  AConn.FWriteLock.Enter;
  try
    LEndpoint := AConn.FEndpoint;
    AConn.FEndpoint := nil;
  finally
    AConn.FWriteLock.Leave;
  end;
  LEndpoint.Free;
  // Its parked fetches' correlation ids went with it.
  FMonitor.Enter;
  try
    LKeep := nil;
    K := 0;
    for I := 0 to High(FParked) do
      if FParked[I].Connection <> AConn then
      begin
        SetLength(LKeep, K + 1);
        LKeep[K] := FParked[I];
        Inc(K);
      end;
    FParked := LKeep;
    FMonitor.PulseAll;
  finally
    FMonitor.Leave;
  end;
end;

procedure TKafkaFakeBroker.ServeClient(AConn: TKafkaFakeConnection);
var
  LSize: array[0..3] of Byte;
  N: Int32;
  LContent: TBytes;
  LClient: TPcEndpoint;
begin
  FillChar(LSize, SizeOf(LSize), 0);
  // Only this thread frees the endpoint (ConnectionEnded), so it can read
  // without the write lock.
  LClient := AConn.FEndpoint;
  try
    try
      try
        LClient.Handshake; // server-side TLS; nothing on plain TCP
      except
        on EPcTransport do
          Exit; // a client that doesn't trust us, for instance
      end;
      while True do
      begin
        KafkaReadExactly(LClient, LSize[0], 4, 'fake broker');
        N := Int32((Cardinal(LSize[0]) shl 24) or (Cardinal(LSize[1]) shl 16) or
          (Cardinal(LSize[2]) shl 8) or Cardinal(LSize[3]));
        if (N < 8) or (N > 1024 * 1024) then
          Exit; // not Kafka (a TLS ClientHello, for instance): hang up
        LContent := nil;
        SetLength(LContent, N);
        KafkaReadExactly(LClient, LContent[0], N, 'fake broker');
        HandleRequest(AConn, LContent);
      end;
    except
      on EKafka do ; // the client went away, or DropClient
    end;
  finally
    try
      ConnectionEnded(AConn);
    except
      // A test broker never takes the test runner down.
    end;
  end;
end;

function TKafkaFakeBroker.ConnectionCount: Integer;
begin
  FMonitor.Enter;
  Result := FConnectionCount;
  FMonitor.Leave;
end;

function TKafkaFakeBroker.LiveConnectionCount: Integer;
var
  I: Integer;
begin
  Result := 0;
  FMonitor.Enter;
  try
    for I := 0 to FConnections.Count - 1 do
    begin
      FConnections[I].FWriteLock.Enter;
      if FConnections[I].FEndpoint <> nil then
        Inc(Result);
      FConnections[I].FWriteLock.Leave;
    end;
  finally
    FMonitor.Leave;
  end;
end;

procedure TKafkaFakeBroker.HandleRequest(AConn: TKafkaFakeConnection; const AContent: TBytes);
var
  LKey, LVersion: Int16;
  LReader: TKafkaReader;
  LHeader: TKafkaRequestHeader;
  LHeaderVersion: Int16;
  LBody: TBytes;
  LMetadata: TKafkaMetadataRequest;
  LTopic: string;
  LBig: TBytes;
  N: Integer;
  LProduce: TKafkaProduceRequest;
  LFetch: TKafkaFetchRequest;
  LListOffsets: TKafkaListOffsetsRequest;
begin
  KafkaPeekRequestApi(AContent, LKey, LVersion);
  FMonitor.Enter;
  Inc(FRequestCount);
  FMonitor.Leave;
  if HandleSasl(AConn, LKey, LVersion, AContent) then
    Exit;
  if HandleGroup(AConn, LKey, LVersion, AContent) then
    Exit;
  if LKey = KAFKA_API_KEY_API_VERSIONS then
  begin
    LReader := TKafkaReader.Create(AContent);
    try
      if LVersion > ApiVersionsMax then
      begin
        // KIP-511: an unknown version gets the error in v0, read with a v1 header.
        LHeader := KafkaReadRequestHeader(LReader, 1);
        SendResponse(AConn, LHeader.CorrelationId, LKey, 0, ApiVersionsBody(-1));
        Exit;
      end;
      LHeaderVersion := KafkaRequestHeaderVersion(KafkaApiIsFlexible(LKey, LVersion));
      LHeader := KafkaReadRequestHeader(LReader, LHeaderVersion);
    finally
      LReader.Free;
    end;
    FMonitor.Enter;
    FLastClientId := LHeader.ClientId;
    FLastHeaderVersion := LHeaderVersion;
    FMonitor.Leave;
    SendResponse(AConn, LHeader.CorrelationId, LKey, LVersion, ApiVersionsBody(LVersion));
    Exit;
  end;
  if LKey = KAFKA_API_KEY_PRODUCE then
  begin
    LHeaderVersion := KafkaRequestHeaderVersion(KafkaApiIsFlexible(LKey, LVersion));
    LReader := TKafkaReader.Create(AContent);
    try
      LHeader := KafkaReadRequestHeader(LReader, LHeaderVersion);
      LBody := Copy(AContent, LReader.Position, Length(AContent) - LReader.Position);
    finally
      LReader.Free;
    end;
    LProduce.FromBytes(LBody, LVersion);
    FMonitor.Enter;
    try
      Inc(FProduceCount);
      FMonitor.PulseAll;
      if DropProduce > 0 then
      begin
        Dec(DropProduce);
        Exit;
      end;
      if HoldProduce then
      begin
        N := Length(FHeldProduce);
        SetLength(FHeldProduce, N + 1);
        FHeldProduce[N].Connection := AConn;
        FHeldProduce[N].CorrelationId := LHeader.CorrelationId;
        FHeldProduce[N].Version := LVersion;
        FHeldProduce[N].Request := LProduce;
        Exit;
      end;
    finally
      FMonitor.Leave;
    end;
    HandleProduce(AConn, LHeader.CorrelationId, LVersion, LProduce);
    Exit;
  end;
  if (LKey = KAFKA_API_KEY_FETCH) or (LKey = KAFKA_API_KEY_LIST_OFFSETS) then
  begin
    LHeaderVersion := KafkaRequestHeaderVersion(KafkaApiIsFlexible(LKey, LVersion));
    LReader := TKafkaReader.Create(AContent);
    try
      LHeader := KafkaReadRequestHeader(LReader, LHeaderVersion);
      LBody := Copy(AContent, LReader.Position, Length(AContent) - LReader.Position);
    finally
      LReader.Free;
    end;
    if LKey = KAFKA_API_KEY_LIST_OFFSETS then
    begin
      LListOffsets.FromBytes(LBody, LVersion);
      HandleListOffsets(AConn, LHeader.CorrelationId, LVersion, LListOffsets);
      Exit;
    end;
    LFetch.FromBytes(LBody, LVersion);
    FMonitor.Enter;
    try
      Inc(FFetchCount);
      if not BuildFetch(LVersion, LFetch, LFetch.MaxWaitMs <= 0, LBig) then
      begin
        N := Length(FParked);
        SetLength(FParked, N + 1);
        FParked[N].Connection := AConn;
        FParked[N].CorrelationId := LHeader.CorrelationId;
        FParked[N].Version := LVersion;
        FParked[N].Request := LFetch;
        FParked[N].Deadline := PcTickMs + Cardinal(LFetch.MaxWaitMs);
        FMonitor.PulseAll;
        Exit;
      end;
    finally
      FMonitor.Leave;
    end;
    SendResponse(AConn, LHeader.CorrelationId, LKey, LVersion, LBig);
    Exit;
  end;
  if LKey <> KAFKA_API_KEY_METADATA then
    Exit; // nothing else is spoken here
  LHeaderVersion := KafkaRequestHeaderVersion(KafkaApiIsFlexible(LKey, LVersion));
  LReader := TKafkaReader.Create(AContent);
  try
    LHeader := KafkaReadRequestHeader(LReader, LHeaderVersion);
    LBody := Copy(AContent, LReader.Position, Length(AContent) - LReader.Position);
  finally
    LReader.Free;
  end;
  LMetadata.FromBytes(LBody, LVersion);
  LTopic := '';
  if Length(LMetadata.Topics) > 0 then
    LTopic := LMetadata.Topics[0].Name;
  FMonitor.Enter;
  try
    Inc(FMetadataCount);
    FLastClientId := LHeader.ClientId;
    FLastHeaderVersion := LHeaderVersion;
    if UseClusterView then
    begin
      LBig := ClusterViewBody(LVersion, LMetadata);
      SendResponse(AConn, LHeader.CorrelationId, LKey, LVersion, LBig);
      Exit;
    end;
    if DropNextMetadata > 0 then
    begin
      Dec(DropNextMetadata);
      Exit;
    end;
    if HoldMetadata then
    begin
      N := Length(FHeld);
      SetLength(FHeld, N + 1);
      FHeld[N].Connection := AConn;
      FHeld[N].CorrelationId := LHeader.CorrelationId;
      FHeld[N].Version := LVersion;
      FHeld[N].Topic := LTopic;
      FMonitor.PulseAll;
      Exit;
    end;
  finally
    FMonitor.Leave;
  end;
  if OversizedMetadata then
  begin
    // A size prefix of 1 MB, then the correlation id; the client must refuse
    // it before reading the rest.
    LBig := nil;
    SetLength(LBig, 8);
    LBig[1] := $10; // $00100000 = 1 MB
    LBig[4] := Byte(Cardinal(LHeader.CorrelationId) shr 24);
    LBig[5] := Byte(Cardinal(LHeader.CorrelationId) shr 16);
    LBig[6] := Byte(Cardinal(LHeader.CorrelationId) shr 8);
    LBig[7] := Byte(LHeader.CorrelationId);
    AConn.SendRaw(LBig);
    Exit;
  end;
  SendResponse(AConn, LHeader.CorrelationId, LKey, LVersion, MetadataBody(LVersion, LTopic));
end;

function ConcatBytes(const A, B: TBytes): TBytes;
begin
  Result := nil;
  SetLength(Result, Length(A) + Length(B));
  if Length(A) > 0 then
    Move(A[0], Result[0], Length(A));
  if Length(B) > 0 then
    Move(B[0], Result[Length(A)], Length(B));
end;

function FakeHmac(const AKey, AData: TBytes): TBytes;
begin
  Result := PcDigestToBytes(PcHmacSha256(AKey, AData));
end;

/// The value of attribute AName ('r', 'p'...) in a SCRAM message; '' if absent.
function ScramAttribute(const AText: string; AName: Char): string;
var
  LRest: string;
  P: Integer;
begin
  LRest := AText;
  while LRest <> '' do
  begin
    P := Pos(',', LRest);
    if P = 0 then
      P := Length(LRest) + 1;
    if (Length(LRest) >= 2) and (LRest[1] = AName) and (LRest[2] = '=') then
      Exit(Copy(LRest, 3, P - 3));
    Delete(LRest, 1, P);
  end;
  Result := '';
end;

function TKafkaFakeBroker.HandleSasl(AConn: TKafkaFakeConnection; AKey, AVersion: Int16;
  const AContent: TBytes): Boolean;
var
  LReader: TKafkaReader;
  LHeader: TKafkaRequestHeader;
  LBody: TBytes;
  LHandshake: TKafkaSaslHandshakeRequest;
  LHandshakeAnswer: TKafkaSaslHandshakeResponse;
  LAuth: TKafkaSaslAuthenticateRequest;
  LState: TKafkaFakeConnection.TSaslState;
  LMechanisms: string;
  P, N: Integer;
begin
  FMonitor.Enter;
  try
    LState := AConn.FSaslState;
    LMechanisms := SaslMechanisms;
  finally
    FMonitor.Leave;
  end;
  Result := (AKey = KAFKA_API_KEY_SASL_HANDSHAKE) or (AKey = KAFKA_API_KEY_SASL_AUTHENTICATE);
  if not Result then
  begin
    // A SASL listener answers nothing else before the client authenticated:
    // the broker closes the connection (ServeClient ends on EKafka).
    if (LMechanisms <> '') and (LState <> ssAuthenticated) and
      (AKey <> KAFKA_API_KEY_API_VERSIONS) then
      raise EKafkaConnection.Create('fake broker: a request before SASL authentication');
    Exit;
  end;
  LReader := TKafkaReader.Create(AContent);
  try
    LHeader := KafkaReadRequestHeader(LReader,
      KafkaRequestHeaderVersion(KafkaApiIsFlexible(AKey, AVersion)));
    LBody := Copy(AContent, LReader.Position, Length(AContent) - LReader.Position);
  finally
    LReader.Free;
  end;
  if AKey = KAFKA_API_KEY_SASL_HANDSHAKE then
  begin
    LHandshake.FromBytes(LBody, AVersion);
    LHandshakeAnswer.Init;
    FMonitor.Enter;
    try
      FLastSaslMechanism := LHandshake.Mechanism;
      if (LMechanisms = '') or (LState <> ssNone) then
        LHandshakeAnswer.ErrorCode := KAFKA_ERROR_ILLEGAL_SASL_STATE
      else if Pos(',' + LHandshake.Mechanism + ',', ',' + LMechanisms + ',') = 0 then
        LHandshakeAnswer.ErrorCode := KAFKA_ERROR_UNSUPPORTED_SASL_MECHANISM
      else
      begin
        AConn.FSaslChosen := LHandshake.Mechanism;
        AConn.FSaslState := ssHandshaken;
      end;
    finally
      FMonitor.Leave;
    end;
    // The enabled mechanisms, as a broker lists them.
    N := 0;
    while LMechanisms <> '' do
    begin
      P := Pos(',', LMechanisms);
      if P = 0 then
        P := Length(LMechanisms) + 1;
      SetLength(LHandshakeAnswer.Mechanisms, N + 1);
      LHandshakeAnswer.Mechanisms[N] := Copy(LMechanisms, 1, P - 1);
      Inc(N);
      Delete(LMechanisms, 1, P);
    end;
    SendResponse(AConn, LHeader.CorrelationId, AKey, AVersion, LHandshakeAnswer.ToBytes(AVersion));
    Exit;
  end;
  LAuth.FromBytes(LBody, AVersion);
  SendResponse(AConn, LHeader.CorrelationId, AKey, AVersion,
    SaslAuthenticateBody(AConn, AVersion, LAuth.AuthBytes));
end;

function TKafkaFakeBroker.SaslAuthenticateBody(AConn: TKafkaFakeConnection; AVersion: Int16;
  const AAuthBytes: TBytes): TBytes;
const
  SALT = 'fake-broker-salt';
var
  R: TKafkaSaslAuthenticateResponse;
  LText, LUser, LFinalWithoutProof: string;
  LParts: array[0..2] of string;
  LSalted, LClientKey, LStoredKey, LServerKey, LAuthMessage, LSignature, LProof: TBytes;
  P, I, K: Integer;
  LOk: Boolean;
begin
  R.Init;
  LUser := '';
  FMonitor.Enter;
  try
    if AConn.FSaslState = ssNone then
    begin
      R.ErrorCode := KAFKA_ERROR_ILLEGAL_SASL_STATE;
      R.ErrorMessage := 'SaslAuthenticate before SaslHandshake';
      R.ErrorMessageIsNull := False;
    end
    else if AConn.FSaslState = ssAuthenticated then
    begin
      R.ErrorCode := KAFKA_ERROR_ILLEGAL_SASL_STATE;
      R.ErrorMessage := 'Already authenticated';
      R.ErrorMessageIsNull := False;
    end
    else if AConn.FSaslChosen = 'PLAIN' then
    begin
      // authzid NUL authcid NUL passwd
      LOk := PcTryUtf8BytesToString(AAuthBytes, LText);
      K := 0;
      LParts[0] := '';
      LParts[1] := '';
      LParts[2] := '';
      if LOk then
        for I := 1 to Length(LText) do
          if LText[I] = #0 then
          begin
            Inc(K);
            if K > 2 then
              Break;
          end
          else
            LParts[K] := LParts[K] + LText[I];
      if LOk and (K = 2) and (LParts[1] = SaslUser) and (LParts[2] = SaslPassword) then
      begin
        AConn.FSaslState := ssAuthenticated;
        Inc(FSaslAuthenticatedCount);
        R.SessionLifetimeMs := SaslSessionLifetimeMs;
      end
      else
      begin
        R.ErrorCode := KAFKA_ERROR_SASL_AUTHENTICATION_FAILED;
        R.ErrorMessage := 'Authentication failed: Invalid username or password';
        R.ErrorMessageIsNull := False;
      end;
    end
    else if AConn.FSaslState = ssHandshaken then
    begin
      // SCRAM client-first: n,,n=<name>,r=<nonce>
      LOk := PcTryUtf8BytesToString(AAuthBytes, LText) and (Copy(LText, 1, 3) = 'n,,');
      AConn.FScramNonce := '';
      if LOk then
      begin
        AConn.FScramClientFirstBare := Copy(AAuthBytes, 3, Length(AAuthBytes) - 3);
        LUser := ScramAttribute(Copy(LText, 4, MaxInt), 'n');
        LUser := StringReplace(LUser, '=2C', ',', [rfReplaceAll]);
        LUser := StringReplace(LUser, '=3D', '=', [rfReplaceAll]);
        AConn.FScramNonce := ScramAttribute(Copy(LText, 4, MaxInt), 'r');
      end;
      if not LOk or (AConn.FScramNonce = '') or (LUser <> SaslUser) then
      begin
        R.ErrorCode := KAFKA_ERROR_SASL_AUTHENTICATION_FAILED;
        R.ErrorMessage := 'Authentication failed: Invalid user credentials';
        R.ErrorMessageIsNull := False;
      end
      else
      begin
        AConn.FScramNonce := AConn.FScramNonce + KafkaBase64Encode(PcRandomBytes(18));
        AConn.FScramServerFirst := PcStringToUtf8Bytes('r=' + AConn.FScramNonce + ',s=' +
          KafkaBase64Encode(PcStringToUtf8Bytes(SALT)) + ',i=' + IntToStr(SaslIterations));
        R.AuthBytes := AConn.FScramServerFirst;
        AConn.FSaslState := ssScramFirst;
      end;
    end
    else
    begin
      // SCRAM client-final: c=biws,r=<nonce>,p=<proof>
      LOk := PcTryUtf8BytesToString(AAuthBytes, LText);
      P := Pos(',p=', LText);
      LOk := LOk and (P > 0) and (ScramAttribute(LText, 'r') = AConn.FScramNonce) and
        (ScramAttribute(LText, 'c') = 'biws');
      LProof := nil;
      if LOk then
      begin
        LFinalWithoutProof := Copy(LText, 1, P - 1);
        LOk := KafkaTryBase64Decode(Copy(LText, P + 3, MaxInt), LProof) and
          (Length(LProof) = 32);
      end;
      if LOk then
      begin
        LSalted := PcPbkdf2HmacSha256(PcStringToUtf8Bytes(SaslPassword),
          PcStringToUtf8Bytes(SALT), SaslIterations, 32);
        LStoredKey := PcDigestToBytes(PcSha256(FakeHmac(LSalted,
          PcStringToUtf8Bytes('Client Key'))));
        LServerKey := FakeHmac(LSalted, PcStringToUtf8Bytes('Server Key'));
        LAuthMessage := ConcatBytes(ConcatBytes(AConn.FScramClientFirstBare, PcStringToUtf8Bytes(',')),
          ConcatBytes(ConcatBytes(AConn.FScramServerFirst, PcStringToUtf8Bytes(',')),
          PcStringToUtf8Bytes(LFinalWithoutProof)));
        // A server's check: ClientKey = proof xor HMAC(StoredKey, AuthMessage),
        // and H(ClientKey) must be the stored key.
        LSignature := FakeHmac(LStoredKey, LAuthMessage);
        LClientKey := nil;
        SetLength(LClientKey, 32);
        for I := 0 to 31 do
          LClientKey[I] := LProof[I] xor LSignature[I];
        LOk := PcSameBytes(PcDigestToBytes(PcSha256(LClientKey)), LStoredKey);
      end;
      if not LOk then
      begin
        R.ErrorCode := KAFKA_ERROR_SASL_AUTHENTICATION_FAILED;
        R.ErrorMessage := 'Authentication failed: Invalid user credentials';
        R.ErrorMessageIsNull := False;
      end
      else
      begin
        LSignature := FakeHmac(LServerKey, LAuthMessage);
        if SaslBadServerSignature then
          LSignature[0] := LSignature[0] xor $FF;
        R.AuthBytes := PcStringToUtf8Bytes('v=' + KafkaBase64Encode(LSignature));
        R.SessionLifetimeMs := SaslSessionLifetimeMs;
        AConn.FSaslState := ssAuthenticated;
        Inc(FSaslAuthenticatedCount);
      end;
    end;
    if R.ErrorCode <> 0 then
      AConn.FSaslState := ssNone;
  finally
    FMonitor.Leave;
  end;
  // SessionLifetimeMs is ignorable: v0 drops it rather than refusing it.
  Result := R.ToBytes(AVersion);
end;

function TKafkaFakeBroker.FindCoordinatorBody(AVersion: Int16;
  const ARequest: TKafkaFindCoordinatorRequest): TBytes;
var
  R: TKafkaFindCoordinatorResponse;
  LError: Int16;
  LPort: Integer;
  I: Integer;
begin
  FMonitor.Enter;
  try
    Inc(FFindCoordinatorCount);
    FLastFindCoordinatorVersion := AVersion;
    LError := 0;
    if FindCoordinatorErrors > 0 then
    begin
      Dec(FindCoordinatorErrors);
      LError := FindCoordinatorErrorCode;
    end;
    LPort := CoordinatorPort;
    if LPort = 0 then
      LPort := Port;
    R.Init;
    if AVersion >= 4 then
    begin
      SetLength(R.Coordinators, Length(ARequest.CoordinatorKeys));
      for I := 0 to High(ARequest.CoordinatorKeys) do
      begin
        R.Coordinators[I].Init;
        R.Coordinators[I].Key := ARequest.CoordinatorKeys[I];
        R.Coordinators[I].ErrorCode := LError;
        if LError = 0 then
        begin
          R.Coordinators[I].NodeId := CoordinatorNodeId;
          R.Coordinators[I].Host := '127.0.0.1';
          R.Coordinators[I].Port := LPort;
        end
        else
          R.Coordinators[I].NodeId := -1;
      end;
    end
    else
    begin
      R.ErrorCode := LError;
      if LError = 0 then
      begin
        R.NodeId := CoordinatorNodeId;
        R.Host := '127.0.0.1';
        R.Port := LPort;
      end
      else
        R.NodeId := -1;
    end;
    FMonitor.PulseAll;
  finally
    FMonitor.Leave;
  end;
  Result := R.ToBytes(AVersion);
end;

function TKafkaFakeBroker.HandleGroup(AConn: TKafkaFakeConnection; AKey, AVersion: Int16;
  const AContent: TBytes): Boolean;
var
  LReader: TKafkaReader;
  LHeader: TKafkaRequestHeader;
  LBody: TBytes;
  LView: TKafkaMetadataResponse;
  LFind: TKafkaFindCoordinatorRequest;
  LHeartbeat: TKafkaConsumerGroupHeartbeatRequest;
  LCommit: TKafkaOffsetCommitRequest;
  LFetch: TKafkaOffsetFetchRequest;
begin
  Result := (AKey = KAFKA_API_KEY_FIND_COORDINATOR) or
    (AKey = KAFKA_API_KEY_CONSUMER_GROUP_HEARTBEAT) or (AKey = KAFKA_API_KEY_OFFSET_COMMIT) or
    (AKey = KAFKA_API_KEY_OFFSET_FETCH);
  if not Result then
    Exit;
  LReader := TKafkaReader.Create(AContent);
  try
    LHeader := KafkaReadRequestHeader(LReader,
      KafkaRequestHeaderVersion(KafkaApiIsFlexible(AKey, AVersion)));
    LBody := Copy(AContent, LReader.Position, Length(AContent) - LReader.Position);
  finally
    LReader.Free;
  end;
  FMonitor.Enter;
  try
    LView := ClusterView;
  finally
    FMonitor.Leave;
  end;
  case AKey of
    KAFKA_API_KEY_FIND_COORDINATOR:
      begin
        LFind.FromBytes(LBody, AVersion);
        SendResponse(AConn, LHeader.CorrelationId, AKey, AVersion,
          FindCoordinatorBody(AVersion, LFind));
      end;
    KAFKA_API_KEY_CONSUMER_GROUP_HEARTBEAT:
      begin
        LHeartbeat.FromBytes(LBody, AVersion);
        SendResponse(AConn, LHeader.CorrelationId, AKey, AVersion,
          FGroups.Heartbeat(LView, LHeartbeat).ToBytes(AVersion));
      end;
    KAFKA_API_KEY_OFFSET_COMMIT:
      begin
        LCommit.FromBytes(LBody, AVersion);
        SendResponse(AConn, LHeader.CorrelationId, AKey, AVersion,
          FGroups.OffsetCommit(LView, AVersion, LCommit).ToBytes(AVersion));
      end;
    KAFKA_API_KEY_OFFSET_FETCH:
      begin
        LFetch.FromBytes(LBody, AVersion);
        SendResponse(AConn, LHeader.CorrelationId, AKey, AVersion,
          FGroups.OffsetFetch(LView, AVersion, LFetch).ToBytes(AVersion));
      end;
  end;
end;

function TKafkaFakeBroker.FindCoordinatorCount: Integer;
begin
  FMonitor.Enter;
  Result := FFindCoordinatorCount;
  FMonitor.Leave;
end;

function TKafkaFakeBroker.LastFindCoordinatorVersion: Int16;
begin
  FMonitor.Enter;
  Result := FLastFindCoordinatorVersion;
  FMonitor.Leave;
end;

function TKafkaFakeBroker.SaslAuthenticatedCount: Integer;
begin
  FMonitor.Enter;
  Result := FSaslAuthenticatedCount;
  FMonitor.Leave;
end;

function TKafkaFakeBroker.LastSaslMechanism: string;
begin
  FMonitor.Enter;
  Result := FLastSaslMechanism;
  FMonitor.Leave;
end;

function TKafkaFakeBroker.TopicNameOf(AVersion: Int16;
  const ATopic: TKafkaProduceRequestTopicProduceData): string;
var
  K: Integer;
begin
  // Called under FMonitor.
  if AVersion < 13 then
    Exit(ATopic.Name);
  for K := 0 to High(ClusterView.Topics) do
    if ClusterView.Topics[K].TopicId.Equals(ATopic.TopicId) then
      Exit(ClusterView.Topics[K].Name);
  Result := '';
end;

function TKafkaFakeBroker.NextOffset(const ATopic: string; APartition: Int32): Int64;
var
  I: Integer;
begin
  // Called under FMonitor. From the batches: a control batch takes offsets
  // too.
  Result := 0;
  for I := 0 to FBatchCount - 1 do
    if (FBatches[I].Topic = ATopic) and (FBatches[I].Partition = APartition) and
      (FBatches[I].NextOffset > Result) then
      Result := FBatches[I].NextOffset;
end;

procedure TKafkaFakeBroker.HandleProduce(AConn: TKafkaFakeConnection; ACorrelationId: Int32;
  AVersion: Int16;
  const ARequest: TKafkaProduceRequest);
var
  R: TKafkaProduceResponse;
  T, P, B, K: Integer;
  LName: string;
  LBatches: TKafkaRecordBatchArray;
  LTrailing: Integer;
  LBase: Int64;
  LPart: TKafkaProduceResponsePartitionProduceResponse;
begin
  R.Init;
  SetLength(R.Responses, Length(ARequest.TopicData));
  FMonitor.Enter;
  try
    for T := 0 to High(ARequest.TopicData) do
    begin
      R.Responses[T].Init;
      R.Responses[T].Name := ARequest.TopicData[T].Name;
      R.Responses[T].TopicId := ARequest.TopicData[T].TopicId;
      LName := TopicNameOf(AVersion, ARequest.TopicData[T]);
      SetLength(R.Responses[T].PartitionResponses, Length(ARequest.TopicData[T].PartitionData));
      for P := 0 to High(ARequest.TopicData[T].PartitionData) do
      begin
        LPart.Init;
        LPart.Index := ARequest.TopicData[T].PartitionData[P].Index;
        if LName = '' then
          LPart.ErrorCode := KAFKA_ERROR_UNKNOWN_TOPIC_ID
        else if ProduceErrors > 0 then
          LPart.ErrorCode := ProduceErrorCode
        else
        begin
          LBase := NextOffset(LName, LPart.Index);
          LPart.BaseOffset := LBase;
          LBatches := KafkaDecodeRecordBatches(ARequest.TopicData[T].PartitionData[P].Records,
            16 * 1024 * 1024, LTrailing);
          for B := 0 to High(LBatches) do
          begin
            LBatches[B].BaseOffset := LBase;
            StoreBatch(LName, LPart.Index, LBatches[B]);
            for K := 0 to High(LBatches[B].Records) do
            begin
              if FLogCount = Length(FLog) then
                SetLength(FLog, 2 * FLogCount + 16);
              FLog[FLogCount].Topic := LName;
              FLog[FLogCount].Partition := LPart.Index;
              FLog[FLogCount].Offset := LBase;
              FLog[FLogCount].Rec := LBatches[B].Records[K];
              FLog[FLogCount].Compression := Ord(LBatches[B].Compression);
              Inc(FLogCount);
              Inc(LBase);
            end;
          end;
        end;
        R.Responses[T].PartitionResponses[P] := LPart;
      end;
    end;
    if ProduceErrors > 0 then
      Dec(ProduceErrors);
    FMonitor.PulseAll;
  finally
    FMonitor.Leave;
  end;
  if ARequest.Acks <> 0 then
    SendResponse(AConn, ACorrelationId, KAFKA_API_KEY_PRODUCE, AVersion, R.ToBytes(AVersion));
end;

{ TKafkaFakeBroker.TParkThread }

constructor TKafkaFakeBroker.TParkThread.Create(AOwner: TKafkaFakeBroker);
begin
  inherited Create(True);
  FOwner := AOwner;
  FreeOnTerminate := False;
end;

procedure TKafkaFakeBroker.TParkThread.Execute;
begin
  FOwner.ParkLoop;
end;

procedure TKafkaFakeBroker.ParkLoop;
var
  LAnswers: array of TBytes;
  LIds: array of Int32;
  LConns: array of TKafkaFakeConnection;
  LVersions: array of Int16;
  LKeep: TParkedFetchArray;
  I, N, K: Integer;
  LNow, LNext: UInt64;
  LBody: TBytes;
begin
  try
    while True do
    begin
      LAnswers := nil;
      LIds := nil;
      LConns := nil;
      LVersions := nil;
      FMonitor.Enter;
      try
        if FStopping then
          Exit;
        LNow := PcTickMs;
        LNext := LNow + 1000;
        LKeep := nil;
        N := 0;
        K := 0;
        for I := 0 to High(FParked) do
          if BuildFetch(FParked[I].Version, FParked[I].Request, LNow >= FParked[I].Deadline,
            LBody) then
          begin
            SetLength(LAnswers, N + 1);
            SetLength(LIds, N + 1);
            SetLength(LConns, N + 1);
            SetLength(LVersions, N + 1);
            LAnswers[N] := LBody;
            LIds[N] := FParked[I].CorrelationId;
            LConns[N] := FParked[I].Connection;
            LVersions[N] := FParked[I].Version;
            Inc(N);
          end
          else
          begin
            SetLength(LKeep, K + 1);
            LKeep[K] := FParked[I];
            Inc(K);
            if FParked[I].Deadline < LNext then
              LNext := FParked[I].Deadline;
          end;
        FParked := LKeep;
        if N = 0 then
        begin
          // Woken by a new parked fetch, an append, or the next deadline.
          if LNext > LNow then
            FMonitor.Wait(Cardinal(LNext - LNow));
          Continue;
        end;
      finally
        FMonitor.Leave;
      end;
      for I := 0 to N - 1 do
        SendResponse(LConns[I], LIds[I], KAFKA_API_KEY_FETCH, LVersions[I], LAnswers[I]);
    end;
  except
    // A test broker never takes the test runner down.
  end;
end;

procedure TKafkaFakeBroker.StoreBatch(const ATopic: string; APartition: Int32;
  var ABatch: TKafkaRecordBatch);
var
  I: Integer;
begin
  // Called under FMonitor, ABatch.BaseOffset set.
  if FBatchCount = Length(FBatches) then
    SetLength(FBatches, 2 * FBatchCount + 16);
  FBatches[FBatchCount].Topic := ATopic;
  FBatches[FBatchCount].Partition := APartition;
  FBatches[FBatchCount].BaseOffset := ABatch.BaseOffset;
  FBatches[FBatchCount].NextOffset := ABatch.NextOffset;
  FBatches[FBatchCount].Data := ABatch.Encode;
  for I := 0 to High(ABatch.Records) do
    ABatch.Records[I].Offset := ABatch.BaseOffset + ABatch.Records[I].OffsetDelta;
  Inc(FBatchCount);
end;

function TKafkaFakeBroker.AppendBatch(const ATopic: string; APartition: Int32;
  ABatch: TKafkaRecordBatch): Int64;
var
  K: Integer;
begin
  FMonitor.Enter;
  try
    Result := NextOffset(ATopic, APartition);
    ABatch.BaseOffset := Result;
    StoreBatch(ATopic, APartition, ABatch);
    for K := 0 to High(ABatch.Records) do
    begin
      if FLogCount = Length(FLog) then
        SetLength(FLog, 2 * FLogCount + 16);
      FLog[FLogCount].Topic := ATopic;
      FLog[FLogCount].Partition := APartition;
      FLog[FLogCount].Offset := ABatch.Records[K].Offset;
      FLog[FLogCount].Rec := ABatch.Records[K];
      FLog[FLogCount].Compression := Ord(ABatch.Compression);
      Inc(FLogCount);
    end;
    // A control batch takes its offset without a logged record.
    if (Length(ABatch.Records) > 0) and ABatch.IsControl then
      Dec(FLogCount, Length(ABatch.Records));
    FMonitor.PulseAll;
  finally
    FMonitor.Leave;
  end;
end;

procedure TKafkaFakeBroker.SetLogStart(const ATopic: string; APartition: Int32; AOffset: Int64);
begin
  FMonitor.Enter;
  try
    FLogStart.AddOrSetValue(ATopic + #0 + IntToStr(APartition), AOffset);
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeBroker.LogStartOf(const ATopic: string; APartition: Int32): Int64;
begin
  // Called under FMonitor.
  if not FLogStart.TryGetValue(ATopic + #0 + IntToStr(APartition), Result) then
    Result := 0;
end;

function TKafkaFakeBroker.FetchTopicName(AVersion: Int16; const ATopic: TKafkaFetchTopic): string;
var
  K: Integer;
begin
  // Called under FMonitor.
  if AVersion < 13 then
    Exit(ATopic.Topic);
  for K := 0 to High(ClusterView.Topics) do
    if ClusterView.Topics[K].TopicId.Equals(ATopic.TopicId) then
      Exit(ClusterView.Topics[K].Name);
  Result := '';
end;

function TKafkaFakeBroker.BuildFetch(AVersion: Int16; const ARequest: TKafkaFetchRequest;
  AForce: Boolean; out ABody: TBytes): Boolean;
var
  R: TKafkaFetchResponse;
  T, P, I, LSize: Integer;
  LName: string;
  LReq: TKafkaFetchPartition;
  LPart: TKafkaFetchResponsePartitionData;
  LEnd, LStart: Int64;
  LData: TBytes;
  LAny, LError: Boolean;
begin
  // Called under FMonitor.
  R.Init;
  LAny := False;
  LError := False;
  SetLength(R.Responses, Length(ARequest.Topics));
  for T := 0 to High(ARequest.Topics) do
  begin
    R.Responses[T].Init;
    R.Responses[T].Topic := ARequest.Topics[T].Topic;
    R.Responses[T].TopicId := ARequest.Topics[T].TopicId;
    LName := FetchTopicName(AVersion, ARequest.Topics[T]);
    SetLength(R.Responses[T].Partitions, Length(ARequest.Topics[T].Partitions));
    for P := 0 to High(ARequest.Topics[T].Partitions) do
    begin
      LReq := ARequest.Topics[T].Partitions[P];
      LPart.Init;
      LPart.PartitionIndex := LReq.Partition;
      LEnd := NextOffset(LName, LReq.Partition);
      LStart := LogStartOf(LName, LReq.Partition);
      LPart.HighWatermark := LEnd;
      LPart.LastStableOffset := LEnd;
      LPart.LogStartOffset := LStart;
      if LName = '' then
        LPart.ErrorCode := KAFKA_ERROR_UNKNOWN_TOPIC_ID
      else if FetchErrors > 0 then
      begin
        LPart.ErrorCode := FetchErrorCode;
        Dec(FetchErrors);
      end
      else if (LReq.FetchOffset < LStart) or (LReq.FetchOffset > LEnd) then
        LPart.ErrorCode := KAFKA_ERROR_OFFSET_OUT_OF_RANGE
      else
      begin
        LData := nil;
        for I := 0 to FBatchCount - 1 do
          if (FBatches[I].Topic = LName) and (FBatches[I].Partition = LReq.Partition) and
            (FBatches[I].NextOffset > LReq.FetchOffset) and (FBatches[I].BaseOffset >= LStart) then
          begin
            LSize := Length(LData);
            // The first batch whole, then as many as fit.
            if (LSize > 0) and (LSize + Length(FBatches[I].Data) > LReq.PartitionMaxBytes) then
              Break;
            SetLength(LData, LSize + Length(FBatches[I].Data));
            Move(FBatches[I].Data[0], LData[LSize], Length(FBatches[I].Data));
          end;
        if Length(LData) > 0 then
        begin
          LPart.Records := LData;
          LPart.RecordsIsNull := False;
          LAny := True;
        end;
      end;
      if LPart.ErrorCode <> 0 then
        LError := True;
      if LPart.RecordsIsNull then
      begin
        LPart.Records := nil;
        LPart.RecordsIsNull := False;
      end;
      R.Responses[T].Partitions[P] := LPart;
    end;
  end;
  Result := LAny or LError or AForce;
  if Result then
    ABody := R.ToBytes(AVersion)
  else
    ABody := nil;
end;

procedure TKafkaFakeBroker.HandleListOffsets(AConn: TKafkaFakeConnection; ACorrelationId: Int32;
  AVersion: Int16;
  const ARequest: TKafkaListOffsetsRequest);
var
  R: TKafkaListOffsetsResponse;
  T, P, I: Integer;
  LReq: TKafkaListOffsetsPartition;
  LName: string;
begin
  R.Init;
  FMonitor.Enter;
  try
    Inc(FListOffsetsCount);
    SetLength(R.Topics, Length(ARequest.Topics));
    for T := 0 to High(ARequest.Topics) do
    begin
      LName := ARequest.Topics[T].Name;
      R.Topics[T].Init;
      R.Topics[T].Name := LName;
      SetLength(R.Topics[T].Partitions, Length(ARequest.Topics[T].Partitions));
      for P := 0 to High(ARequest.Topics[T].Partitions) do
      begin
        LReq := ARequest.Topics[T].Partitions[P];
        R.Topics[T].Partitions[P].Init;
        R.Topics[T].Partitions[P].PartitionIndex := LReq.PartitionIndex;
        if LReq.Timestamp = -2 then
          R.Topics[T].Partitions[P].Offset := LogStartOf(LName, LReq.PartitionIndex)
        else if LReq.Timestamp = -1 then
          R.Topics[T].Partitions[P].Offset := NextOffset(LName, LReq.PartitionIndex)
        else
        begin
          R.Topics[T].Partitions[P].Offset := -1;
          for I := 0 to FLogCount - 1 do
            if (FLog[I].Topic = LName) and (FLog[I].Partition = LReq.PartitionIndex) and
              (FLog[I].Rec.Timestamp >= LReq.Timestamp) then
            begin
              R.Topics[T].Partitions[P].Offset := FLog[I].Offset;
              R.Topics[T].Partitions[P].Timestamp := FLog[I].Rec.Timestamp;
              Break;
            end;
        end;
      end;
    end;
  finally
    FMonitor.Leave;
  end;
  SendResponse(AConn, ACorrelationId, KAFKA_API_KEY_LIST_OFFSETS, AVersion, R.ToBytes(AVersion));
end;

function TKafkaFakeBroker.FetchCount: Integer;
begin
  FMonitor.Enter;
  try
    Result := FFetchCount;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeBroker.ListOffsetsCount: Integer;
begin
  FMonitor.Enter;
  try
    Result := FListOffsetsCount;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeBroker.WaitParked(ACount: Integer; ATimeoutMs: Cardinal): Boolean;
var
  LDeadline, LNow: UInt64;
begin
  LDeadline := PcTickMs + ATimeoutMs;
  FMonitor.Enter;
  try
    while Length(FParked) < ACount do
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

function TKafkaFakeBroker.ProduceCount: Integer;
begin
  FMonitor.Enter;
  try
    Result := FProduceCount;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeBroker.LoggedCount: Integer;
begin
  FMonitor.Enter;
  try
    Result := FLogCount;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeBroker.Logged(const ATopic: string; APartition: Int32): TKafkaRecordArray;
var
  I, N: Integer;
begin
  Result := nil;
  FMonitor.Enter;
  try
    N := 0;
    for I := 0 to FLogCount - 1 do
      if (FLog[I].Topic = ATopic) and (FLog[I].Partition = APartition) then
      begin
        SetLength(Result, N + 1);
        Result[N] := FLog[I].Rec;
        Result[N].Offset := FLog[I].Offset;
        Inc(N);
      end;
  finally
    FMonitor.Leave;
  end;
end;

procedure TKafkaFakeBroker.LoggedAt(AIndex: Integer; out ATopic: string;
  out APartition: Int32; out AOffset: Int64; out AValue: TBytes);
begin
  FMonitor.Enter;
  try
    ATopic := FLog[AIndex].Topic;
    APartition := FLog[AIndex].Partition;
    AOffset := FLog[AIndex].Offset;
    AValue := FLog[AIndex].Rec.Value;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeBroker.LoggedCompression(AIndex: Integer): Integer;
begin
  FMonitor.Enter;
  try
    Result := FLog[AIndex].Compression;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeBroker.WaitProduceCount(ACount: Integer; ATimeoutMs: Cardinal): Boolean;
var
  LDeadline, LNow: UInt64;
begin
  LDeadline := PcTickMs + ATimeoutMs;
  FMonitor.Enter;
  try
    while FProduceCount < ACount do
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

procedure TKafkaFakeBroker.ReleaseHeldProduce;
var
  LHeld: THeldProduceArray;
  I: Integer;
begin
  FMonitor.Enter;
  try
    LHeld := Copy(FHeldProduce, 0, Length(FHeldProduce));
    FHeldProduce := nil;
    HoldProduce := False;
  finally
    FMonitor.Leave;
  end;
  for I := 0 to High(LHeld) do
    HandleProduce(LHeld[I].Connection, LHeld[I].CorrelationId, LHeld[I].Version,
      LHeld[I].Request);
end;

procedure TKafkaFakeBroker.SendResponse(AConn: TKafkaFakeConnection; ACorrelationId: Int32;
  AApiKey, AVersion: Int16; const ABody: TBytes);
var
  LHeader: TKafkaResponseHeader;
  LFlexible: Boolean;
begin
  LHeader := Default(TKafkaResponseHeader);
  LHeader.CorrelationId := ACorrelationId;
  LFlexible := (AVersion >= 0) and KafkaApiIsFlexible(AApiKey, AVersion);
  AConn.SendRaw(KafkaEncodeResponseFrame(LHeader, KafkaResponseHeaderVersion(AApiKey, LFlexible),
    ABody));
end;

function TKafkaFakeBroker.ApiVersionsBody(AVersion: Int16): TBytes;
var
  R: TKafkaApiVersionsResponse;
  N, G: Integer;
begin
  R.Init;
  N := 1;
  if OfferMetadata then
    N := 2;
  if OfferRecords then
    Inc(N, 3);
  G := 0;
  if OfferGroups then
  begin
    G := 3;
    if OfferConsumerGroupHeartbeat then
      G := 4;
  end;
  // SaslHandshake and SaslAuthenticate: a broker lists them on every
  // listener, SASL or not.
  SetLength(R.ApiKeys, N + G + 2);
  // The group APIs after the SASL pair.
  if OfferGroups then
  begin
    R.ApiKeys[N + 2].Init;
    R.ApiKeys[N + 2].ApiKey := KAFKA_API_KEY_FIND_COORDINATOR;
    R.ApiKeys[N + 2].MinVersion := 0;
    R.ApiKeys[N + 2].MaxVersion := FindCoordinatorMax;
    R.ApiKeys[N + 3].Init;
    R.ApiKeys[N + 3].ApiKey := KAFKA_API_KEY_OFFSET_COMMIT;
    R.ApiKeys[N + 3].MinVersion := 2;
    R.ApiKeys[N + 3].MaxVersion := 10;
    R.ApiKeys[N + 4].Init;
    R.ApiKeys[N + 4].ApiKey := KAFKA_API_KEY_OFFSET_FETCH;
    R.ApiKeys[N + 4].MinVersion := 8;
    R.ApiKeys[N + 4].MaxVersion := 10;
    if OfferConsumerGroupHeartbeat then
    begin
      R.ApiKeys[N + 5].Init;
      R.ApiKeys[N + 5].ApiKey := KAFKA_API_KEY_CONSUMER_GROUP_HEARTBEAT;
      R.ApiKeys[N + 5].MinVersion := 0;
      R.ApiKeys[N + 5].MaxVersion := 1;
    end;
  end;
  R.ApiKeys[N].Init;
  R.ApiKeys[N].ApiKey := KAFKA_API_KEY_SASL_HANDSHAKE;
  R.ApiKeys[N].MinVersion := 0;
  R.ApiKeys[N].MaxVersion := 1;
  R.ApiKeys[N + 1].Init;
  R.ApiKeys[N + 1].ApiKey := KAFKA_API_KEY_SASL_AUTHENTICATE;
  R.ApiKeys[N + 1].MinVersion := 0;
  R.ApiKeys[N + 1].MaxVersion := 2;
  R.ApiKeys[0].Init;
  R.ApiKeys[0].ApiKey := KAFKA_API_KEY_API_VERSIONS;
  R.ApiKeys[0].MinVersion := ApiVersionsMin;
  R.ApiKeys[0].MaxVersion := ApiVersionsMax;
  if OfferMetadata then
  begin
    R.ApiKeys[1].Init;
    R.ApiKeys[1].ApiKey := KAFKA_API_KEY_METADATA;
    R.ApiKeys[1].MinVersion := MetadataMin;
    R.ApiKeys[1].MaxVersion := MetadataMax;
  end;
  if OfferRecords then
  begin
    R.ApiKeys[N - 3].Init;
    R.ApiKeys[N - 3].ApiKey := KAFKA_API_KEY_PRODUCE;
    R.ApiKeys[N - 3].MinVersion := 3;
    R.ApiKeys[N - 3].MaxVersion := 13;
    R.ApiKeys[N - 2].Init;
    R.ApiKeys[N - 2].ApiKey := KAFKA_API_KEY_FETCH;
    R.ApiKeys[N - 2].MinVersion := 4;
    R.ApiKeys[N - 2].MaxVersion := 18;
    R.ApiKeys[N - 1].Init;
    R.ApiKeys[N - 1].ApiKey := KAFKA_API_KEY_LIST_OFFSETS;
    R.ApiKeys[N - 1].MinVersion := 1;
    R.ApiKeys[N - 1].MaxVersion := 11;
  end;
  if AVersion < 0 then
  begin
    R.ErrorCode := KAFKA_ERROR_UNSUPPORTED_VERSION;
    AVersion := 0;
  end;
  Result := R.ToBytes(AVersion);
end;

function TKafkaFakeBroker.ClusterViewBody(AVersion: Int16;
  const ARequest: TKafkaMetadataRequest): TBytes;
var
  R: TKafkaMetadataResponse;
  I, K, N: Integer;
  LFound: Boolean;
  LNotReady: Boolean;
begin
  // Called under FMonitor.
  R := ClusterView;
  LNotReady := (NotReadyAnswers > 0) and (Length(ARequest.Topics) > 0);
  if LNotReady then
    Dec(NotReadyAnswers);
  SetLength(R.Topics, Length(ARequest.Topics));
  for I := 0 to High(ARequest.Topics) do
  begin
    LFound := False;
    for K := 0 to High(ClusterView.Topics) do
      if ClusterView.Topics[K].Name = ARequest.Topics[I].Name then
      begin
        R.Topics[I] := ClusterView.Topics[K];
        LFound := True;
      end;
    if not LFound then
    begin
      R.Topics[I].Init;
      R.Topics[I].Name := ARequest.Topics[I].Name;
      R.Topics[I].ErrorCode := KAFKA_ERROR_UNKNOWN_TOPIC_OR_PARTITION;
    end;
    if LNotReady then
    begin
      R.Topics[I].ErrorCode := KAFKA_ERROR_LEADER_NOT_AVAILABLE;
      R.Topics[I].Partitions := nil;
    end;
  end;
  N := Length(R.Topics);
  if N = 0 then
    R.Topics := nil;
  Result := R.ToBytes(AVersion);
end;

function TKafkaFakeBroker.MetadataCount: Integer;
begin
  FMonitor.Enter;
  try
    Result := FMetadataCount;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeBroker.MetadataBody(AVersion: Int16; const ATopic: string): TBytes;
var
  R: TKafkaMetadataResponse;
begin
  R.Init;
  SetLength(R.Brokers, 1);
  R.Brokers[0].Init;
  R.Brokers[0].NodeId := 1;
  R.Brokers[0].Host := '127.0.0.1';
  R.Brokers[0].Port := Port;
  SetLength(R.Topics, 1);
  R.Topics[0].Init;
  R.Topics[0].Name := ATopic;
  Result := R.ToBytes(AVersion);
end;

function TKafkaFakeBroker.WaitHeld(ACount: Integer; ATimeoutMs: Cardinal): Boolean;
var
  LDeadline: UInt64;
  LNow: UInt64;
begin
  LDeadline := PcTickMs + ATimeoutMs;
  FMonitor.Enter;
  try
    while Length(FHeld) < ACount do
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

procedure TKafkaFakeBroker.ReleaseHeld;
var
  LHeld: THeldArray;
  I: Integer;
begin
  FMonitor.Enter;
  try
    LHeld := Copy(FHeld, 0, Length(FHeld));
    FHeld := nil;
    HoldMetadata := False;
  finally
    FMonitor.Leave;
  end;
  for I := High(LHeld) downto 0 do
    SendResponse(LHeld[I].Connection, LHeld[I].CorrelationId, KAFKA_API_KEY_METADATA,
      LHeld[I].Version,
      MetadataBody(LHeld[I].Version, LHeld[I].Topic));
end;

procedure TKafkaFakeBroker.DropClient;
var
  I: Integer;
begin
  FMonitor.Enter;
  try
    for I := 0 to FConnections.Count - 1 do
      FConnections[I].Abort;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaFakeBroker.RequestCount: Integer;
begin
  FMonitor.Enter;
  Result := FRequestCount;
  FMonitor.Leave;
end;

function TKafkaFakeBroker.LastClientId: string;
begin
  FMonitor.Enter;
  Result := FLastClientId;
  FMonitor.Leave;
end;

function TKafkaFakeBroker.LastHeaderVersion: Int16;
begin
  FMonitor.Enter;
  Result := FLastHeaderVersion;
  FMonitor.Leave;
end;

end.
