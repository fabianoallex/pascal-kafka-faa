unit Kafka.TestFakeBroker;

{ An in-process broker for Kafka.ConnectionTests: it listens on 127.0.0.1,
  port 0, accepts one connection at a time, and answers ApiVersions and
  Metadata with the library's own codecs, so the connection's behaviour can be
  tested without Docker and deterministically.

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
  - answer Produce (OfferProduce): the records of each partition are
    decoded (gzip too) and appended to that partition's log, offsets from 0,
    and the answer gives each batch's base offset. With acks=0 nothing is
    answered. A topic is named (v3-12) or given by id (v13, resolved through
    ClusterView). The next ProduceErrors requests are answered with
    ProduceErrorCode for every partition, appending nothing; the next
    DropProduce requests get no answer at all (and append nothing);
    HoldProduce keeps requests unanswered and unappended until
    ReleaseHeldProduce, which handles them in the order they came.

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
  Kafka.Protocol.RecordBatch;

type
  TKafkaFakeBroker = class
  private type
    THeld = record
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
      CorrelationId: Int32;
      Version: Int16;
      Request: TKafkaProduceRequest;
    end;
    THeldProduceArray = array of THeldProduce;

    TServeThread = class(TThread)
    private
      FOwner: TKafkaFakeBroker;
    protected
      procedure Execute; override;
    public
      constructor Create(AOwner: TKafkaFakeBroker);
    end;
  private
    FListener: TPcListener;
    FThread: TServeThread;
    FClient: TPcEndpoint;
    FWriteLock: TCriticalSection;
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
    procedure Serve;
    procedure HandleProduce(ACorrelationId: Int32; AVersion: Int16;
      const ARequest: TKafkaProduceRequest);
    function TopicNameOf(AVersion: Int16; const ATopic: TKafkaProduceRequestTopicProduceData): string;
    function NextOffset(const ATopic: string; APartition: Int32): Int64;
    procedure ServeClient(AClient: TPcEndpoint);
    procedure HandleRequest(const AContent: TBytes);
    procedure SendResponse(ACorrelationId: Int32; AApiKey, AVersion: Int16; const ABody: TBytes);
    procedure SendRaw(const AFrame: TBytes);
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
    OfferProduce: Boolean;
    ProduceErrors: Integer;
    ProduceErrorCode: Int16;
    DropProduce: Integer;
    HoldProduce: Boolean;

    constructor Create;
    constructor CreateTls;
    destructor Destroy; override;
    function Port: Word;
    /// Waits up to ATimeoutMs until ACount Metadata requests are held.
    function WaitHeld(ACount: Integer; ATimeoutMs: Cardinal): Boolean;
    /// Answers every held request, last received first.
    procedure ReleaseHeld;
    /// Aborts the current client connection (the broker "goes away").
    procedure DropClient;
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
    ABrokers[I].OfferProduce := True;
  end;
end;

{ TKafkaFakeBroker.TServeThread }

constructor TKafkaFakeBroker.TServeThread.Create(AOwner: TKafkaFakeBroker);
begin
  inherited Create(True);
  FOwner := AOwner;
  FreeOnTerminate := False;
end;

procedure TKafkaFakeBroker.TServeThread.Execute;
begin
  FOwner.Serve;
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
  FWriteLock := TCriticalSection.Create;
  FMonitor := TPcMonitor.Create;
  if FListener = nil then
    FListener := PcTcpListen('127.0.0.1', 0, Default(TPcTcpOptions));
  FThread := TServeThread.Create(Self);
  FThread.Start;
end;

destructor TKafkaFakeBroker.Destroy;
begin
  FListener.Close;
  DropClient;
  FThread.WaitFor;
  FThread.Free;
  FListener.Free;
  FMonitor.Free;
  FWriteLock.Free;
  inherited;
end;

function TKafkaFakeBroker.Port: Word;
begin
  Result := FListener.Port;
end;

procedure TKafkaFakeBroker.Serve;
var
  LClient: TPcEndpoint;
begin
  try
    while True do
    begin
      LClient := FListener.Accept;
      if LClient = nil then
        Exit; // listener closed
      FWriteLock.Enter;
      FClient := LClient;
      FWriteLock.Leave;
      try
        ServeClient(LClient);
      finally
        FWriteLock.Enter;
        FClient := nil;
        FWriteLock.Leave;
        LClient.Free;
      end;
    end;
  except
    // A test broker never takes the test runner down.
  end;
end;

procedure TKafkaFakeBroker.ServeClient(AClient: TPcEndpoint);
var
  LSize: array[0..3] of Byte;
  N: Int32;
  LContent: TBytes;
begin
  FillChar(LSize, SizeOf(LSize), 0);
  try
    try
      AClient.Handshake; // server-side TLS; nothing on plain TCP
    except
      on EPcTransport do
        Exit; // a client that doesn't trust us, for instance
    end;
    while True do
    begin
      KafkaReadExactly(AClient, LSize[0], 4, 'fake broker');
      N := Int32((Cardinal(LSize[0]) shl 24) or (Cardinal(LSize[1]) shl 16) or
        (Cardinal(LSize[2]) shl 8) or Cardinal(LSize[3]));
      if (N < 8) or (N > 1024 * 1024) then
        Exit; // not Kafka (a TLS ClientHello, for instance): hang up
      LContent := nil;
      SetLength(LContent, N);
      KafkaReadExactly(AClient, LContent[0], N, 'fake broker');
      HandleRequest(LContent);
    end;
  except
    on EKafka do ; // the client went away, or DropClient
  end;
end;

procedure TKafkaFakeBroker.HandleRequest(const AContent: TBytes);
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
begin
  KafkaPeekRequestApi(AContent, LKey, LVersion);
  FMonitor.Enter;
  Inc(FRequestCount);
  FMonitor.Leave;
  if LKey = KAFKA_API_KEY_API_VERSIONS then
  begin
    LReader := TKafkaReader.Create(AContent);
    try
      if LVersion > ApiVersionsMax then
      begin
        // KIP-511: an unknown version gets the error in v0, read with a v1 header.
        LHeader := KafkaReadRequestHeader(LReader, 1);
        SendResponse(LHeader.CorrelationId, LKey, 0, ApiVersionsBody(-1));
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
    SendResponse(LHeader.CorrelationId, LKey, LVersion, ApiVersionsBody(LVersion));
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
        FHeldProduce[N].CorrelationId := LHeader.CorrelationId;
        FHeldProduce[N].Version := LVersion;
        FHeldProduce[N].Request := LProduce;
        Exit;
      end;
    finally
      FMonitor.Leave;
    end;
    HandleProduce(LHeader.CorrelationId, LVersion, LProduce);
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
      SendResponse(LHeader.CorrelationId, LKey, LVersion, LBig);
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
    SendRaw(LBig);
    Exit;
  end;
  SendResponse(LHeader.CorrelationId, LKey, LVersion, MetadataBody(LVersion, LTopic));
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
  // Called under FMonitor.
  Result := 0;
  for I := 0 to FLogCount - 1 do
    if (FLog[I].Topic = ATopic) and (FLog[I].Partition = APartition) then
      Result := FLog[I].Offset + 1;
end;

procedure TKafkaFakeBroker.HandleProduce(ACorrelationId: Int32; AVersion: Int16;
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
    SendResponse(ACorrelationId, KAFKA_API_KEY_PRODUCE, AVersion, R.ToBytes(AVersion));
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
    HandleProduce(LHeld[I].CorrelationId, LHeld[I].Version, LHeld[I].Request);
end;

procedure TKafkaFakeBroker.SendResponse(ACorrelationId: Int32; AApiKey, AVersion: Int16;
  const ABody: TBytes);
var
  LHeader: TKafkaResponseHeader;
  LFlexible: Boolean;
begin
  LHeader := Default(TKafkaResponseHeader);
  LHeader.CorrelationId := ACorrelationId;
  LFlexible := (AVersion >= 0) and KafkaApiIsFlexible(AApiKey, AVersion);
  SendRaw(KafkaEncodeResponseFrame(LHeader, KafkaResponseHeaderVersion(AApiKey, LFlexible), ABody));
end;

procedure TKafkaFakeBroker.SendRaw(const AFrame: TBytes);
begin
  FWriteLock.Enter;
  try
    if FClient <> nil then
      KafkaWriteExactly(FClient, AFrame[0], Length(AFrame), 'fake broker');
  finally
    FWriteLock.Leave;
  end;
end;

function TKafkaFakeBroker.ApiVersionsBody(AVersion: Int16): TBytes;
var
  R: TKafkaApiVersionsResponse;
  N: Integer;
begin
  R.Init;
  N := 1;
  if OfferMetadata then
    N := 2;
  if OfferProduce then
    Inc(N);
  SetLength(R.ApiKeys, N);
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
  if OfferProduce then
  begin
    R.ApiKeys[N - 1].Init;
    R.ApiKeys[N - 1].ApiKey := KAFKA_API_KEY_PRODUCE;
    R.ApiKeys[N - 1].MinVersion := 3;
    R.ApiKeys[N - 1].MaxVersion := 13;
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
    SendResponse(LHeld[I].CorrelationId, KAFKA_API_KEY_METADATA, LHeld[I].Version,
      MetadataBody(LHeld[I].Version, LHeld[I].Topic));
end;

procedure TKafkaFakeBroker.DropClient;
begin
  FWriteLock.Enter;
  try
    if FClient <> nil then
      FClient.CloseAbort;
  finally
    FWriteLock.Leave;
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
