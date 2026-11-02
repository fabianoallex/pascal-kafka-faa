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
  - drop the client's connection (DropClient).

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
  Kafka.Messages.Metadata;

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
    procedure Serve;
    procedure ServeClient(AClient: TPcEndpoint);
    procedure HandleRequest(const AContent: TBytes);
    procedure SendResponse(ACorrelationId: Int32; AApiKey, AVersion: Int16; const ABody: TBytes);
    procedure SendRaw(const AFrame: TBytes);
    function ApiVersionsBody(AVersion: Int16): TBytes;
    function MetadataBody(AVersion: Int16; const ATopic: string): TBytes;
  public
    ApiVersionsMin, ApiVersionsMax: Int16;
    MetadataMin, MetadataMax: Int16;
    OfferMetadata: Boolean;
    HoldMetadata: Boolean;
    DropNextMetadata: Integer;
    OversizedMetadata: Boolean;

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
    function LastClientId: string;
    function LastHeaderVersion: Int16;
  end;

implementation

uses
  PascalCommon.Threading,
  PascalCommon.Transport.Tcp,
  PascalCommon.Transport.Tls,
  Kafka.Errors,
  Kafka.Transport,
  Kafka.TestHelpers;

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
    FLastClientId := LHeader.ClientId;
    FLastHeaderVersion := LHeaderVersion;
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
  if AVersion < 0 then
  begin
    R.ErrorCode := KAFKA_ERROR_UNSUPPORTED_VERSION;
    AVersion := 0;
  end;
  Result := R.ToBytes(AVersion);
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
