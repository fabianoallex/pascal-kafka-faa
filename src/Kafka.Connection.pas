unit Kafka.Connection;

{$I kafka.inc}

{ One connection to one broker: TCP or TLS from pascal-common-faa, a reader
  thread, requests matched to responses by correlation id, and the API
  versions negotiated with ApiVersions when the connection opens.

  Several requests in flight. Send writes a request and returns a future at
  once; the reader thread hands each response to the future waiting for its
  correlation id, in whatever order the broker answers (a broker answers one
  connection's requests in order, but nothing here depends on it). Request is
  Send plus a wait bounded by RequestTimeoutMs. Writers are serialized by a
  lock, so any number of threads may send on one connection; the frame of a
  request goes out in one write.

  Timeouts. The socket has no read deadline: the reader waits indefinitely
  while the broker has nothing to say. A request's deadline is the wait on
  its future. When it passes, Request raises EKafkaTimeout and forgets the
  correlation id; a response that arrives later is discarded. The connection
  stays open, unlike Java's client, which closes it on a request timeout:
  that is the cluster layer's decision (Kafka.Cluster, later in K1), which
  knows whether other requests share the connection.

  Failure. When the reader can't go on (the broker closed the connection, a
  transport error, a frame larger than MaxResponseBytes, a malformed size),
  the connection is BROKEN: every request in flight fails with the reason,
  and Send raises it from then on. A broken connection isn't reopened here;
  the owner closes it and opens a new one (reconnect.backoff is the
  cluster's).

  Waking someone else. Send may be given a signal (IKafkaSignal) that the
  future fires once it is done, whatever the outcome: the producer's sender
  waits on one signal for all its requests in flight instead of on each
  future. Signal runs on the reader thread (or on the thread that fails or
  abandons the request), so it must be quick and must not raise; the
  library's own signals only set an event (TKafkaEventSignal).

  Requests with no answer. A Produce with acks=0 gets none from the broker:
  SendOneWay writes it with a correlation id of its own but registers no
  future, so nothing waits and nothing is left in the table.

  ApiVersions (KIP-511, KIP-896). Open sends ApiVersions at the highest
  version this library has. A broker that doesn't know it answers
  UNSUPPORTED_VERSION in the v0 format, with the versions of ApiVersions it
  does know; Open then retries once at the highest version both know. With
  the broker's table in hand, NegotiatedVersion(api) is the highest version in
  both the broker's range and Kafka.Messages.Apis's. An API the broker
  doesn't offer, or whose ranges don't meet, has no negotiated version, and
  using it raises EKafkaUnsupportedVersion.

  Shutdown. Close aborts the socket (which unblocks the reader and any writer
  stuck on a full socket), joins the reader thread, fails what is still in
  flight, and frees the endpoint. It is idempotent, and the destructor calls
  it. It must not be called from a future's waiter on the reader thread;
  nothing in the library calls user code on that thread.

  SASL. With a SaslMechanism, Open authenticates right after ApiVersions,
  before anything else is sent, as Java's client does: SaslHandshake v1
  names the mechanism, then SaslAuthenticate carries the mechanism's
  messages (Kafka.Sasl) until it is complete. The same over plaintext
  (SASL_PLAINTEXT) and TLS (SASL_SSL). SaslHandshake v0, where the tokens
  went unframed on the socket, isn't spoken: Kafka 4 brokers still offer it,
  but every broker since 1.0 has v1. A failure raises EKafkaAuthentication
  with the broker's error code and message (the user name, never the
  password); the broker closes the connection after a failed exchange, and
  Open closes it on its side too.

  Re-authentication (KIP-368). A broker with connections.max.reauth.ms
  answers SaslAuthenticate (v1+) with a session lifetime, and closes the
  connection when a request arrives after it ran out. Java's client
  re-authenticates on the same socket at a random point between 85% and 95%
  of the lifetime. This connection doesn't: it notes that point
  (ReauthenticationDue), and Kafka.Cluster opens a new connection to that
  broker the next time one is asked for, while requests already sent on the
  old one finish there. That costs a TCP (and TLS) handshake per session
  instead of a SASL exchange, and it needs no pause of the writers while
  the exchange runs; with sessions measured in minutes or hours, the
  difference doesn't show. Someone holding the connection itself past its
  lifetime (not through the cluster) gets EKafkaConnection when the broker
  hangs up.

  Exceptions: EKafkaConnection/EKafkaTls for the socket (Kafka.Transport is
  the boundary: no EPcTransport* leaves), EKafkaTimeout, EKafkaProtocol,
  EKafkaUnsupportedVersion, EKafkaBrokerError, EKafkaAuthentication
  (Kafka.Types). }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  PascalCommon.Transport,
  Kafka.Types,
  Kafka.Sasl,
  Kafka.Messages.ApiVersions,
  Kafka.Messages.Metadata,
  Kafka.Messages.Produce,
  Kafka.Messages.Fetch,
  Kafka.Messages.ListOffsets;

const
  /// TKafkaConnectionParams.RequestTimeoutMs = 0: Java's request.timeout.ms.
  KAFKA_DEFAULT_REQUEST_TIMEOUT_MS = 30000;
  /// TKafkaConnectionParams.MaxResponseBytes = 0. A response larger than this
  /// breaks the connection instead of allocating it: a corrupt or hostile
  /// size prefix would otherwise ask for up to 2 GB.
  KAFKA_DEFAULT_MAX_RESPONSE_BYTES = 100 * 1024 * 1024;

type
  { How to reach one broker. A zeroed record plus Host and Port is a plain
    TCP connection with the defaults (KafkaConnectionParams). }
  TKafkaConnectionParams = record
    Host: string;
    Port: Word;
    /// The client.id of every request header ('' is sent as an empty string,
    /// which brokers accept).
    ClientId: string;
    /// The whole connect, every address of the name included. 0 = 10 s.
    ConnectTimeoutMs: Cardinal;
    /// Request's wait for a response. 0 = KAFKA_DEFAULT_REQUEST_TIMEOUT_MS.
    RequestTimeoutMs: Cardinal;
    /// 0 = KAFKA_DEFAULT_MAX_RESPONSE_BYTES.
    MaxResponseBytes: Integer;

    /// TLS (security.protocol SSL). The fields below follow pascal-common-
    /// faa's TPcTlsOptions (its transport-design.md, T7): TlsCaFile is PEM on
    /// both backends; a client certificate (mTLS) is a PEM pair (TlsCertFile
    /// + TlsKeyFile) on OpenSSL and a PFX (TlsCertFile + TlsCertPassword) on
    /// SChannel.
    UseTls: Boolean;
    /// Trust this CA (PEM) for the broker instead of the system store.
    TlsCaFile: string;
    TlsCertFile: string;
    TlsKeyFile: string;
    TlsCertPassword: string;
    /// Turns off the broker's chain and hostname validation (Kafka's
    /// ssl.endpoint.identification.algorithm=""). For a lab only.
    TlsSkipServerVerification: Boolean;
    /// The name sent in SNI and checked against the certificate; '' = Host.
    TlsServerName: string;
    /// tbDefault: SChannel on Windows, OpenSSL elsewhere.
    TlsBackend: TPcTlsBackend;
    /// 0 = the common's default (15 s).
    TlsHandshakeTimeoutMs: Cardinal;

    /// SASL (security.protocol SASL_PLAINTEXT, or SASL_SSL with UseTls).
    /// ksmNone: no authentication. The user name and password are sent as
    /// their UTF-8 bytes (Kafka.Sasl).
    SaslMechanism: TKafkaSaslMechanism;
    SaslUsername: string;
    SaslPassword: string;
  end;

  { Fired by a response future once it is done (the unit's header). }
  IKafkaSignal = interface
    ['{3C6E1F0B-8A47-4D2E-9B15-7F0A2C4E6D93}']
    procedure Signal;
  end;

  { An IKafkaSignal over an auto-reset event: Signal sets it, WaitFor
    returns once it was set since the last WaitFor (or on the timeout). }
  TKafkaEventSignal = class(TInterfacedObject, IKafkaSignal)
  private
    FEvent: TEvent;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Signal;
    /// True when the signal was set.
    function WaitFor(ATimeoutMs: Cardinal): Boolean;
  end;

  { The answer to one request, filled by the reader thread. }
  IKafkaResponseFuture = interface
    ['{6B0C1A57-3E1B-4B7E-9F55-2C1E4B2B9A61}']
    function CorrelationId: Int32;
    function ApiKey: Int16;
    function ApiVersion: Int16;
    /// Waits up to ATimeoutMs for the response or a failure; True when one of
    /// them arrived. PC_WAIT_INFINITE-style $FFFFFFFF waits forever.
    function Wait(ATimeoutMs: Cardinal): Boolean;
    function IsDone: Boolean;
    /// The response's body (after its header). Raises the failure if the
    /// request failed, and EKafka if it isn't done yet.
    function Body: TBytes;
  end;

  TKafkaConnection = class
  private type
    TPending = class(TInterfacedObject, IKafkaResponseFuture)
    private
      FCorrelationId: Int32;
      FApiKey: Int16;
      FApiVersion: Int16;
      FResponseHeaderVersion: Int16;
      FEvent: TEvent;
      FDone: Boolean;
      FBody: TBytes;
      FErrorClass: EKafkaClass;
      FErrorMessage: string;
      FNotify: IKafkaSignal;
    public
      constructor Create(AApiKey, AApiVersion, AResponseHeaderVersion: Int16;
        const ANotify: IKafkaSignal);
      destructor Destroy; override;
      procedure Complete(const ABody: TBytes);
      procedure Fail(AClass: EKafkaClass; const AMessage: string);
      function CorrelationId: Int32;
      function ApiKey: Int16;
      function ApiVersion: Int16;
      function Wait(ATimeoutMs: Cardinal): Boolean;
      function IsDone: Boolean;
      function Body: TBytes;
      /// The correlation table's reference (_AddRef/_Release are protected
      /// in TInterfacedObject).
      procedure TableHold;
      procedure TableRelease;
    end;

    TReaderThread = class(TThread)
    private
      FOwner: TKafkaConnection;
    protected
      procedure Execute; override;
    public
      constructor Create(AOwner: TKafkaConnection);
      function Stopping: Boolean;
    end;

    TVersionRange = record
      Known: Boolean;
      Min, Max: Int16;
    end;
  private
    FParams: TKafkaConnectionParams;
    FAddress: string;
    FEndpoint: TPcEndpoint;
    FReader: TReaderThread;
    FWriteLock: TCriticalSection;
    FPendingLock: TCriticalSection;
    FPending: TDictionary<Int32, TPending>;
    FNextCorrelationId: Int32;
    FOpened: Boolean;
    FOpen: Boolean;
    FBroken: Boolean;
    FBrokenReason: string;
    FBrokenClass: EKafkaClass;
    FApiVersions: TKafkaApiVersionsResponse;
    FApiVersionsVersion: Int16;
    FNegotiated: array of TVersionRange;  // indexed by API key
    FSessionLifetimeMs: Int64;
    FReauthenticateAt: UInt64;            // PcTickMs; 0 = never
    procedure ReaderLoop;
    procedure HandleFrame(const AContent: TBytes);
    procedure MarkBroken(AClass: EKafkaClass; const AReason: string);
    procedure FailAll(AClass: EKafkaClass; const AMessage: string);
    function TakePending(ACorrelationId: Int32): TPending;
    procedure Negotiate;
    procedure BuildNegotiatedTable;
    procedure Authenticate;
    function RequestTimeoutMs: Cardinal;
    function MaxResponseBytes: Integer;
  public
    constructor Create(const AParams: TKafkaConnectionParams);
    destructor Destroy; override;

    /// Connects, starts the reader and negotiates API versions. Raises on any
    /// failure, leaving the connection closed. Once only: a closed or broken
    /// connection isn't reopened; create another.
    procedure Open;
    /// See the unit's header. Idempotent.
    procedure Close;
    /// Open, and not broken.
    function IsOpen: Boolean;
    /// Why the connection broke ('' when it didn't).
    function BrokenReason: string;

    /// Writes a request (ABody: the encoded body of AApiKey at AApiVersion)
    /// and returns its future at once. Thread-safe.
    function Send(AApiKey, AApiVersion: Int16; const ABody: TBytes): IKafkaResponseFuture; overload;
    /// The same, firing ANotify once the future is done (nil: none).
    function Send(AApiKey, AApiVersion: Int16; const ABody: TBytes;
      const ANotify: IKafkaSignal): IKafkaResponseFuture; overload;
    /// Send, then wait up to RequestTimeoutMs; the response's body.
    function Request(AApiKey, AApiVersion: Int16; const ABody: TBytes): TBytes; overload;
    /// The same with a deadline of its own (Fetch adds its MaxWaitMs).
    function Request(AApiKey, AApiVersion: Int16; const ABody: TBytes;
      ATimeoutMs: Cardinal): TBytes; overload;
    /// Writes a request the broker won't answer (Produce with acks=0);
    /// returns its correlation id. Thread-safe.
    function SendOneWay(AApiKey, AApiVersion: Int16; const ABody: TBytes): Int32;
    /// Forgets a request (after its own timeout): its response, if it comes,
    /// is discarded, and its future fails with EKafkaTimeout.
    procedure Abandon(const AFuture: IKafkaResponseFuture);

    /// The highest version of AApiKey both sides know; False when there is
    /// none (the broker doesn't offer it, or the library has no codec).
    function TryNegotiatedVersion(AApiKey: Int16; out AVersion: Int16): Boolean;
    /// TryNegotiatedVersion, raising EKafkaUnsupportedVersion with the two
    /// ranges when there is none.
    function NegotiatedVersion(AApiKey: Int16): Int16;
    /// The broker's ApiVersions answer, as decoded at Open.
    function BrokerApiVersions: TKafkaApiVersionsResponse;
    /// The version of ApiVersions that answer was read in.
    property ApiVersionsVersion: Int16 read FApiVersionsVersion;

    /// The SASL session's lifetime the broker gave (KIP-368); 0 when it gave
    /// none (no connections.max.reauth.ms, or no SASL).
    property SessionLifetimeMs: Int64 read FSessionLifetimeMs;
    /// The SASL session is past 85-95% of its lifetime (the point is chosen
    /// at random, as Java's): the cluster replaces the connection (the
    /// unit's header). Always False without a lifetime.
    function ReauthenticationDue: Boolean;

    /// A Metadata request at the negotiated version. A top-level error
    /// (v13+) raises EKafkaBrokerError; errors per topic and partition are in
    /// the response, for the caller.
    function Metadata(const ARequest: TKafkaMetadataRequest): TKafkaMetadataResponse;
    /// A Produce request at the negotiated version. With Acks 0 the broker
    /// doesn't answer: the request is written (SendOneWay) and the result is
    /// an Init'ed response, with no topics. Errors per partition are in the
    /// response.
    function Produce(const ARequest: TKafkaProduceRequest): TKafkaProduceResponse;
    /// A ListOffsets request at the negotiated version; errors per partition
    /// are in the response.
    function ListOffsets(const ARequest: TKafkaListOffsetsRequest): TKafkaListOffsetsResponse;
    /// A Fetch request at the negotiated version, waited for up to
    /// RequestTimeoutMs plus its MaxWaitMs (the broker may hold it that
    /// long). A top-level error (v7+) raises EKafkaBrokerError; errors per
    /// partition are in the response, and the records are raw bytes
    /// (KafkaDecodeRecordBatches).
    function Fetch(const ARequest: TKafkaFetchRequest): TKafkaFetchResponse;

    /// Requests waiting for their response.
    function InFlightCount: Integer;
    property Params: TKafkaConnectionParams read FParams;
    /// 'host:port', for messages.
    property Address: string read FAddress;
  end;

/// Host and Port with every other field at its default (plain TCP).
function KafkaConnectionParams(const AHost: string; APort: Word): TKafkaConnectionParams;

implementation

uses
  PascalCommon.Transport.Tcp,
  PascalCommon.Transport.Tls,
  PascalCommon.Threading,
  PascalCommon.Random,
  Kafka.Errors,
  Kafka.Messages.SaslHandshake,
  Kafka.Messages.SaslAuthenticate,
  Kafka.Transport,
  Kafka.Protocol.Primitives,
  Kafka.Protocol.Envelope,
  Kafka.Messages.Apis;

function KafkaConnectionParams(const AHost: string; APort: Word): TKafkaConnectionParams;
begin
  Result := Default(TKafkaConnectionParams);
  Result.Host := AHost;
  Result.Port := APort;
end;

/// 0, 1, ... High(Int32), 0: never negative, and no overflow under $Q+
/// (Delphi's Debug configuration).
function NextCorrelationId(AId: Int32): Int32;
begin
  if AId >= High(Int32) then
    Result := 0
  else
    Result := AId + 1;
end;

{ TKafkaEventSignal }

constructor TKafkaEventSignal.Create;
begin
  inherited Create;
  FEvent := TEvent.Create(nil, False, False, '');
end;

destructor TKafkaEventSignal.Destroy;
begin
  FEvent.Free;
  inherited;
end;

procedure TKafkaEventSignal.Signal;
begin
  FEvent.SetEvent;
end;

function TKafkaEventSignal.WaitFor(ATimeoutMs: Cardinal): Boolean;
begin
  Result := FEvent.WaitFor(ATimeoutMs) = wrSignaled;
end;

{ TKafkaConnection.TPending }

constructor TKafkaConnection.TPending.Create(AApiKey, AApiVersion,
  AResponseHeaderVersion: Int16; const ANotify: IKafkaSignal);
begin
  inherited Create;
  FNotify := ANotify;
  FApiKey := AApiKey;
  FApiVersion := AApiVersion;
  FResponseHeaderVersion := AResponseHeaderVersion;
  FEvent := TEvent.Create(nil, True, False, '');
end;

destructor TKafkaConnection.TPending.Destroy;
begin
  FEvent.Free;
  inherited;
end;

procedure TKafkaConnection.TPending.Complete(const ABody: TBytes);
begin
  if FDone then
    Exit;
  FBody := ABody;
  FDone := True;
  FEvent.SetEvent;
  if FNotify <> nil then
    FNotify.Signal;
end;

procedure TKafkaConnection.TPending.Fail(AClass: EKafkaClass; const AMessage: string);
begin
  if FDone then
    Exit;
  FErrorClass := AClass;
  FErrorMessage := AMessage;
  FDone := True;
  FEvent.SetEvent;
  if FNotify <> nil then
    FNotify.Signal;
end;

function TKafkaConnection.TPending.CorrelationId: Int32;
begin
  Result := FCorrelationId;
end;

function TKafkaConnection.TPending.ApiKey: Int16;
begin
  Result := FApiKey;
end;

function TKafkaConnection.TPending.ApiVersion: Int16;
begin
  Result := FApiVersion;
end;

function TKafkaConnection.TPending.Wait(ATimeoutMs: Cardinal): Boolean;
begin
  Result := FEvent.WaitFor(ATimeoutMs) = wrSignaled;
end;

function TKafkaConnection.TPending.IsDone: Boolean;
begin
  Result := FEvent.WaitFor(0) = wrSignaled;
end;

procedure TKafkaConnection.TPending.TableHold;
begin
  _AddRef;
end;

procedure TKafkaConnection.TPending.TableRelease;
begin
  _Release;
end;

function TKafkaConnection.TPending.Body: TBytes;
begin
  if not IsDone then
    raise EKafka.CreateFmt('%s v%d (correlation %d) has no response yet',
      [KafkaApiName(FApiKey), FApiVersion, FCorrelationId]);
  if FErrorClass <> nil then
    raise FErrorClass.Create(FErrorMessage);
  Result := FBody;
end;

{ TKafkaConnection.TReaderThread }

constructor TKafkaConnection.TReaderThread.Create(AOwner: TKafkaConnection);
begin
  inherited Create(True);
  FOwner := AOwner;
  FreeOnTerminate := False;
end;

function TKafkaConnection.TReaderThread.Stopping: Boolean;
begin
  Result := Terminated;
end;

procedure TKafkaConnection.TReaderThread.Execute;
begin
  FOwner.ReaderLoop;
end;

{ TKafkaConnection }

constructor TKafkaConnection.Create(const AParams: TKafkaConnectionParams);
begin
  inherited Create;
  FParams := AParams;
  FAddress := PcFormatHostPort(AParams.Host, AParams.Port);
  FWriteLock := TCriticalSection.Create;
  FPendingLock := TCriticalSection.Create;
  FPending := TDictionary<Int32, TPending>.Create;
  FApiVersions.Init;
end;

destructor TKafkaConnection.Destroy;
begin
  Close;
  FPending.Free;
  FPendingLock.Free;
  FWriteLock.Free;
  inherited;
end;

function TKafkaConnection.RequestTimeoutMs: Cardinal;
begin
  Result := FParams.RequestTimeoutMs;
  if Result = 0 then
    Result := KAFKA_DEFAULT_REQUEST_TIMEOUT_MS;
end;

function TKafkaConnection.MaxResponseBytes: Integer;
begin
  Result := FParams.MaxResponseBytes;
  if Result <= 0 then
    Result := KAFKA_DEFAULT_MAX_RESPONSE_BYTES;
end;

procedure TKafkaConnection.Open;
var
  LTls: TPcTlsOptions;
  LServerName: string;
begin
  if FOpened then
    raise EKafka.CreateFmt('Connection to %s was already opened', [FAddress]);
  FOpened := True;
  try
    try
      if FParams.UseTls then
      begin
        LTls := Default(TPcTlsOptions);
        LTls.CaFile := FParams.TlsCaFile;
        LTls.CertFile := FParams.TlsCertFile;
        LTls.KeyFile := FParams.TlsKeyFile;
        LTls.CertPassword := FParams.TlsCertPassword;
        LTls.SkipServerVerification := FParams.TlsSkipServerVerification;
        LTls.HandshakeTimeoutMs := FParams.TlsHandshakeTimeoutMs;
        LTls.Backend := FParams.TlsBackend;
        LServerName := FParams.TlsServerName;
        if LServerName = '' then
          LServerName := FParams.Host;
        // PcTlsWrap owns the TCP endpoint, also when the handshake fails.
        FEndpoint := PcTlsWrap(PcTcpConnect(FParams.Host, FParams.Port,
          KafkaTcpOptions(FParams.ConnectTimeoutMs)), LServerName, LTls);
      end
      else
        FEndpoint := PcTcpConnect(FParams.Host, FParams.Port,
          KafkaTcpOptions(FParams.ConnectTimeoutMs));
    except
      on E: EPcTransport do
        KafkaRaiseTransportError(E, 'Connecting to ' + FAddress);
    end;
    FOpen := True;
    FReader := TReaderThread.Create(Self);
    FReader.Start;
    Negotiate;
    if FParams.SaslMechanism <> ksmNone then
      Authenticate;
  except
    Close;
    raise;
  end;
end;

procedure TKafkaConnection.Close;
begin
  // Terminate first, so that the reader, woken by the abort, reports a close
  // rather than the transport's error.
  if FReader <> nil then
    FReader.Terminate;
  if FEndpoint <> nil then
    FEndpoint.CloseAbort;
  if FReader <> nil then
  begin
    FReader.WaitFor;
    FreeAndNil(FReader);
  end;
  FailAll(EKafkaConnection, Format('Connection to %s closed', [FAddress]));
  FWriteLock.Enter;
  try
    FOpen := False;
    FreeAndNil(FEndpoint);
  finally
    FWriteLock.Leave;
  end;
end;

function TKafkaConnection.IsOpen: Boolean;
begin
  FPendingLock.Enter;
  try
    Result := FOpen and not FBroken;
  finally
    FPendingLock.Leave;
  end;
end;

function TKafkaConnection.BrokenReason: string;
begin
  FPendingLock.Enter;
  try
    Result := FBrokenReason;
  finally
    FPendingLock.Leave;
  end;
end;

function TKafkaConnection.InFlightCount: Integer;
begin
  FPendingLock.Enter;
  try
    Result := FPending.Count;
  finally
    FPendingLock.Leave;
  end;
end;

procedure TKafkaConnection.MarkBroken(AClass: EKafkaClass; const AReason: string);
begin
  FPendingLock.Enter;
  try
    if FBroken then
      Exit;
    FBroken := True;
    FBrokenReason := AReason;
    FBrokenClass := AClass;
  finally
    FPendingLock.Leave;
  end;
end;

procedure TKafkaConnection.FailAll(AClass: EKafkaClass; const AMessage: string);
var
  LTaken: array of TPending;
  LPending: TPending;
  I: Integer;
begin
  LTaken := nil;
  FPendingLock.Enter;
  try
    SetLength(LTaken, FPending.Count);
    I := 0;
    for LPending in FPending.Values do
    begin
      LTaken[I] := LPending;
      Inc(I);
    end;
    FPending.Clear;
  finally
    FPendingLock.Leave;
  end;
  for I := 0 to High(LTaken) do
  begin
    LTaken[I].Fail(AClass, AMessage);
    LTaken[I].TableRelease; // the table's reference
  end;
end;

function TKafkaConnection.TakePending(ACorrelationId: Int32): TPending;
begin
  FPendingLock.Enter;
  try
    if FPending.TryGetValue(ACorrelationId, Result) then
      FPending.Remove(ACorrelationId)
    else
      Result := nil;
  finally
    FPendingLock.Leave;
  end;
end;

function TKafkaConnection.Send(AApiKey, AApiVersion: Int16;
  const ABody: TBytes): IKafkaResponseFuture;
begin
  Result := Send(AApiKey, AApiVersion, ABody, nil);
end;

function TKafkaConnection.Send(AApiKey, AApiVersion: Int16; const ABody: TBytes;
  const ANotify: IKafkaSignal): IKafkaResponseFuture;
var
  LPending: TPending;
  LHeader: TKafkaRequestHeader;
  LFlexible: Boolean;
  LFrame: TBytes;
begin
  LFlexible := KafkaApiIsFlexible(AApiKey, AApiVersion);
  LPending := TPending.Create(AApiKey, AApiVersion,
    KafkaResponseHeaderVersion(AApiKey, LFlexible), ANotify);
  Result := LPending; // the caller's reference
  LHeader := Default(TKafkaRequestHeader);
  LHeader.ApiKey := AApiKey;
  LHeader.ApiVersion := AApiVersion;
  LHeader.ClientId := FParams.ClientId;
  FWriteLock.Enter;
  try
    FPendingLock.Enter;
    try
      if not FOpen then
        raise EKafkaConnection.CreateFmt('Connection to %s is not open', [FAddress]);
      if FBroken then
        raise FBrokenClass.Create(FBrokenReason);
      // Skip ids still in flight after a wrap-around (2^31 requests later).
      while FPending.ContainsKey(FNextCorrelationId) do
        FNextCorrelationId := NextCorrelationId(FNextCorrelationId);
      LPending.FCorrelationId := FNextCorrelationId;
      FNextCorrelationId := NextCorrelationId(FNextCorrelationId);
      // Registered before writing, so that a fast response finds it.
      FPending.Add(LPending.FCorrelationId, LPending);
      LPending.TableHold; // the table's reference
    finally
      FPendingLock.Leave;
    end;
    LHeader.CorrelationId := LPending.FCorrelationId;
    LFrame := KafkaEncodeRequestFrame(LHeader, KafkaRequestHeaderVersion(LFlexible), ABody);
    try
      KafkaWriteExactly(FEndpoint, LFrame[0], Length(LFrame), 'Writing to ' + FAddress);
    except
      on E: EKafka do
      begin
        // A write that failed may have sent part of the frame: the stream is
        // out of step, and the connection can't be used again.
        MarkBroken(EKafkaClass(E.ClassType), E.Message);
        FEndpoint.CloseAbort;
        if TakePending(LPending.FCorrelationId) <> nil then
          LPending.TableRelease;
        raise;
      end;
    end;
  finally
    FWriteLock.Leave;
  end;
end;

function TKafkaConnection.Request(AApiKey, AApiVersion: Int16; const ABody: TBytes): TBytes;
begin
  Result := Request(AApiKey, AApiVersion, ABody, RequestTimeoutMs);
end;

function TKafkaConnection.Request(AApiKey, AApiVersion: Int16; const ABody: TBytes;
  ATimeoutMs: Cardinal): TBytes;
var
  LFuture: IKafkaResponseFuture;
begin
  LFuture := Send(AApiKey, AApiVersion, ABody);
  if not LFuture.Wait(ATimeoutMs) then
  begin
    Abandon(LFuture);
    raise EKafkaTimeout.CreateFmt('%s v%d to %s: no response in %d ms',
      [KafkaApiName(AApiKey), AApiVersion, FAddress, ATimeoutMs]);
  end;
  Result := LFuture.Body;
end;

function TKafkaConnection.SendOneWay(AApiKey, AApiVersion: Int16; const ABody: TBytes): Int32;
var
  LHeader: TKafkaRequestHeader;
  LFlexible: Boolean;
  LFrame: TBytes;
begin
  LFlexible := KafkaApiIsFlexible(AApiKey, AApiVersion);
  LHeader := Default(TKafkaRequestHeader);
  LHeader.ApiKey := AApiKey;
  LHeader.ApiVersion := AApiVersion;
  LHeader.ClientId := FParams.ClientId;
  FWriteLock.Enter;
  try
    FPendingLock.Enter;
    try
      if not FOpen then
        raise EKafkaConnection.CreateFmt('Connection to %s is not open', [FAddress]);
      if FBroken then
        raise FBrokenClass.Create(FBrokenReason);
      while FPending.ContainsKey(FNextCorrelationId) do
        FNextCorrelationId := NextCorrelationId(FNextCorrelationId);
      Result := FNextCorrelationId;
      FNextCorrelationId := NextCorrelationId(FNextCorrelationId);
    finally
      FPendingLock.Leave;
    end;
    LHeader.CorrelationId := Result;
    LFrame := KafkaEncodeRequestFrame(LHeader, KafkaRequestHeaderVersion(LFlexible), ABody);
    try
      KafkaWriteExactly(FEndpoint, LFrame[0], Length(LFrame), 'Writing to ' + FAddress);
    except
      on E: EKafka do
      begin
        // As in Send: a partial frame leaves the stream out of step.
        MarkBroken(EKafkaClass(E.ClassType), E.Message);
        FEndpoint.CloseAbort;
        raise;
      end;
    end;
  finally
    FWriteLock.Leave;
  end;
end;

procedure TKafkaConnection.Abandon(const AFuture: IKafkaResponseFuture);
var
  LPending: TPending;
begin
  LPending := TakePending(AFuture.CorrelationId);
  if LPending = nil then
    Exit; // answered or failed meanwhile
  LPending.Fail(EKafkaTimeout, Format('%s v%d to %s (correlation %d) was abandoned',
    [KafkaApiName(LPending.FApiKey), LPending.FApiVersion, FAddress, LPending.FCorrelationId]));
  LPending.TableRelease;
end;

procedure TKafkaConnection.ReaderLoop;
var
  LSizeBytes: array[0..3] of Byte;
  LSize: Int32;
  LContent: TBytes;
  LReason: string;
  LClass: EKafkaClass;
begin
  LClass := EKafkaConnection;
  try
    while not FReader.Stopping do
    begin
      KafkaReadExactly(FEndpoint, LSizeBytes[0], 4, 'Reading from ' + FAddress);
      LSize := Int32((Cardinal(LSizeBytes[0]) shl 24) or (Cardinal(LSizeBytes[1]) shl 16) or
        (Cardinal(LSizeBytes[2]) shl 8) or Cardinal(LSizeBytes[3]));
      if (LSize < 4) or (LSize > MaxResponseBytes) then
      begin
        LClass := EKafkaProtocol;
        // A TLS record starts with its type (20..23) and major version 3:
        // a plaintext client on a TLS listener reads the broker's alert as
        // a size of some 350 MB.
        if (LSizeBytes[0] >= 20) and (LSizeBytes[0] <= 23) and (LSizeBytes[1] = 3) then
          raise EKafkaProtocol.CreateFmt('Response of %d bytes from %s (the limit is %d); ' +
            'the bytes look like a TLS record: is this a TLS listener (UseTls)?',
            [LSize, FAddress, MaxResponseBytes]);
        raise EKafkaProtocol.CreateFmt('Response of %d bytes from %s (the limit is %d)',
          [LSize, FAddress, MaxResponseBytes]);
      end;
      LContent := nil;
      SetLength(LContent, LSize);
      KafkaReadExactly(FEndpoint, LContent[0], LSize, 'Reading from ' + FAddress);
      HandleFrame(LContent);
    end;
  except
    on E: Exception do
    begin
      if E is EKafka then
        LClass := EKafkaClass(E.ClassType);
      LReason := E.Message;
      if FReader.Stopping then
        LReason := Format('Connection to %s closed', [FAddress]);
      MarkBroken(LClass, LReason);
      FailAll(LClass, LReason);
    end;
  end;
end;

procedure TKafkaConnection.HandleFrame(const AContent: TBytes);
var
  LPending: TPending;
  LReader: TKafkaReader;
begin
  LPending := TakePending(KafkaPeekCorrelationId(AContent));
  if LPending = nil then
    Exit; // a response to an abandoned request
  try
    LReader := TKafkaReader.Create(AContent);
    try
      KafkaReadResponseHeader(LReader, LPending.FResponseHeaderVersion);
      LPending.Complete(Copy(AContent, LReader.Position, Length(AContent) - LReader.Position));
    finally
      LReader.Free;
    end;
  except
    on E: EKafka do
      LPending.Fail(EKafkaClass(E.ClassType), E.Message);
  end;
  LPending.TableRelease;
end;

procedure TKafkaConnection.Negotiate;
var
  LRequest: TKafkaApiVersionsRequest;
  LResponse: TKafkaApiVersionsResponse;
  LVersion: Int16;
  LBody: TBytes;
  LError: Int16;
  LAttempt, I: Integer;
  LBrokerMax: Int16;
begin
  LRequest.Init;
  LRequest.ClientSoftwareName := KAFKA_CLIENT_SOFTWARE_NAME;
  LRequest.ClientSoftwareVersion := KAFKA_FAA_VERSION_STRING;
  LVersion := KAFKA_API_VERSIONS_REQUEST_MAX_VERSION;
  for LAttempt := 1 to 2 do
  begin
    LBody := Request(KAFKA_API_KEY_API_VERSIONS, LVersion, LRequest.ToBytes(LVersion));
    if Length(LBody) < 2 then
      raise EKafkaProtocol.CreateFmt('ApiVersions answer from %s has %d bytes',
        [FAddress, Length(LBody)]);
    // ErrorCode is the first field in every version.
    LError := Int16((Word(LBody[0]) shl 8) or LBody[1]);
    if (LError = KAFKA_ERROR_UNSUPPORTED_VERSION) and (LAttempt = 1) then
    begin
      // The broker answers in v0 with the versions of ApiVersions it knows.
      LResponse.FromBytes(LBody, 0);
      LBrokerMax := -1;
      for I := 0 to High(LResponse.ApiKeys) do
        if LResponse.ApiKeys[I].ApiKey = KAFKA_API_KEY_API_VERSIONS then
          LBrokerMax := LResponse.ApiKeys[I].MaxVersion;
      if (LBrokerMax < 0) or (LBrokerMax >= LVersion) then
        raise EKafkaUnsupportedVersion.CreateFmt('%s refused ApiVersions v%d and offers no ' +
          'lower version', [FAddress, LVersion]);
      LVersion := LBrokerMax;
      Continue;
    end;
    if LError <> 0 then
      raise EKafkaBrokerError.Create(LError, Format('ApiVersions v%d to %s: %s',
        [LVersion, FAddress, KafkaErrorText(LError)]));
    LResponse.FromBytes(LBody, LVersion);
    FApiVersions := LResponse;
    FApiVersionsVersion := LVersion;
    BuildNegotiatedTable;
    Exit;
  end;
  raise EKafkaUnsupportedVersion.CreateFmt('No version of ApiVersions agreed with %s', [FAddress]);
end;

procedure TKafkaConnection.BuildNegotiatedTable;
var
  I: Integer;
  LKey: Int16;
  LInfo: TKafkaApiInfo;
  LMin, LMax: Int16;
begin
  FNegotiated := nil;
  for I := 0 to High(FApiVersions.ApiKeys) do
  begin
    LKey := FApiVersions.ApiKeys[I].ApiKey;
    if (LKey < 0) or not KafkaFindApi(LKey, LInfo) then
      Continue;
    if LKey > High(FNegotiated) then
      SetLength(FNegotiated, LKey + 1);
    LMin := FApiVersions.ApiKeys[I].MinVersion;
    if LInfo.MinVersion > LMin then
      LMin := LInfo.MinVersion;
    LMax := FApiVersions.ApiKeys[I].MaxVersion;
    if LInfo.MaxVersion < LMax then
      LMax := LInfo.MaxVersion;
    FNegotiated[LKey].Known := LMin <= LMax;
    FNegotiated[LKey].Min := LMin;
    FNegotiated[LKey].Max := LMax;
  end;
end;

procedure TKafkaConnection.Authenticate;
const
  // PLAIN takes one, SCRAM two; a broker asking for more is not following
  // the mechanism.
  MAX_ROUNDS = 8;
var
  LClient: TKafkaSaslClient;
  LHandshake: TKafkaSaslHandshakeRequest;
  LHandshakeAnswer: TKafkaSaslHandshakeResponse;
  LAuth: TKafkaSaslAuthenticateRequest;
  LAnswer: TKafkaSaslAuthenticateResponse;
  LHandshakeVersion, LAuthVersion: Int16;
  LToken: TBytes;
  LRounds, I: Integer;
  LSentAt: UInt64;
  LLifetime: Int64;
  LContext, LDetail: string;
begin
  LContext := Format('SASL %s to %s as "%s"', [KafkaSaslMechanismName(FParams.SaslMechanism),
    FAddress, FParams.SaslUsername]);
  if not TryNegotiatedVersion(KAFKA_API_KEY_SASL_HANDSHAKE, LHandshakeVersion) or
    (LHandshakeVersion < 1) then
    raise EKafkaAuthentication.Create(LContext + ': the broker offers no SaslHandshake v1');
  if not TryNegotiatedVersion(KAFKA_API_KEY_SASL_AUTHENTICATE, LAuthVersion) then
    raise EKafkaAuthentication.Create(LContext + ': the broker offers no SaslAuthenticate');
  LClient := KafkaCreateSaslClient(FParams.SaslMechanism, FParams.SaslUsername,
    FParams.SaslPassword);
  try
    LHandshake.Init;
    LHandshake.Mechanism := LClient.Mechanism;
    LHandshakeAnswer.FromBytes(Request(KAFKA_API_KEY_SASL_HANDSHAKE, LHandshakeVersion,
      LHandshake.ToBytes(LHandshakeVersion)), LHandshakeVersion);
    if LHandshakeAnswer.ErrorCode <> 0 then
    begin
      LDetail := '';
      if LHandshakeAnswer.ErrorCode = KAFKA_ERROR_UNSUPPORTED_SASL_MECHANISM then
      begin
        for I := 0 to High(LHandshakeAnswer.Mechanisms) do
        begin
          if I > 0 then
            LDetail := LDetail + ', ';
          LDetail := LDetail + LHandshakeAnswer.Mechanisms[I];
        end;
        LDetail := ' (the broker enables: ' + LDetail + ')';
      end
      else if LHandshakeAnswer.ErrorCode = KAFKA_ERROR_ILLEGAL_SASL_STATE then
        // What a broker answers on a listener without SASL.
        LDetail := ' (is this a SASL listener?)';
      raise EKafkaAuthentication.CreateCode(LHandshakeAnswer.ErrorCode, Format('%s: %s%s',
        [LContext, KafkaErrorText(LHandshakeAnswer.ErrorCode), LDetail]));
    end;
    try
      LToken := LClient.InitialResponse;
    except
      on E: EKafkaAuthentication do
        raise EKafkaAuthentication.Create(LContext + ': ' + E.Message);
    end;
    LRounds := 0;
    LSentAt := 0;
    LAnswer.Init;
    repeat
      Inc(LRounds);
      if LRounds > MAX_ROUNDS then
        raise EKafkaAuthentication.CreateFmt('%s: not complete after %d rounds',
          [LContext, MAX_ROUNDS]);
      LAuth.Init;
      LAuth.AuthBytes := LToken;
      LSentAt := PcTickMs;
      LAnswer.FromBytes(Request(KAFKA_API_KEY_SASL_AUTHENTICATE, LAuthVersion,
        LAuth.ToBytes(LAuthVersion)), LAuthVersion);
      if LAnswer.ErrorCode <> 0 then
      begin
        LDetail := '';
        if not LAnswer.ErrorMessageIsNull and (LAnswer.ErrorMessage <> '') then
          LDetail := ': ' + LAnswer.ErrorMessage;
        raise EKafkaAuthentication.CreateCode(LAnswer.ErrorCode, Format('%s: %s%s',
          [LContext, KafkaErrorText(LAnswer.ErrorCode), LDetail]));
      end;
      try
        LToken := LClient.Evaluate(LAnswer.AuthBytes);
      except
        on E: EKafkaAuthentication do
          raise EKafkaAuthentication.Create(LContext + ': ' + E.Message);
      end;
    until LClient.IsComplete;
  finally
    LClient.Free;
  end;
  // KIP-368. The broker counts the lifetime from its check, which comes
  // after the last SaslAuthenticate was sent: counting from the send is safe.
  LLifetime := LAnswer.SessionLifetimeMs;
  if LLifetime > 0 then
  begin
    FSessionLifetimeMs := LLifetime;
    if LLifetime > High(Int64) div 1000 then
      LLifetime := High(Int64) div 1000;
    // 85% to 95% (Java's window), the point chosen at random so that many
    // connections opened together don't all reconnect together.
    FReauthenticateAt := LSentAt +
      UInt64(LLifetime * (850 + PcRandomBytes(1)[0] mod 101) div 1000);
    if FReauthenticateAt = 0 then
      FReauthenticateAt := 1;
  end;
end;

function TKafkaConnection.ReauthenticationDue: Boolean;
begin
  Result := (FReauthenticateAt <> 0) and (PcTickMs >= FReauthenticateAt);
end;

function TKafkaConnection.TryNegotiatedVersion(AApiKey: Int16; out AVersion: Int16): Boolean;
begin
  Result := (AApiKey >= 0) and (AApiKey <= High(FNegotiated)) and FNegotiated[AApiKey].Known;
  if Result then
    AVersion := FNegotiated[AApiKey].Max
  else
    AVersion := -1;
end;

function TKafkaConnection.NegotiatedVersion(AApiKey: Int16): Int16;
var
  LInfo: TKafkaApiInfo;
  I: Integer;
  LBroker: string;
begin
  if TryNegotiatedVersion(AApiKey, Result) then
    Exit;
  if not KafkaFindApi(AApiKey, LInfo) then
    raise EKafkaUnsupportedVersion.CreateFmt('API key %d has no codec in this library', [AApiKey]);
  LBroker := 'doesn''t offer it';
  for I := 0 to High(FApiVersions.ApiKeys) do
    if FApiVersions.ApiKeys[I].ApiKey = AApiKey then
      LBroker := Format('offers %d-%d', [FApiVersions.ApiKeys[I].MinVersion,
        FApiVersions.ApiKeys[I].MaxVersion]);
  raise EKafkaUnsupportedVersion.CreateFmt('%s: this library has versions %d-%d and %s %s',
    [LInfo.Name, LInfo.MinVersion, LInfo.MaxVersion, FAddress, LBroker]);
end;

function TKafkaConnection.BrokerApiVersions: TKafkaApiVersionsResponse;
begin
  Result := FApiVersions;
end;

function TKafkaConnection.Metadata(const ARequest: TKafkaMetadataRequest): TKafkaMetadataResponse;
var
  LVersion: Int16;
begin
  LVersion := NegotiatedVersion(KAFKA_API_KEY_METADATA);
  Result.FromBytes(Request(KAFKA_API_KEY_METADATA, LVersion, ARequest.ToBytes(LVersion)), LVersion);
  if Result.ErrorCode <> 0 then
    raise EKafkaBrokerError.Create(Result.ErrorCode, Format('Metadata v%d to %s: %s',
      [LVersion, FAddress, KafkaErrorText(Result.ErrorCode)]));
end;

function TKafkaConnection.Produce(const ARequest: TKafkaProduceRequest): TKafkaProduceResponse;
var
  LVersion: Int16;
  LBody: TBytes;
begin
  LVersion := NegotiatedVersion(KAFKA_API_KEY_PRODUCE);
  LBody := ARequest.ToBytes(LVersion);
  if ARequest.Acks = 0 then
  begin
    SendOneWay(KAFKA_API_KEY_PRODUCE, LVersion, LBody);
    Result.Init;
  end
  else
    Result.FromBytes(Request(KAFKA_API_KEY_PRODUCE, LVersion, LBody), LVersion);
end;

function TKafkaConnection.ListOffsets(
  const ARequest: TKafkaListOffsetsRequest): TKafkaListOffsetsResponse;
var
  LVersion: Int16;
begin
  LVersion := NegotiatedVersion(KAFKA_API_KEY_LIST_OFFSETS);
  Result.FromBytes(Request(KAFKA_API_KEY_LIST_OFFSETS, LVersion, ARequest.ToBytes(LVersion)),
    LVersion);
end;

function TKafkaConnection.Fetch(const ARequest: TKafkaFetchRequest): TKafkaFetchResponse;
var
  LVersion: Int16;
  LWait: Cardinal;
begin
  LVersion := NegotiatedVersion(KAFKA_API_KEY_FETCH);
  LWait := RequestTimeoutMs;
  if ARequest.MaxWaitMs > 0 then
    Inc(LWait, Cardinal(ARequest.MaxWaitMs));
  Result.FromBytes(Request(KAFKA_API_KEY_FETCH, LVersion, ARequest.ToBytes(LVersion), LWait),
    LVersion);
  if Result.ErrorCode <> 0 then
    raise EKafkaBrokerError.Create(Result.ErrorCode, Format('Fetch v%d to %s: %s',
      [LVersion, FAddress, KafkaErrorText(Result.ErrorCode)]));
end;

end.
