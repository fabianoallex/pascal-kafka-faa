unit Kafka.TestProxy;

{ An in-process TCP proxy in front of a real broker, for the integration tests
  that need to lose an answer the broker gave (phase K3's criterion: a retry
  after a lost answer writes nothing twice).

  It listens on 127.0.0.1, port 0, and relays every connection to the target
  broker, frame by frame (each Kafka frame is size-prefixed, read whole): a
  thread per direction. Requests are noted by correlation id (API key and
  version), so that their answers can be told apart:

  - Metadata answers are decoded with the generated codec and their brokers'
    addresses replaced with the proxy's: otherwise a client would reach the
    broker directly at its advertised address after the bootstrap
    (CLAUDE.md, gotcha 9). One broker behind it: every node id comes here.
  - LoseProduceAnswers: the next N Produce answers are not passed on; the
    broker wrote the batch, the client never hears of it. LostCount says how
    many were lost.
  - The Produce requests waiting for an answer on one connection are
    counted, and the most seen at once is kept (MaxProduceInFlight): how many
    batches a producer had in flight against the real broker.

  Not a test unit (its name doesn't end in Tests): tools/gen_fpc_mirror.py
  leaves it alone and both suites compile this one copy. TLS and SASL aren't
  spoken: plaintext only. }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  PascalCommon.Transport,
  PascalCommon.ThreadPool;

type
  TKafkaTestProxy = class;

  { One client connection and its connection to the broker. }
  TKafkaTestProxyPair = class
  private type
    TPump = class(TThread)
    private
      FPair: TKafkaTestProxyPair;
      FToBroker: Boolean;
    protected
      procedure Execute; override;
    public
      constructor Create(APair: TKafkaTestProxyPair; AToBroker: Boolean);
    end;
  private
    FOwner: TKafkaTestProxy;
    FClient: TPcEndpoint;
    FBroker: TPcEndpoint;
    FLock: TCriticalSection;
    FRequests: TDictionary<Int32, Integer>;
    FProduceInFlight: Integer;
    FUp, FDown: TPump;
    FAborted: Boolean;
    procedure Abort;
    procedure PumpUp;
    procedure PumpDown;
  public
    constructor Create(AOwner: TKafkaTestProxy; AClient, ABroker: TPcEndpoint);
    destructor Destroy; override;
  end;

  TKafkaTestProxy = class
  private type
    TAcceptThread = class(TThread)
    private
      FOwner: TKafkaTestProxy;
    protected
      procedure Execute; override;
    public
      constructor Create(AOwner: TKafkaTestProxy);
    end;
  private
    FTargetHost: string;
    FTargetPort: Word;
    FListener: TPcListener;
    FAccept: TAcceptThread;
    FPairs: TObjectList<TKafkaTestProxyPair>;
    FMonitor: TPcMonitor;
    FLoseProduceAnswers: Integer;
    FLostCount: Integer;
    FProduceCount: Integer;
    FMaxProduceInFlight: Integer;
    procedure AcceptLoop;
    function RewriteMetadata(const AContent: TBytes; AVersion: Int16): TBytes;
  public
    constructor Create(const ATargetHost: string; ATargetPort: Word);
    destructor Destroy; override;
    function Port: Word;
    /// The next ACount Produce answers are dropped.
    procedure LoseProduceAnswers(ACount: Integer);
    function LostCount: Integer;
    /// Produce requests relayed.
    function ProduceCount: Integer;
    /// The most Produce requests waiting for an answer on one connection.
    function MaxProduceInFlight: Integer;
  end;

implementation

uses
  PascalCommon.Transport.Tcp,
  Kafka.Types,
  Kafka.Protocol.Primitives,
  Kafka.Protocol.Envelope,
  Kafka.Messages.Apis,
  Kafka.Messages.Metadata,
  Kafka.Transport;

const
  MAX_FRAME = 128 * 1024 * 1024;


function ReadFrame(AEndpoint: TPcEndpoint): TBytes;
var
  LSize: array[0..3] of Byte;
  N: Integer;
begin
  KafkaReadExactly(AEndpoint, LSize[0], 4, 'proxy');
  N := (Integer(LSize[0]) shl 24) or (Integer(LSize[1]) shl 16) or (Integer(LSize[2]) shl 8) or
    Integer(LSize[3]);
  if (N <= 0) or (N > MAX_FRAME) then
    raise EKafkaProtocol.CreateFmt('proxy: frame of %d bytes', [N]);
  Result := nil;
  SetLength(Result, N);
  KafkaReadExactly(AEndpoint, Result[0], N, 'proxy');
end;

/// A request's correlation id: bytes 4 to 7, after the API key and version
/// (KafkaPeekCorrelationId reads a response's, bytes 0 to 3).
function RequestCorrelationId(const AContent: TBytes): Int32;
begin
  Result := Int32((Cardinal(AContent[4]) shl 24) or (Cardinal(AContent[5]) shl 16) or
    (Cardinal(AContent[6]) shl 8) or Cardinal(AContent[7]));
end;

procedure WriteFrame(AEndpoint: TPcEndpoint; const AContent: TBytes);
var
  LFrame: TBytes;
  N: Integer;
begin
  N := Length(AContent);
  LFrame := nil;
  SetLength(LFrame, N + 4);
  LFrame[0] := Byte(N shr 24);
  LFrame[1] := Byte(N shr 16);
  LFrame[2] := Byte(N shr 8);
  LFrame[3] := Byte(N);
  Move(AContent[0], LFrame[4], N);
  KafkaWriteExactly(AEndpoint, LFrame[0], Length(LFrame), 'proxy');
end;

{ TKafkaTestProxyPair.TPump }

constructor TKafkaTestProxyPair.TPump.Create(APair: TKafkaTestProxyPair; AToBroker: Boolean);
begin
  inherited Create(True);
  FPair := APair;
  FToBroker := AToBroker;
  FreeOnTerminate := False;
end;

procedure TKafkaTestProxyPair.TPump.Execute;
begin
  try
    if FToBroker then
      FPair.PumpUp
    else
      FPair.PumpDown;
  except
    // A side hung up: both go.
  end;
  FPair.Abort;
end;

{ TKafkaTestProxyPair }

constructor TKafkaTestProxyPair.Create(AOwner: TKafkaTestProxy; AClient, ABroker: TPcEndpoint);
begin
  inherited Create;
  FOwner := AOwner;
  FClient := AClient;
  FBroker := ABroker;
  FLock := TCriticalSection.Create;
  FRequests := TDictionary<Int32, Integer>.Create;
  FUp := TPump.Create(Self, True);
  FDown := TPump.Create(Self, False);
  FUp.Start;
  FDown.Start;
end;

destructor TKafkaTestProxyPair.Destroy;
begin
  Abort;
  FUp.WaitFor;
  FDown.WaitFor;
  FUp.Free;
  FDown.Free;
  FClient.Free;
  FBroker.Free;
  FRequests.Free;
  FLock.Free;
  inherited;
end;

procedure TKafkaTestProxyPair.Abort;
begin
  FLock.Enter;
  try
    if FAborted then
      Exit;
    FAborted := True;
    FClient.CloseAbort;
    FBroker.CloseAbort;
  finally
    FLock.Leave;
  end;
end;

procedure TKafkaTestProxyPair.PumpUp;
var
  LContent: TBytes;
  LKey, LVersion: Int16;
begin
  while True do
  begin
    LContent := ReadFrame(FClient);
    KafkaPeekRequestApi(LContent, LKey, LVersion);
    FLock.Enter;
    try
      FRequests.AddOrSetValue(RequestCorrelationId(LContent),
        (Integer(LKey) shl 16) or (Integer(LVersion) and $FFFF));
    finally
      FLock.Leave;
    end;
    if LKey = KAFKA_API_KEY_PRODUCE then
    begin
      FOwner.FMonitor.Enter;
      try
        Inc(FOwner.FProduceCount);
        Inc(FProduceInFlight);
        if FProduceInFlight > FOwner.FMaxProduceInFlight then
          FOwner.FMaxProduceInFlight := FProduceInFlight;
      finally
        FOwner.FMonitor.Leave;
      end;
    end;
    WriteFrame(FBroker, LContent);
  end;
end;

procedure TKafkaTestProxyPair.PumpDown;
var
  LContent: TBytes;
  LCorrelation, LApi: Integer;
  LKey, LVersion: Int16;
  LDrop: Boolean;
begin
  while True do
  begin
    LContent := ReadFrame(FBroker);
    LCorrelation := KafkaPeekCorrelationId(LContent);
    FLock.Enter;
    try
      if not FRequests.TryGetValue(LCorrelation, LApi) then
        LApi := -1
      else
        FRequests.Remove(LCorrelation);
    finally
      FLock.Leave;
    end;
    LDrop := False;
    if LApi >= 0 then
    begin
      LKey := Int16(LApi shr 16);
      LVersion := Int16(LApi and $FFFF);
      if LKey = KAFKA_API_KEY_METADATA then
        LContent := FOwner.RewriteMetadata(LContent, LVersion)
      else if LKey = KAFKA_API_KEY_PRODUCE then
      begin
        FOwner.FMonitor.Enter;
        try
          Dec(FProduceInFlight);
          if FOwner.FLoseProduceAnswers > 0 then
          begin
            Dec(FOwner.FLoseProduceAnswers);
            Inc(FOwner.FLostCount);
            LDrop := True;
          end;
          FOwner.FMonitor.PulseAll;
        finally
          FOwner.FMonitor.Leave;
        end;
      end;
    end;
    if not LDrop then
      WriteFrame(FClient, LContent);
  end;
end;

{ TKafkaTestProxy.TAcceptThread }

constructor TKafkaTestProxy.TAcceptThread.Create(AOwner: TKafkaTestProxy);
begin
  inherited Create(True);
  FOwner := AOwner;
  FreeOnTerminate := False;
end;

procedure TKafkaTestProxy.TAcceptThread.Execute;
begin
  FOwner.AcceptLoop;
end;

{ TKafkaTestProxy }

constructor TKafkaTestProxy.Create(const ATargetHost: string; ATargetPort: Word);
begin
  inherited Create;
  FTargetHost := ATargetHost;
  FTargetPort := ATargetPort;
  FMonitor := TPcMonitor.Create;
  FPairs := TObjectList<TKafkaTestProxyPair>.Create(True);
  FListener := PcTcpListen('127.0.0.1', 0, Default(TPcTcpOptions));
  FAccept := TAcceptThread.Create(Self);
  FAccept.Start;
end;

destructor TKafkaTestProxy.Destroy;
begin
  FListener.Close;
  FAccept.WaitFor;
  FAccept.Free;
  // Each pair aborts both sockets and joins its threads.
  FPairs.Free;
  FListener.Free;
  FMonitor.Free;
  inherited;
end;

procedure TKafkaTestProxy.AcceptLoop;
var
  LClient, LBroker: TPcEndpoint;
begin
  try
    while True do
    begin
      LClient := FListener.Accept;
      if LClient = nil then
        Exit; // the listener was closed (Accept returns nil, it doesn't raise)
      try
        LBroker := PcTcpConnect(FTargetHost, FTargetPort, KafkaTcpOptions(5000));
      except
        LClient.Free;
        Continue;
      end;
      FMonitor.Enter;
      try
        FPairs.Add(TKafkaTestProxyPair.Create(Self, LClient, LBroker));
      finally
        FMonitor.Leave;
      end;
    end;
  except
    // A test helper never takes the runner down.
  end;
end;

function TKafkaTestProxy.RewriteMetadata(const AContent: TBytes; AVersion: Int16): TBytes;
var
  LReader: TKafkaReader;
  LHeader: TKafkaResponseHeader;
  LHeaderVersion: Int16;
  R: TKafkaMetadataResponse;
  I: Integer;
begin
  LHeaderVersion := KafkaResponseHeaderVersion(KAFKA_API_KEY_METADATA,
    KafkaApiIsFlexible(KAFKA_API_KEY_METADATA, AVersion));
  LReader := TKafkaReader.Create(AContent);
  try
    LHeader := KafkaReadResponseHeader(LReader, LHeaderVersion);
    R.FromBytes(Copy(AContent, LReader.Position, Length(AContent) - LReader.Position), AVersion);
  finally
    LReader.Free;
  end;
  for I := 0 to High(R.Brokers) do
  begin
    R.Brokers[I].Host := '127.0.0.1';
    R.Brokers[I].Port := Port;
  end;
  // Without its size prefix, as the frame was read.
  Result := KafkaEncodeResponseFrame(LHeader, LHeaderVersion, R.ToBytes(AVersion));
  Result := Copy(Result, 4, Length(Result) - 4);
end;

function TKafkaTestProxy.Port: Word;
begin
  Result := FListener.Port;
end;

procedure TKafkaTestProxy.LoseProduceAnswers(ACount: Integer);
begin
  FMonitor.Enter;
  try
    FLoseProduceAnswers := ACount;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaTestProxy.LostCount: Integer;
begin
  FMonitor.Enter;
  try
    Result := FLostCount;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaTestProxy.ProduceCount: Integer;
begin
  FMonitor.Enter;
  try
    Result := FProduceCount;
  finally
    FMonitor.Leave;
  end;
end;

function TKafkaTestProxy.MaxProduceInFlight: Integer;
begin
  FMonitor.Enter;
  try
    Result := FMaxProduceInFlight;
  finally
    FMonitor.Leave;
  end;
end;

end.
