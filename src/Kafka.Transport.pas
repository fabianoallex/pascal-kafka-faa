unit Kafka.Transport;

{$I kafka.inc}

{ The library's side of pascal-common-faa's TCP and TLS transport: the
  exception boundary, exact reads and writes on an endpoint, and the TCP and
  TLS options a connection uses.

  The EXCEPTION BOUNDARY (T6 of the common's docs/transport-design.md): no
  EPcTransport* leaves the library. KafkaRaiseTransportError turns EPcTls into
  EKafkaTls and every other EPcTransport (refused, closed, a deadline, a name
  that doesn't resolve) into EKafkaConnection, with the common's message after
  the library's context ("Connecting to localhost:9093: ..."). redis and amqp
  did the same; amqp did it in a TPcEndpointStream subclass, because its frame
  reader reads a TStream.

  Kafka.Connection reads and writes the endpoint directly instead of through a
  stream: every frame is length-prefixed and read whole, so the only stream
  behaviour it would use is ReadBuffer's loop over short reads, and whether
  TStream.ReadBuffer loops or raises on a short read is a detail of each RTL's
  version. KafkaReadExactly loops on its own.

  TCP options: TCP_NODELAY on (the zeroed record's default in the common:
  Kafka is request-reply, and Nagle's delay costs about 40 ms per round trip
  on Linux), no keep-alive for now, and the connect budget. Not setting a read
  deadline is deliberate: the connection's reader waits indefinitely while the
  broker has nothing to say, and request timeouts live in the correlation
  table (docs/design.md, "Threads"). }

interface

uses
  SysUtils,
  PascalCommon.Transport,
  Kafka.Types;

/// Raises the library's exception for a common transport exception: EKafkaTls
/// for EPcTls, EKafkaConnection otherwise. AContext, when not empty, goes
/// before the common's message with ': '.
procedure KafkaRaiseTransportError(E: EPcTransport; const AContext: string);

/// Reads exactly ACount bytes, looping over short reads. Raises
/// EKafkaConnection/EKafkaTls (never EPcTransport*).
procedure KafkaReadExactly(AEndpoint: TPcEndpoint; var ABuffer; ACount: Integer;
  const AContext: string);

/// Writes ACount bytes. Raises EKafkaConnection/EKafkaTls.
procedure KafkaWriteExactly(AEndpoint: TPcEndpoint; const ABuffer; ACount: Integer;
  const AContext: string);

/// TCP_NODELAY, no keep-alive, and the connect budget (0 = the common's
/// default, PC_TCP_CONNECT_TIMEOUT_DEFAULT, 10 s).
function KafkaTcpOptions(AConnectTimeoutMs: Cardinal): TPcTcpOptions;

/// The name of the TLS backend tbDefault resolves to in this build, or of a
/// given one: 'SChannel' or 'OpenSSL'.
function KafkaTlsBackendName(ABackend: TPcTlsBackend = tbDefault): string;

/// KafkaTlsBackendName with the run-time detail: on OpenSSL the library and
/// version loaded (after the first TLS connection).
function KafkaTlsBackendInfo(ABackend: TPcTlsBackend = tbDefault): string;

implementation

uses
  PascalCommon.Transport.Tls;

procedure KafkaRaiseTransportError(E: EPcTransport; const AContext: string);
var
  LMessage: string;
begin
  if AContext = '' then
    LMessage := E.Message
  else
    LMessage := AContext + ': ' + E.Message;
  if E is EPcTls then
    raise EKafkaTls.Create(LMessage);
  raise EKafkaConnection.Create(LMessage);
end;

procedure KafkaReadExactly(AEndpoint: TPcEndpoint; var ABuffer; ACount: Integer;
  const AContext: string);
var
  P: PByte;
  N: Integer;
begin
  P := @ABuffer;
  try
    while ACount > 0 do
    begin
      N := AEndpoint.Read(P^, ACount);
      Inc(P, N);
      Dec(ACount, N);
    end;
  except
    on E: EPcTransport do
      KafkaRaiseTransportError(E, AContext);
  end;
end;

procedure KafkaWriteExactly(AEndpoint: TPcEndpoint; const ABuffer; ACount: Integer;
  const AContext: string);
begin
  try
    AEndpoint.WriteExactly(ABuffer, ACount);
  except
    on E: EPcTransport do
      KafkaRaiseTransportError(E, AContext);
  end;
end;

function KafkaTcpOptions(AConnectTimeoutMs: Cardinal): TPcTcpOptions;
begin
  Result := Default(TPcTcpOptions);
  Result.ConnectTimeoutMs := AConnectTimeoutMs;
end;

function KafkaTlsBackendName(ABackend: TPcTlsBackend): string;
begin
  Result := PcTlsBackendName(ABackend);
end;

function KafkaTlsBackendInfo(ABackend: TPcTlsBackend): string;
begin
  Result := PcTlsBackendInfo(ABackend);
end;

end.
