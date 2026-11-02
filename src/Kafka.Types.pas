unit Kafka.Types;

{$I kafka.inc}

{ The library's exceptions, its version, and the minimum pascal-common-faa it
  compiles against.

  Exceptions. No exception of pascal-common-faa's transport (EPcTransport*)
  leaves the library: Kafka.Transport turns each into one of these, with the
  common's message (its docs/transport-design.md, T6). The tree:

    EKafka                       any failure raised by the library
      EKafkaConnection           the socket: refused, unreachable, closed,
                                 a transport deadline, a broken connection
        EKafkaTls                handshake, certificate, TLS library missing
      EKafkaTimeout              a request got no response in time
      EKafkaProtocol             bytes that don't follow the protocol
      EKafkaCompression          a codec the library doesn't have (snappy,
                                 lz4, zstd until phase K3), or compressed
                                 data that doesn't decompress
      EKafkaUnsupportedVersion   no version both sides know, or a field that
                                 a version can't carry
      EKafkaWakeup               a consumer's Poll ended by Wakeup
      EKafkaAuthentication       SASL failed: a mechanism the broker doesn't
                                 enable, wrong credentials, a listener
                                 without SASL, or a broker answer that fails
                                 the mechanism's checks (SCRAM's signature)
      EKafkaBrokerError          the broker answered with an error code (or
                                 the producer refused a record with the
                                 code the broker would give: MESSAGE_TOO_LARGE,
                                 UNKNOWN_TOPIC_OR_PARTITION)

  EKafkaTls descends from EKafkaConnection on purpose, unlike amqp's and
  redis's TLS exceptions: those kept a sibling class so that catch blocks
  written before TLS existed wouldn't change meaning. This library has no such
  history, and a TLS failure is a failure to connect. EKafkaTimeout is a
  request's deadline (TKafkaConnectionParams.RequestTimeoutMs); a transport
  deadline (the connect budget, for instance) is an EKafkaConnection.

  EKafkaAuthentication is not an EKafkaConnection, on purpose: the producer
  and the consumer retry a connection failure, and credentials don't fix
  themselves (Java's AuthenticationException isn't retriable either). It
  carries the broker's error code (UNSUPPORTED_SASL_MECHANISM,
  SASL_AUTHENTICATION_FAILED, ILLEGAL_SASL_STATE), or 0 when the client's
  own check failed.

  PASCALCOMMON_VERSION 12701 is pascal-common-faa 1.27.1: the transport came
  in 1.27.0, and 1.27.1 fixed its SChannel server, which waited out the whole
  handshake deadline (15 s) for a client that doesn't speak TLS; OpenSSL's
  refused at once. Also in packages/pascal_kafka_faa.lpk (MinVersion). }

interface

uses
  SysUtils,
  PascalCommon.Version;

{$IF PASCALCOMMON_VERSION < 12701}
  {$MESSAGE FATAL 'pascal-kafka-faa needs pascal-common-faa 1.27.1 or later'}
{$IFEND}

const
  KAFKA_FAA_VERSION_MAJOR = 0;
  KAFKA_FAA_VERSION_MINOR = 1;
  KAFKA_FAA_VERSION_PATCH = 0;
  /// major * 10000 + minor * 100 + patch, as pascal-common-faa's.
  KAFKA_FAA_VERSION = 100;
  KAFKA_FAA_VERSION_STRING = '0.1.0';

  /// No timeout: Flush, Close and the delivery futures' Wait (Kafka.Producer),
  /// Poll (Kafka.Consumer). The same value as pascal-common-faa's
  /// PC_WAIT_INFINITE.
  KAFKA_WAIT_INFINITE = Cardinal($FFFFFFFF);

  /// What the library reports in ApiVersions (ClientSoftwareName, KIP-511).
  KAFKA_CLIENT_SOFTWARE_NAME = 'pascal-kafka-faa';

type
  EKafka = class(Exception);

  EKafkaConnection = class(EKafka);
  EKafkaTls = class(EKafkaConnection);
  EKafkaTimeout = class(EKafka);
  EKafkaProtocol = class(EKafka);
  EKafkaUnsupportedVersion = class(EKafka);
  EKafkaCompression = class(EKafka);
  EKafkaWakeup = class(EKafka);

  EKafkaAuthentication = class(EKafka)
  private
    FErrorCode: Int16;
  public
    constructor CreateCode(AErrorCode: Int16; const AMessage: string);
    /// The broker's error code; 0 when the client's own check failed.
    property ErrorCode: Int16 read FErrorCode;
  end;

  { An error code in a broker's answer. The message names the code
    (Kafka.Errors); ErrorCode keeps the number for a caller that decides on
    it (retry on a retriable error, refresh metadata on
    NOT_LEADER_OR_FOLLOWER...). }
  EKafkaBrokerError = class(EKafka)
  private
    FErrorCode: Int16;
  public
    constructor Create(AErrorCode: Int16; const AMessage: string);
    property ErrorCode: Int16 read FErrorCode;
  end;

  /// The class of a library exception, to store a failure and raise it
  /// again on another thread (a request's response future).
  EKafkaClass = class of EKafka;

implementation

constructor EKafkaAuthentication.CreateCode(AErrorCode: Int16; const AMessage: string);
begin
  inherited Create(AMessage);
  FErrorCode := AErrorCode;
end;

constructor EKafkaBrokerError.Create(AErrorCode: Int16; const AMessage: string);
begin
  inherited Create(AMessage);
  FErrorCode := AErrorCode;
end;

end.
