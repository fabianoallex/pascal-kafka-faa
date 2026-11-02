unit Kafka.Protocol.Envelope;

{$I kafka.inc}

{ The request and response envelope: the size prefix and the headers around
  every message body.

    request   INT32 size | request header | body
    response  INT32 size | response header | body

  Request header (RequestHeader.json at the Kafka tag in
  tools/kafka-messages):
    v1  INT16 api_key, INT16 api_version, INT32 correlation_id,
        NULLABLE_STRING client_id
    v2  the same, then a tagged-field section
  v0 (no client_id) served only ControlledShutdown v0 and is gone in Kafka 4
  (KIP-896). client_id stays a classic INT16-length string in v2 too: a
  broker must be able to read the header of an ApiVersions request of any
  version, including one newer than it knows.

  Response header (ResponseHeader.json):
    v0  INT32 correlation_id
    v1  the same, then a tagged-field section

  Which header a message uses follows from its body: v2/v1 when the body's
  version is flexible, v1/v0 otherwise. One exception, kept here so that no
  caller has to remember it: an ApiVersions RESPONSE always has header v0,
  even for a flexible version, so that a client that asked for a version the
  broker doesn't know can still read the error (ApiVersionsResponse.json, the
  comment on version 3).

  The headers are written by hand because the generator (decision 3 of
  docs/design.md) covers bodies; RequestHeader.json's per-field
  flexibleVersions for client_id is exactly the kind of rule that is easier
  to state once than to generate. }

interface

uses
  SysUtils,
  Kafka.Types,
  Kafka.Protocol.Primitives;

type
  TKafkaRequestHeader = record
    ApiKey: Int16;
    ApiVersion: Int16;
    CorrelationId: Int32;
    ClientId: string;
    ClientIdIsNull: Boolean;
    /// Tagged fields of a v2 header (none are defined today).
    TaggedFields: TKafkaRawTaggedFields;
  end;

  TKafkaResponseHeader = record
    CorrelationId: Int32;
    TaggedFields: TKafkaRawTaggedFields;
  end;

/// 2 for a flexible body, 1 otherwise.
function KafkaRequestHeaderVersion(AFlexibleBody: Boolean): Int16;
/// 1 for a flexible body, 0 otherwise; always 0 for ApiVersions.
function KafkaResponseHeaderVersion(AApiKey: Int16; AFlexibleBody: Boolean): Int16;

procedure KafkaWriteRequestHeader(AWriter: TKafkaWriter; const AHeader: TKafkaRequestHeader;
  AHeaderVersion: Int16);
function KafkaReadRequestHeader(AReader: TKafkaReader; AHeaderVersion: Int16): TKafkaRequestHeader;

procedure KafkaWriteResponseHeader(AWriter: TKafkaWriter; const AHeader: TKafkaResponseHeader;
  AHeaderVersion: Int16);
function KafkaReadResponseHeader(AReader: TKafkaReader; AHeaderVersion: Int16): TKafkaResponseHeader;

/// A whole request frame: size prefix, header and ABody (an encoded body).
function KafkaEncodeRequestFrame(const AHeader: TKafkaRequestHeader; AHeaderVersion: Int16;
  const ABody: TBytes): TBytes;
/// A whole response frame, as a broker sends it (the tests' fake broker).
function KafkaEncodeResponseFrame(const AHeader: TKafkaResponseHeader; AHeaderVersion: Int16;
  const ABody: TBytes): TBytes;

/// The correlation id of a response frame's content (the bytes after the
/// size prefix): its first 4 bytes, the same in both header versions.
function KafkaPeekCorrelationId(const AFrameContent: TBytes): Int32;

/// The request API key and version of a request frame's content (after the
/// size prefix), without reading the rest.
procedure KafkaPeekRequestApi(const AFrameContent: TBytes; out AApiKey, AApiVersion: Int16);

implementation

uses
  Kafka.Messages.Apis;

function KafkaRequestHeaderVersion(AFlexibleBody: Boolean): Int16;
begin
  if AFlexibleBody then
    Result := 2
  else
    Result := 1;
end;

function KafkaResponseHeaderVersion(AApiKey: Int16; AFlexibleBody: Boolean): Int16;
begin
  if AFlexibleBody and (AApiKey <> KAFKA_API_KEY_API_VERSIONS) then
    Result := 1
  else
    Result := 0;
end;

procedure CheckRequestHeaderVersion(AVersion: Int16);
begin
  if (AVersion < 1) or (AVersion > 2) then
    raise EKafkaUnsupportedVersion.CreateFmt('Request header version %d is not supported (1-2)',
      [AVersion]);
end;

procedure CheckResponseHeaderVersion(AVersion: Int16);
begin
  if (AVersion < 0) or (AVersion > 1) then
    raise EKafkaUnsupportedVersion.CreateFmt('Response header version %d is not supported (0-1)',
      [AVersion]);
end;

procedure WriteTaggedSection(AWriter: TKafkaWriter; const AFields: TKafkaRawTaggedFields);
var
  Index: Integer;
begin
  AWriter.WriteUnsignedVarint(Cardinal(Length(AFields)));
  Index := 0;
  AWriter.WriteRawTaggedFieldsBelow(AFields, Index, High(Int64));
end;

procedure KafkaWriteRequestHeader(AWriter: TKafkaWriter; const AHeader: TKafkaRequestHeader;
  AHeaderVersion: Int16);
begin
  CheckRequestHeaderVersion(AHeaderVersion);
  AWriter.WriteInt16(AHeader.ApiKey);
  AWriter.WriteInt16(AHeader.ApiVersion);
  AWriter.WriteInt32(AHeader.CorrelationId);
  // Classic form in both versions (see the unit's header).
  AWriter.WriteNullableString(AHeader.ClientId, AHeader.ClientIdIsNull, False);
  if AHeaderVersion >= 2 then
    WriteTaggedSection(AWriter, AHeader.TaggedFields);
end;

function KafkaReadRequestHeader(AReader: TKafkaReader; AHeaderVersion: Int16): TKafkaRequestHeader;
begin
  CheckRequestHeaderVersion(AHeaderVersion);
  Result := Default(TKafkaRequestHeader);
  Result.ApiKey := AReader.ReadInt16;
  Result.ApiVersion := AReader.ReadInt16;
  Result.CorrelationId := AReader.ReadInt32;
  Result.ClientId := AReader.ReadNullableString(False, Result.ClientIdIsNull);
  if AHeaderVersion >= 2 then
    Result.TaggedFields := AReader.ReadRawTaggedFields;
end;

procedure KafkaWriteResponseHeader(AWriter: TKafkaWriter; const AHeader: TKafkaResponseHeader;
  AHeaderVersion: Int16);
begin
  CheckResponseHeaderVersion(AHeaderVersion);
  AWriter.WriteInt32(AHeader.CorrelationId);
  if AHeaderVersion >= 1 then
    WriteTaggedSection(AWriter, AHeader.TaggedFields);
end;

function KafkaReadResponseHeader(AReader: TKafkaReader; AHeaderVersion: Int16): TKafkaResponseHeader;
begin
  CheckResponseHeaderVersion(AHeaderVersion);
  Result := Default(TKafkaResponseHeader);
  Result.CorrelationId := AReader.ReadInt32;
  if AHeaderVersion >= 1 then
    Result.TaggedFields := AReader.ReadRawTaggedFields;
end;

function KafkaEncodeRequestFrame(const AHeader: TKafkaRequestHeader; AHeaderVersion: Int16;
  const ABody: TBytes): TBytes;
var
  W: TKafkaWriter;
begin
  W := TKafkaWriter.Create(64 + Length(ABody));
  try
    W.WriteInt32(0); // the size, patched below
    KafkaWriteRequestHeader(W, AHeader, AHeaderVersion);
    W.WriteRaw(ABody);
    W.PatchInt32(0, W.Length - 4);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function KafkaEncodeResponseFrame(const AHeader: TKafkaResponseHeader; AHeaderVersion: Int16;
  const ABody: TBytes): TBytes;
var
  W: TKafkaWriter;
begin
  W := TKafkaWriter.Create(16 + Length(ABody));
  try
    W.WriteInt32(0);
    KafkaWriteResponseHeader(W, AHeader, AHeaderVersion);
    W.WriteRaw(ABody);
    W.PatchInt32(0, W.Length - 4);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function KafkaPeekCorrelationId(const AFrameContent: TBytes): Int32;
var
  R: TKafkaReader;
begin
  R := TKafkaReader.Create(AFrameContent);
  try
    Result := R.ReadInt32;
  finally
    R.Free;
  end;
end;

procedure KafkaPeekRequestApi(const AFrameContent: TBytes; out AApiKey, AApiVersion: Int16);
var
  R: TKafkaReader;
begin
  R := TKafkaReader.Create(AFrameContent);
  try
    AApiKey := R.ReadInt16;
    AApiVersion := R.ReadInt16;
  finally
    R.Free;
  end;
end;

end.
