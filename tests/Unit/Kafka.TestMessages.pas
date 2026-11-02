unit Kafka.TestMessages;

{ Helpers shared by the codec tests: a capture by name (Kafka.TestCaptures),
  split into its header and its body, and the records of a captured Fetch
  answer.

  Shared, not mirrored: the name doesn't end in Tests, so
  tools/gen_fpc_mirror.py leaves it alone and both runners compile this one
  file (the FPC one finds it through the .lpi's search path, ..\..\Unit). }

interface

uses
  SysUtils,
  Kafka.Protocol.Primitives,
  Kafka.Protocol.Envelope,
  Kafka.Messages.Apis,
  Kafka.Messages.Fetch,
  Kafka.Messages.Produce,
  Kafka.TestCaptures,
  Kafka.TestHelpers;

/// The capture's frame without its size prefix; an exception naming it when
/// there is none.
function CaptureBytes(const AName: string; AIsRequest: Boolean): TBytes;
/// The body of a captured request: after its header.
function RequestBody(const AContent: TBytes; out AHeader: TKafkaRequestHeader): TBytes;
/// The body of a captured response of AKey/AVersion: after its header.
function ResponseBody(const AContent: TBytes; AKey, AVersion: Int16;
  out AHeader: TKafkaResponseHeader): TBytes;
/// The capture's API version (the request's and the response's are the same).
function CaptureVersion(const AName: string): Int16;
/// The request body of capture AName.
function CaptureRequestBody(const AName: string): TBytes;
/// The response body of capture AName.
function CaptureResponseBody(const AName: string): TBytes;

/// A captured Fetch answer, decoded.
function DecodeFetchResponse(const AName: string): TKafkaFetchResponse;
/// The records of the first partition of a captured Fetch answer.
function FetchRecords(const AName: string): TBytes;
/// The records of the first partition of a captured Produce request.
function ProduceRecords(const AName: string): TBytes;

/// UTF-8 bytes of AText, and back (an exception for bytes that aren't UTF-8).
function Utf8(const AText: string): TBytes;
function FromUtf8(const AData: TBytes): string;

implementation

uses
  PascalCommon.Utf8;

function CaptureIndex(const AName: string; AIsRequest: Boolean): Integer;
begin
  Result := KafkaFindCapture(AName, AIsRequest);
  if Result < 0 then
    raise Exception.CreateFmt('No capture "%s" (%s)', [AName, BoolToStr(AIsRequest, True)]);
end;

function CaptureBytes(const AName: string; AIsRequest: Boolean): TBytes;
begin
  Result := KafkaCaptureBytes(KAFKA_CAPTURES[CaptureIndex(AName, AIsRequest)]);
end;

function RequestBody(const AContent: TBytes; out AHeader: TKafkaRequestHeader): TBytes;
var
  R: TKafkaReader;
  Key, Version: Int16;
begin
  KafkaPeekRequestApi(AContent, Key, Version);
  R := TKafkaReader.Create(AContent);
  try
    AHeader := KafkaReadRequestHeader(R, KafkaRequestHeaderVersion(KafkaApiIsFlexible(Key, Version)));
    Result := BytesFrom(AContent, R.Position);
  finally
    R.Free;
  end;
end;

function ResponseBody(const AContent: TBytes; AKey, AVersion: Int16;
  out AHeader: TKafkaResponseHeader): TBytes;
var
  R: TKafkaReader;
begin
  R := TKafkaReader.Create(AContent);
  try
    AHeader := KafkaReadResponseHeader(R,
      KafkaResponseHeaderVersion(AKey, KafkaApiIsFlexible(AKey, AVersion)));
    Result := BytesFrom(AContent, R.Position);
  finally
    R.Free;
  end;
end;

function CaptureVersion(const AName: string): Int16;
begin
  Result := KAFKA_CAPTURES[CaptureIndex(AName, True)].ApiVersion;
end;

function CaptureRequestBody(const AName: string): TBytes;
var
  H: TKafkaRequestHeader;
begin
  Result := RequestBody(CaptureBytes(AName, True), H);
end;

function CaptureResponseBody(const AName: string): TBytes;
var
  H: TKafkaResponseHeader;
  I: Integer;
begin
  I := CaptureIndex(AName, False);
  Result := ResponseBody(KafkaCaptureBytes(KAFKA_CAPTURES[I]), KAFKA_CAPTURES[I].ApiKey,
    KAFKA_CAPTURES[I].ApiVersion, H);
end;

function DecodeFetchResponse(const AName: string): TKafkaFetchResponse;
begin
  Result.FromBytes(CaptureResponseBody(AName), CaptureVersion(AName));
end;

function FetchRecords(const AName: string): TBytes;
var
  R: TKafkaFetchResponse;
begin
  R := DecodeFetchResponse(AName);
  Result := R.Responses[0].Partitions[0].Records;
end;

function ProduceRecords(const AName: string): TBytes;
var
  R: TKafkaProduceRequest;
begin
  R.FromBytes(CaptureRequestBody(AName), CaptureVersion(AName));
  Result := R.TopicData[0].PartitionData[0].Records;
end;

function Utf8(const AText: string): TBytes;
begin
  Result := PcStringToUtf8Bytes(AText);
end;

function FromUtf8(const AData: TBytes): string;
begin
  if not PcTryUtf8BytesToString(AData, Result) then
    raise Exception.Create('Not UTF-8: ' + BytesToHex(AData));
end;

end.
