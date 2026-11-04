unit Kafka.Tracing;

{$I kafka.inc}

{ OpenTelemetry spans for sending and consuming records, on pascal-common-faa's
  PascalCommon.Tracing, and the W3C trace context carried in the records'
  headers, as pascal-amqp-faa and pascal-redis-faa do (phase D of
  pascal-api-infra-faa's observability design).

  The producer and the consumer open their spans themselves; the application
  only starts the tracer (TPcTracing.Start). This unit holds the pieces: the
  names, the attributes (OpenTelemetry's messaging conventions, with
  messaging.system kafka), and the injection and reading of traceparent.

  - Send: a producer span "send <topic>", a child of the thread's current
    span, detached (StartDetachedSpan): Send only queues the record, and the
    span ends with the record's outcome, on the sender thread (a span that
    became the current one must end on the thread that started it). Its
    traceparent (and tracestate, when there is one) goes into the record's
    headers as UTF-8 text, in a copy: the caller's array is never changed. A
    record that already has a traceparent header keeps it, and nothing is
    added (the application's context wins, as in pascal-amqp-faa).
  - Poll: a client span "poll <topic>" (receive), the current span while
    Poll runs, so the rebalance and commit handlers called inside it are its
    children. It is exported only when Poll returns records: an empty Poll
    lets it go unfinished, so an idle consumer exports nothing.
  - Process: pascal-common-faa has no span links, so the poll span can't
    point at the producers' spans. The record's own trace is joined by a
    consumer span "process <topic>" whose parent is the record's
    traceparent (StartSpanFromParent), which the application opens around
    its processing (TKafkaConsumer.StartProcessSpan): Poll returns an array,
    and the loop over it is the application's.
  - Disabled costs nothing: with TPcTracing.Enabled false (a flag read
    without a lock) no function here creates a span, an id or an attribute,
    and no header is copied, so the batch's bytes are those of a producer
    without tracing.
  - The key is never an attribute: it is bytes, and may be personal data.
  - Outside the trace: commits, heartbeats, metadata, InitProducerId, and
    retries (a batch sent again carries the headers of its first Send).

  The header's value is the traceparent's ASCII, so its UTF-8 bytes are the
  same on both compilers; reading goes through PcTryUtf8BytesToString, and a
  value that isn't UTF-8 counts as absent. When a record has the header more
  than once, the last one counts (Java's Headers.lastHeader). }

interface

uses
  SysUtils,
  PascalCommon.Tracing,
  Kafka.Protocol.RecordBatch;

const
  KAFKA_TRACEPARENT_HEADER = 'traceparent';
  KAFKA_TRACESTATE_HEADER = 'tracestate';

/// The last traceparent header's text (and the last tracestate's, '' when
/// there is none). False when there is no traceparent, or its value is null
/// or not UTF-8. The format isn't checked: StartSpanFromParent takes an
/// invalid one as absent.
function KafkaTraceParentOf(const AHeaders: TKafkaRecordHeaderArray;
  out ATraceParent, ATraceState: string): Boolean;

/// AHeaders with ASpan's traceparent (and tracestate) added, in a new array;
/// AHeaders itself when ASpan is nil or AHeaders has a traceparent already.
function KafkaInjectTraceContext(const AHeaders: TKafkaRecordHeaderArray;
  const ASpan: IPcSpan): TKafkaRecordHeaderArray;

/// The span of a Send (kind producer, detached, a child of the current
/// span). APartition < 0: not chosen yet. nil when the tracer is off.
function KafkaStartSendSpan(const ATopic: string; APartition: Int32; AValueSize: Int64;
  ATombstone: Boolean; const AClientId: string): IPcSpan;

/// The broker a batch went to, on the span of one of its records. ASpan nil
/// is a no-op.
procedure KafkaSetServer(const ASpan: IPcSpan; const AHost: string; APort: Integer);

/// Ends a Send's span with the record's outcome: the partition, the offset
/// (when >= 0) and, when AErrorType isn't '', the error status. ASpan nil is
/// a no-op.
procedure KafkaFinishSendSpan(const ASpan: IPcSpan; APartition: Int32; AOffset: Int64;
  const AErrorType, AErrorMessage: string);

/// The span of a Poll (kind client), the current span until it ends. nil
/// when the tracer is off.
function KafkaStartPollSpan(const AGroupId, AClientId: string): IPcSpan;

/// Ends a Poll's span: ATopic is the records' topic, '' when they came from
/// more than one. ASpan nil is a no-op.
procedure KafkaFinishPollSpan(const ASpan: IPcSpan; const ATopic: string; ACount: Integer);

/// The span of processing one record (kind consumer), a child of the
/// record's traceparent (a new trace without a valid one), the current span
/// until it ends. AValueSize < 0 for a null value. nil when the tracer is
/// off.
function KafkaStartProcessSpan(const ATopic: string; APartition: Int32; AOffset: Int64;
  const AHeaders: TKafkaRecordHeaderArray; AValueSize: Int64;
  const AGroupId, AClientId: string): IPcSpan;

/// Ends a span; with AError, the error status with its message and
/// error.type with its class. ASpan nil is a no-op.
procedure KafkaFinishSpan(const ASpan: IPcSpan; AError: Exception);

implementation

uses
  PascalCommon.Utf8;

function HeaderText(const AHeader: TKafkaRecordHeader; out AText: string): Boolean;
begin
  AText := '';
  Result := not AHeader.ValueIsNull and PcTryUtf8BytesToString(AHeader.Value, AText);
end;

function KafkaTraceParentOf(const AHeaders: TKafkaRecordHeaderArray;
  out ATraceParent, ATraceState: string): Boolean;
var
  I: Integer;
  LParent, LState: Integer;
begin
  ATraceParent := '';
  ATraceState := '';
  LParent := -1;
  LState := -1;
  for I := 0 to High(AHeaders) do
    if AHeaders[I].Key = KAFKA_TRACEPARENT_HEADER then
      LParent := I
    else if AHeaders[I].Key = KAFKA_TRACESTATE_HEADER then
      LState := I;
  Result := (LParent >= 0) and HeaderText(AHeaders[LParent], ATraceParent);
  if not Result then
  begin
    ATraceParent := '';
    Exit;
  end;
  if (LState >= 0) and not HeaderText(AHeaders[LState], ATraceState) then
    ATraceState := '';
end;

function HasHeader(const AHeaders: TKafkaRecordHeaderArray; const AName: string): Boolean;
var
  I: Integer;
begin
  for I := 0 to High(AHeaders) do
    if AHeaders[I].Key = AName then
      Exit(True);
  Result := False;
end;

function KafkaInjectTraceContext(const AHeaders: TKafkaRecordHeaderArray;
  const ASpan: IPcSpan): TKafkaRecordHeaderArray;
var
  I, N: Integer;
  LState: string;
begin
  if (ASpan = nil) or HasHeader(AHeaders, KAFKA_TRACEPARENT_HEADER) then
    Exit(AHeaders);
  LState := ASpan.TraceState;
  N := Length(AHeaders);
  Result := nil;
  if LState <> '' then
    SetLength(Result, N + 2)
  else
    SetLength(Result, N + 1);
  for I := 0 to N - 1 do
    Result[I] := AHeaders[I];
  Result[N].Key := KAFKA_TRACEPARENT_HEADER;
  Result[N].Value := PcStringToUtf8Bytes(ASpan.TraceParent);
  Result[N].ValueIsNull := False;
  if LState <> '' then
  begin
    Result[N + 1].Key := KAFKA_TRACESTATE_HEADER;
    Result[N + 1].Value := PcStringToUtf8Bytes(LState);
    Result[N + 1].ValueIsNull := False;
  end;
end;

procedure SetCommon(const ASpan: IPcSpan; const AOperationType, AOperationName,
  ATopic, AClientId: string);
begin
  ASpan.SetAttribute('messaging.system', 'kafka');
  ASpan.SetAttribute('messaging.operation.type', AOperationType);
  ASpan.SetAttribute('messaging.operation.name', AOperationName);
  if ATopic <> '' then
    ASpan.SetAttribute('messaging.destination.name', ATopic);
  if AClientId <> '' then
    ASpan.SetAttribute('messaging.client.id', AClientId);
end;

procedure SetValueSize(const ASpan: IPcSpan; AValueSize: Int64; ATombstone: Boolean);
begin
  if ATombstone then
    ASpan.SetBoolAttribute('messaging.kafka.message.tombstone', True)
  else
    ASpan.SetIntAttribute('messaging.message.body.size', AValueSize);
end;

function KafkaStartSendSpan(const ATopic: string; APartition: Int32; AValueSize: Int64;
  ATombstone: Boolean; const AClientId: string): IPcSpan;
begin
  Result := nil;
  if not TPcTracing.Enabled then
    Exit;
  Result := TPcTracing.StartDetachedSpan('send ' + ATopic, skProducer);
  SetCommon(Result, 'send', 'send', ATopic, AClientId);
  if APartition >= 0 then
    Result.SetAttribute('messaging.destination.partition.id', IntToStr(APartition));
  SetValueSize(Result, AValueSize, ATombstone);
end;

procedure KafkaSetServer(const ASpan: IPcSpan; const AHost: string; APort: Integer);
begin
  if ASpan = nil then
    Exit;
  if AHost <> '' then
    ASpan.SetAttribute('server.address', AHost);
  if APort > 0 then
    ASpan.SetIntAttribute('server.port', APort);
end;

procedure KafkaFinishSendSpan(const ASpan: IPcSpan; APartition: Int32; AOffset: Int64;
  const AErrorType, AErrorMessage: string);
begin
  if ASpan = nil then
    Exit;
  if APartition >= 0 then
    ASpan.SetAttribute('messaging.destination.partition.id', IntToStr(APartition));
  if AOffset >= 0 then
    ASpan.SetIntAttribute('messaging.kafka.offset', AOffset);
  if AErrorType <> '' then
  begin
    ASpan.SetAttribute('error.type', AErrorType);
    ASpan.SetStatus(ssError, AErrorMessage);
  end;
  ASpan.Finish;
end;

function KafkaStartPollSpan(const AGroupId, AClientId: string): IPcSpan;
begin
  Result := nil;
  if not TPcTracing.Enabled then
    Exit;
  Result := TPcTracing.StartSpan('poll', skClient);
  SetCommon(Result, 'receive', 'poll', '', AClientId);
  if AGroupId <> '' then
    Result.SetAttribute('messaging.consumer.group.name', AGroupId);
end;

procedure KafkaFinishPollSpan(const ASpan: IPcSpan; const ATopic: string; ACount: Integer);
begin
  if ASpan = nil then
    Exit;
  if ATopic <> '' then
  begin
    ASpan.SetName('poll ' + ATopic);
    ASpan.SetAttribute('messaging.destination.name', ATopic);
  end;
  ASpan.SetIntAttribute('messaging.batch.message_count', ACount);
  ASpan.Finish;
end;

function KafkaStartProcessSpan(const ATopic: string; APartition: Int32; AOffset: Int64;
  const AHeaders: TKafkaRecordHeaderArray; AValueSize: Int64;
  const AGroupId, AClientId: string): IPcSpan;
var
  LParent, LState: string;
begin
  Result := nil;
  if not TPcTracing.Enabled then
    Exit;
  KafkaTraceParentOf(AHeaders, LParent, LState);
  Result := TPcTracing.StartSpanFromParent(LParent, LState, 'process ' + ATopic, skConsumer);
  SetCommon(Result, 'process', 'process', ATopic, AClientId);
  Result.SetAttribute('messaging.destination.partition.id', IntToStr(APartition));
  Result.SetIntAttribute('messaging.kafka.offset', AOffset);
  if AGroupId <> '' then
    Result.SetAttribute('messaging.consumer.group.name', AGroupId);
  SetValueSize(Result, AValueSize, AValueSize < 0);
end;

procedure KafkaFinishSpan(const ASpan: IPcSpan; AError: Exception);
begin
  if ASpan = nil then
    Exit;
  if AError <> nil then
  begin
    ASpan.SetAttribute('error.type', AError.ClassName);
    ASpan.SetStatus(ssError, AError.Message);
  end;
  ASpan.Finish;
end;

end.
