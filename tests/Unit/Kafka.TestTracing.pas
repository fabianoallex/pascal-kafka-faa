unit Kafka.TestTracing;

{ Test helpers for spans and metrics: an exporter that keeps the spans it
  gets, the tracer started and stopped around a test, and a metric's value
  read from the process-wide PcMetrics registry.

  The tracer is pascal-common-faa's process-wide TPcTracing, started without
  its export thread (AAutoFlush False): a test calls Spans, which exports
  what is queued on the calling thread and returns everything seen so far.
  The registry is process-wide too, and never drops a series, so a metric
  test compares a value with the one read before it acted (a delta), or
  uses a label value no other test uses (a unique topic or group).

  Shared by the unit and integration suites (not mirrored: the name doesn't
  end in Tests). }

interface

uses
  SysUtils,
  SyncObjs,
  PascalCommon.Tracing,
  PascalCommon.Metrics;

type
  TKafkaTestSpanExporter = class(TInterfacedObject, IPcSpanExporter)
  private
    FLock: TCriticalSection;
    FSpans: TPcSpanDataArray;
  public
    constructor Create;
    destructor Destroy; override;
    function ExportSpans(const ASpans: TPcSpanDataArray; out AError: string): Boolean;
    /// Flushes the tracer's queue (on this thread), then every span so far.
    function Spans: TPcSpanDataArray;
  end;

/// Starts the tracer (every trace sampled, no export thread) with a new
/// exporter, which it returns.
function KafkaTestStartTracing: TKafkaTestSpanExporter;
/// Shuts the tracer down.
procedure KafkaTestStopTracing;

/// The index of the last span named AName, -1 when none.
function KafkaTestFindSpan(const ASpans: TPcSpanDataArray; const AName: string): Integer;
/// How many spans are named AName.
function KafkaTestCountSpans(const ASpans: TPcSpanDataArray; const AName: string): Integer;
/// An attribute as text ('' when absent): strings as they are, integers and
/// booleans written out ('42', 'true').
function KafkaTestAttribute(const ASpan: TPcSpanData; const AKey: string): string;

/// The value of the series of metric AName with these label values, 0 when
/// the metric or the series doesn't exist.
function KafkaTestMetric(const AName: string; const ALabels: array of string): Double;

implementation

constructor TKafkaTestSpanExporter.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
end;

destructor TKafkaTestSpanExporter.Destroy;
begin
  FLock.Free;
  inherited;
end;

function TKafkaTestSpanExporter.ExportSpans(const ASpans: TPcSpanDataArray;
  out AError: string): Boolean;
var
  I, N: Integer;
begin
  AError := '';
  FLock.Enter;
  try
    N := Length(FSpans);
    SetLength(FSpans, N + Length(ASpans));
    for I := 0 to High(ASpans) do
      FSpans[N + I] := ASpans[I];
  finally
    FLock.Leave;
  end;
  Result := True;
end;

function TKafkaTestSpanExporter.Spans: TPcSpanDataArray;
begin
  TPcTracing.FlushNow;
  FLock.Enter;
  try
    Result := Copy(FSpans, 0, Length(FSpans));
  finally
    FLock.Leave;
  end;
end;

function KafkaTestStartTracing: TKafkaTestSpanExporter;
var
  LOptions: TPcTracingOptions;
begin
  LOptions := TPcTracingOptions.Default('pascal-kafka-faa-tests', '1');
  LOptions.SampleRatio := 1;
  Result := TKafkaTestSpanExporter.Create;
  TPcTracing.Start(LOptions, Result, False);
end;

procedure KafkaTestStopTracing;
begin
  TPcTracing.Shutdown;
end;

function KafkaTestFindSpan(const ASpans: TPcSpanDataArray; const AName: string): Integer;
var
  I: Integer;
begin
  for I := High(ASpans) downto 0 do
    if ASpans[I].Name = AName then
      Exit(I);
  Result := -1;
end;

function KafkaTestCountSpans(const ASpans: TPcSpanDataArray; const AName: string): Integer;
var
  I: Integer;
begin
  Result := 0;
  for I := 0 to High(ASpans) do
    if ASpans[I].Name = AName then
      Inc(Result);
end;

function KafkaTestAttribute(const ASpan: TPcSpanData; const AKey: string): string;
var
  I: Integer;
begin
  for I := 0 to High(ASpan.Attributes) do
    if ASpan.Attributes[I].Key = AKey then
      case ASpan.Attributes[I].ValueType of
        satString: Exit(ASpan.Attributes[I].StringValue);
        satInt: Exit(IntToStr(ASpan.Attributes[I].IntValue));
        satBool:
          if ASpan.Attributes[I].BoolValue then
            Exit('true')
          else
            Exit('false');
      else
        Exit(FloatToStr(ASpan.Attributes[I].DoubleValue));
      end;
  Result := '';
end;

function KafkaTestMetric(const AName: string; const ALabels: array of string): Double;
var
  LMetrics: TPcMetricList;
  LPoints: TPcMetricPoints;
  I, P, L: Integer;
  LMatch: Boolean;
begin
  Result := 0;
  LMetrics := PcMetrics.Metrics;
  for I := 0 to High(LMetrics) do
    if LMetrics[I].Name = AName then
    begin
      LPoints := LMetrics[I].Snapshot;
      for P := 0 to High(LPoints) do
      begin
        if Length(LPoints[P].LabelValues) <> Length(ALabels) then
          Continue;
        LMatch := True;
        for L := 0 to High(ALabels) do
          if LPoints[P].LabelValues[L] <> ALabels[L] then
            LMatch := False;
        if LMatch then
          Exit(LPoints[P].Value);
      end;
      Exit;
    end;
end;

end.
