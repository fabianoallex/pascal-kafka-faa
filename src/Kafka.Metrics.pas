unit Kafka.Metrics;

{$I kafka.inc}

{ The producer's and the consumer's metrics, on pascal-common-faa's
  process-wide PcMetrics registry, as pascal-db-faa's pool records its own.

  Names: OpenTelemetry's messaging conventions where there is one
  (messaging.client.sent.messages, messaging.client.consumed.messages, with
  messaging.system kafka), the library's own under pascalkafka.* otherwise:

    messaging.client.sent.messages      counter    a record's outcome, with
                                                    error.type on a failure
    pascalkafka.producer.bytes          counter    a batch's bytes as written
                                                    (compressed), once, when
                                                    the broker took it
    pascalkafka.producer.retries        counter    records sent again
    pascalkafka.producer.batches.in_flight  up-down  batches in Produce
                                                    requests not answered
    pascalkafka.producer.epoch_bumps    counter    the idempotent producer's
    messaging.client.consumed.messages  counter    records Poll returned
    pascalkafka.consumer.lag            up-down    per partition: the high
                                                    watermark minus the
                                                    position
    pascalkafka.consumer.commits        counter    by kind (sync, async,
                                                    auto) and error.type
    pascalkafka.consumer.rebalances     counter    by event (assigned,
                                                    revoked, lost)

  The families are registered in initialization (no lazily created shared
  state). A producer or consumer looks a series up once (per partition, the
  first time it sees it) and keeps it; an update is a lock-free atomic
  operation, made whether or not anything reads the metrics, and never per
  record: a batch's records are added at once.

  Everything that rises and falls goes as deltas: the registry never drops a
  series, so a partition revoked gives its lag back (its series reads 0
  again), a producer closed gives back its batches in flight, and two clients
  in the process add up instead of overwriting each other. No label names the
  client: the series are per topic, partition and group, a bounded set.

  error.type is the Kafka error's name for a broker's error
  (NOT_LEADER_OR_FOLLOWER), the exception's class otherwise; '' (no label,
  for Prometheus) on success.

  The Kafka* functions return nil, and the Add functions do nothing, past
  this unit's finalization: a client freed by a unit finalized later records
  nothing instead of touching freed families. }

interface

uses
  SysUtils,
  PascalCommon.Metrics,
  Kafka.Types;

/// The error.type of a failure: the Kafka error's name for a broker's code,
/// the class's name otherwise; '' when AClass is nil.
function KafkaErrorType(AClass: EKafkaClass; ACode: Int16): string;

function KafkaSentMessagesSeries(const ATopic, AErrorType: string): TPcCounterSeries;
function KafkaProducerBytesSeries(const ATopic: string): TPcCounterSeries;
function KafkaProducerRetriesSeries(const ATopic: string): TPcCounterSeries;
function KafkaConsumedMessagesSeries(const ATopic, AGroupId: string): TPcCounterSeries;
function KafkaConsumerLagSeries(const ATopic: string; APartition: Int32;
  const AGroupId: string): TPcUpDownCounterSeries;
/// AKind: 'sync', 'async' or 'auto'.
function KafkaCommitsSeries(const AGroupId, AKind, AErrorType: string): TPcCounterSeries;
/// AEvent: 'assigned', 'revoked' or 'lost'.
function KafkaRebalancesSeries(const AGroupId, AEvent: string): TPcCounterSeries;

/// The no-label families' updates.
procedure KafkaAddBatchesInFlight(ADelta: Integer);
procedure KafkaAddEpochBump;

/// Add to a series kept by a client; nil or past finalization: nothing.
procedure KafkaMetricAdd(ASeries: TPcCounterSeries; AValue: Double); overload;
procedure KafkaMetricAdd(ASeries: TPcUpDownCounterSeries; AValue: Double); overload;

implementation

uses
  Kafka.Errors;

var
  GSent: TPcCounter;
  GBytes: TPcCounter;
  GRetries: TPcCounter;
  GInFlight: TPcUpDownCounter;
  GEpochBumps: TPcCounter;
  GConsumed: TPcCounter;
  GLag: TPcUpDownCounter;
  GCommits: TPcCounter;
  GRebalances: TPcCounter;
  GAlive: Boolean;

function KafkaErrorType(AClass: EKafkaClass; ACode: Int16): string;
begin
  if AClass = nil then
    Result := ''
  else if (ACode <> 0) and AClass.InheritsFrom(EKafkaBrokerError) then
    Result := KafkaErrorName(ACode)
  else
    Result := AClass.ClassName;
end;

function Alive: Boolean;
begin
  Result := GAlive and Assigned(PcMetrics);
end;

function KafkaSentMessagesSeries(const ATopic, AErrorType: string): TPcCounterSeries;
begin
  Result := nil;
  if Alive then
    Result := GSent.Labels(['kafka', ATopic, AErrorType]);
end;

function KafkaProducerBytesSeries(const ATopic: string): TPcCounterSeries;
begin
  Result := nil;
  if Alive then
    Result := GBytes.Labels([ATopic]);
end;

function KafkaProducerRetriesSeries(const ATopic: string): TPcCounterSeries;
begin
  Result := nil;
  if Alive then
    Result := GRetries.Labels([ATopic]);
end;

function KafkaConsumedMessagesSeries(const ATopic, AGroupId: string): TPcCounterSeries;
begin
  Result := nil;
  if Alive then
    Result := GConsumed.Labels(['kafka', ATopic, AGroupId]);
end;

function KafkaConsumerLagSeries(const ATopic: string; APartition: Int32;
  const AGroupId: string): TPcUpDownCounterSeries;
begin
  Result := nil;
  if Alive then
    Result := GLag.Labels([ATopic, IntToStr(APartition), AGroupId]);
end;

function KafkaCommitsSeries(const AGroupId, AKind, AErrorType: string): TPcCounterSeries;
begin
  Result := nil;
  if Alive then
    Result := GCommits.Labels([AGroupId, AKind, AErrorType]);
end;

function KafkaRebalancesSeries(const AGroupId, AEvent: string): TPcCounterSeries;
begin
  Result := nil;
  if Alive then
    Result := GRebalances.Labels([AGroupId, AEvent]);
end;

procedure KafkaAddBatchesInFlight(ADelta: Integer);
begin
  if Alive and (ADelta <> 0) then
    GInFlight.Add(ADelta);
end;

procedure KafkaAddEpochBump;
begin
  if Alive then
    GEpochBumps.Inc;
end;

procedure KafkaMetricAdd(ASeries: TPcCounterSeries; AValue: Double);
begin
  if Assigned(ASeries) and Alive then
    ASeries.Add(AValue);
end;

procedure KafkaMetricAdd(ASeries: TPcUpDownCounterSeries; AValue: Double);
begin
  if Assigned(ASeries) and Alive and (AValue <> 0) then
    ASeries.Add(AValue);
end;

initialization
  GSent := PcMetrics.Counter('messaging.client.sent.messages', '{message}',
    'Number of messages the producer attempted to send to the broker, by outcome.',
    ['messaging.system', 'messaging.destination.name', 'error.type']);
  GBytes := PcMetrics.Counter('pascalkafka.producer.bytes', 'By',
    'Bytes of record batches the broker took, as written (compressed).',
    ['messaging.destination.name']);
  GRetries := PcMetrics.Counter('pascalkafka.producer.retries', '{record}',
    'Records sent again after a retriable error.',
    ['messaging.destination.name']);
  GInFlight := PcMetrics.UpDownCounter('pascalkafka.producer.batches.in_flight', '{batch}',
    'Record batches in Produce requests waiting for an answer.', []);
  GEpochBumps := PcMetrics.Counter('pascalkafka.producer.epoch_bumps', '{bump}',
    'Epoch bumps of the idempotent producer.', []);
  GConsumed := PcMetrics.Counter('messaging.client.consumed.messages', '{message}',
    'Number of messages that were delivered to the application.',
    ['messaging.system', 'messaging.destination.name', 'messaging.consumer.group.name']);
  GLag := PcMetrics.UpDownCounter('pascalkafka.consumer.lag', '{record}',
    'Records between the consumer''s position and the partition''s high watermark.',
    ['messaging.destination.name', 'messaging.destination.partition.id',
     'messaging.consumer.group.name']);
  GCommits := PcMetrics.Counter('pascalkafka.consumer.commits', '{commit}',
    'Offset commits by kind (sync, async, auto) and outcome.',
    ['messaging.consumer.group.name', 'pascalkafka.commit.kind', 'error.type']);
  GRebalances := PcMetrics.Counter('pascalkafka.consumer.rebalances', '{event}',
    'Rebalance events the consumer applied (assigned, revoked, lost).',
    ['messaging.consumer.group.name', 'pascalkafka.rebalance.event']);
  GAlive := True;

finalization
  GAlive := False;

end.
