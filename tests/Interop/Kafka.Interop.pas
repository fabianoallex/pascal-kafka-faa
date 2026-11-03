unit Kafka.Interop;

{ The library's side of the interoperability test with the Java client:
  a console program (KafkaInterop.dpr on Delphi, fpc/KafkaInteropFpc.lpr on
  FPC) that tools/interop.py runs between the Java console tools, in both
  directions, over the records of tests/data/interop-records.tsv.

  Why a program and not a fixture of the integration suite: the Java side
  runs in the broker's container (docker exec), and the suite on Linux runs
  in a container of its own with no Docker inside. The script on the host
  drives both; each side checks what it can see.

    KafkaInterop consume <bootstrap> <topic> <fixture>
      Java produced the fixture (the console producer, with gzip, snappy or
      lz4: the script checks the codec in the broker's log); this reads
      every partition from the start and checks each record against its
      line: key (null or bytes), value, headers in order (a null value too),
      a CreateTime timestamp, the partition Java's partitioner chose for a
      keyed record (KafkaPartitionForKey: the same murmur2), and the
      fixture's order within each partition.

    KafkaInterop produce <bootstrap> <topic> <fixture> [<codec>]
      Sends the fixture with <codec> (gzip, snappy or lz4; gzip when
      omitted), acks=all and fixed timestamps
      (KAFKA_INTEROP_BASE_TIMESTAMP + 1000 * n), and prints one line per
      record, "R <n> <partition> <offset>", for the script to compare with
      what the Java console consumer and kafka-dump-log.sh read.

    KafkaInterop group <bootstrap> <topic> <group> <from> <to> <partitions>
      The KIP-848 consumer group shared with the Java consumer 4.4.0
      (group.protocol=consumer), which already holds every partition: this
      subscribes, waits until the broker gave it <partitions> of them (the
      Java member revoked them first), produces the records <from> to
      <to> - 1 (key and value "g <n>", the key the number), reads its
      partitions up to their end, and closes (the auto-commit, the leave).
      It prints "A <p>,<p>..." (its partitions) and "G <n> <partition>" per
      record read, for the script to compare with what Java read.

  The fixture is the Java console producer's own input: "headers TAB key TAB
  value" per line, headers "name:value" separated by commas, NULL for a
  null key or header value. Header "n" numbers the records.

  Prints "OK ..." and exits 0, or "FAIL ..." and exits 1; 2 for a usage
  error. }

interface

const
  /// 2026-01-01T00:00:00Z: the produced records' timestamps start here.
  KAFKA_INTEROP_BASE_TIMESTAMP = Int64(1767225600000);

/// The program: parses ParamStr and returns the exit code.
function KafkaInteropMain: Integer;

implementation

uses
  SysUtils,
  Classes,
  PascalCommon.Utf8,
  PascalCommon.Threading,
  Kafka.Types,
  Kafka.Compression,
  Kafka.Protocol.Primitives,
  Kafka.Protocol.RecordBatch,
  Kafka.Partitioner,
  Kafka.Cluster,
  Kafka.Producer,
  Kafka.Consumer;

type
  EInteropFailure = class(Exception);

  TFixtureRecord = record
    N: Integer;
    Key: TBytes;
    KeyIsNull: Boolean;
    Value: TBytes;
    Headers: TKafkaRecordHeaderArray;
  end;
  TFixture = array of TFixtureRecord;

  TPartsArray = array of string;

function Split(const AText: string; ASeparator: Char): TPartsArray;
var
  I, LStart, N: Integer;
begin
  Result := nil;
  N := 0;
  LStart := 1;
  for I := 1 to Length(AText) + 1 do
    if (I > Length(AText)) or (AText[I] = ASeparator) then
    begin
      SetLength(Result, N + 1);
      Result[N] := Copy(AText, LStart, I - LStart);
      Inc(N);
      LStart := I + 1;
    end;
end;

function ReadFileBytes(const APath: string): TBytes;
var
  S: TFileStream;
begin
  S := TFileStream.Create(APath, fmOpenRead or fmShareDenyWrite);
  try
    Result := nil;
    SetLength(Result, S.Size);
    if S.Size > 0 then
      S.ReadBuffer(Result[0], S.Size);
  finally
    S.Free;
  end;
end;

function LoadFixture(const APath: string): TFixture;
var
  LText: string;
  LLines, LFields, LHeaders: TPartsArray;
  I, K, P: Integer;
  R: TFixtureRecord;
begin
  if not PcTryUtf8BytesToString(ReadFileBytes(APath), LText) then
    raise EInteropFailure.CreateFmt('%s is not UTF-8', [APath]);
  LLines := Split(LText, #10);
  Result := nil;
  for I := 0 to High(LLines) do
  begin
    if LLines[I] = '' then
      Continue;
    LFields := Split(LLines[I], #9);
    if Length(LFields) <> 3 then
      raise EInteropFailure.CreateFmt('%s, line %d: %d fields, expected 3',
        [APath, I + 1, Length(LFields)]);
    R := Default(TFixtureRecord);
    R.N := -1;
    R.KeyIsNull := LFields[1] = 'NULL';
    if not R.KeyIsNull then
      R.Key := PcStringToUtf8Bytes(LFields[1]);
    R.Value := PcStringToUtf8Bytes(LFields[2]);
    LHeaders := Split(LFields[0], ',');
    SetLength(R.Headers, Length(LHeaders));
    for K := 0 to High(LHeaders) do
    begin
      P := Pos(':', LHeaders[K]);
      if P = 0 then
        raise EInteropFailure.CreateFmt('%s, line %d: header "%s" has no ":"',
          [APath, I + 1, LHeaders[K]]);
      R.Headers[K].Key := Copy(LHeaders[K], 1, P - 1);
      R.Headers[K].ValueIsNull := Copy(LHeaders[K], P + 1, MaxInt) = 'NULL';
      if not R.Headers[K].ValueIsNull then
        R.Headers[K].Value := PcStringToUtf8Bytes(Copy(LHeaders[K], P + 1, MaxInt));
      if R.Headers[K].Key = 'n' then
        R.N := StrToInt(Copy(LHeaders[K], P + 1, MaxInt));
    end;
    if R.N <> Length(Result) then
      raise EInteropFailure.CreateFmt('%s, line %d: header n is %d, expected %d',
        [APath, I + 1, R.N, Length(Result)]);
    SetLength(Result, Length(Result) + 1);
    Result[High(Result)] := R;
  end;
end;

function SameBytes(const A, B: TBytes): Boolean;
begin
  Result := (Length(A) = Length(B)) and ((Length(A) = 0) or CompareMem(@A[0], @B[0], Length(A)));
end;

function Shown(const ABytes: TBytes): string;
begin
  if not PcTryUtf8BytesToString(ABytes, Result) then
    Result := '<not UTF-8>';
end;

procedure Check(ACondition: Boolean; const AMessage: string; const AArgs: array of const);
begin
  if not ACondition then
    raise EInteropFailure.CreateFmt(AMessage, AArgs);
end;

function HeaderN(const AHeaders: TKafkaRecordHeaderArray): Integer;
var
  I: Integer;
begin
  for I := 0 to High(AHeaders) do
    if (AHeaders[I].Key = 'n') and not AHeaders[I].ValueIsNull then
      Exit(StrToIntDef(Shown(AHeaders[I].Value), -1));
  Result := -1;
end;

procedure CheckRecord(const AGot: TKafkaConsumerRecord; const AWant: TFixtureRecord;
  APartitions: Integer);
var
  I: Integer;
  LWhere: string;
begin
  LWhere := Format('record n=%d (partition %d, offset %d)', [AWant.N, AGot.Partition, AGot.Offset]);
  Check(AGot.KeyIsNull = AWant.KeyIsNull, '%s: key null %s, expected %s',
    [LWhere, BoolToStr(AGot.KeyIsNull, True), BoolToStr(AWant.KeyIsNull, True)]);
  Check(SameBytes(AGot.Key, AWant.Key), '%s: key "%s", expected "%s"',
    [LWhere, Shown(AGot.Key), Shown(AWant.Key)]);
  Check(not AGot.ValueIsNull and SameBytes(AGot.Value, AWant.Value),
    '%s: value "%s", expected "%s"', [LWhere, Shown(AGot.Value), Shown(AWant.Value)]);
  Check(Length(AGot.Headers) = Length(AWant.Headers), '%s: %d headers, expected %d',
    [LWhere, Length(AGot.Headers), Length(AWant.Headers)]);
  for I := 0 to High(AWant.Headers) do
  begin
    Check(AGot.Headers[I].Key = AWant.Headers[I].Key, '%s: header %d is "%s", expected "%s"',
      [LWhere, I, AGot.Headers[I].Key, AWant.Headers[I].Key]);
    Check((AGot.Headers[I].ValueIsNull = AWant.Headers[I].ValueIsNull) and
      SameBytes(AGot.Headers[I].Value, AWant.Headers[I].Value),
      '%s: header "%s" has "%s", expected "%s"', [LWhere, AWant.Headers[I].Key,
      Shown(AGot.Headers[I].Value), Shown(AWant.Headers[I].Value)]);
  end;
  Check(AGot.TimestampType = kttCreateTime, '%s: not CreateTime', [LWhere]);
  Check(AGot.Timestamp > KAFKA_INTEROP_BASE_TIMESTAMP, '%s: timestamp %d', [LWhere, AGot.Timestamp]);
  if not AWant.KeyIsNull then
    Check(AGot.Partition = KafkaPartitionForKey(AWant.Key, APartitions),
      '%s: Java put it in partition %d, murmur2 says %d',
      [LWhere, AGot.Partition, KafkaPartitionForKey(AWant.Key, APartitions)]);
end;

function RunConsume(const ABootstrap, ATopic: string; const AFixture: TFixture): string;
var
  P: TKafkaConsumerParams;
  C: TKafkaConsumer;
  LParts: array of TKafkaTopicPartition;
  LSeen: array of Boolean;
  LLastN: array of Integer;
  LGot: TKafkaConsumerRecordArray;
  LPartitions, I, N, LCount: Integer;
  LDeadline: UInt64;
begin
  P := KafkaConsumerParams(ABootstrap);
  P.Cluster.Connection.ClientId := 'pascal-kafka-faa-interop';
  P.AutoOffsetReset := korEarliest;
  P.FetchMaxWaitMs := 200;
  C := TKafkaConsumer.Create(P);
  try
    C.Open;
    LPartitions := Length(C.Cluster.Topic(ATopic).Partitions);
    SetLength(LParts, LPartitions);
    SetLength(LLastN, LPartitions);
    for I := 0 to LPartitions - 1 do
    begin
      LParts[I] := KafkaTopicPartition(ATopic, I);
      LLastN[I] := -1;
    end;
    C.Assign(LParts);
    SetLength(LSeen, Length(AFixture));
    LCount := 0;
    LDeadline := PcTickMs + 30000;
    while (LCount < Length(AFixture)) and (PcTickMs < LDeadline) do
    begin
      LGot := C.Poll(1000);
      for I := 0 to High(LGot) do
      begin
        N := HeaderN(LGot[I].Headers);
        Check((N >= 0) and (N < Length(AFixture)), 'a record without a known n (partition %d, ' +
          'offset %d)', [LGot[I].Partition, LGot[I].Offset]);
        Check(not LSeen[N], 'record n=%d twice', [N]);
        CheckRecord(LGot[I], AFixture[N], LPartitions);
        // Java sends in the fixture's order: so is each partition.
        Check(N > LLastN[LGot[I].Partition], 'record n=%d after n=%d in partition %d',
          [N, LLastN[LGot[I].Partition], LGot[I].Partition]);
        LLastN[LGot[I].Partition] := N;
        LSeen[N] := True;
        Inc(LCount);
      end;
    end;
    Check(LCount = Length(AFixture), '%d of %d records in 30 s', [LCount, Length(AFixture)]);
    Result := Format('OK java->pascal: %d records on %d partitions, each as the fixture says, ' +
      'keyed ones in the partition of murmur2', [LCount, LPartitions]);
  finally
    C.Free;
  end;
end;

function CodecNamed(const AName: string): TKafkaCompression;
var
  C: TKafkaCompression;
begin
  for C := Low(TKafkaCompression) to High(TKafkaCompression) do
    if KafkaCompressionName(C) = AName then
      Exit(C);
  raise EInteropFailure.CreateFmt('unknown codec "%s"', [AName]);
end;

function RunProduce(const ABootstrap, ATopic: string; const AFixture: TFixture;
  ACodec: TKafkaCompression): string;
var
  P: TKafkaProducerParams;
  Pr: TKafkaProducer;
  F: array of IKafkaDeliveryFuture;
  R: TKafkaProducerRecord;
  Rep: TKafkaDeliveryReport;
  I: Integer;
begin
  P := KafkaProducerParams(ABootstrap);
  P.Cluster.Connection.ClientId := 'pascal-kafka-faa-interop';
  P.Compression := ACodec;
  P.Acks := kaAll;
  P.LingerMs := 50;
  Pr := TKafkaProducer.Create(P);
  try
    Pr.Open;
    SetLength(F, Length(AFixture));
    for I := 0 to High(AFixture) do
    begin
      R := Default(TKafkaProducerRecord);
      R.Topic := ATopic;
      R.Partition := KAFKA_ANY_PARTITION;
      R.Key := AFixture[I].Key;
      R.KeyIsNull := AFixture[I].KeyIsNull;
      R.Value := AFixture[I].Value;
      R.Headers := AFixture[I].Headers;
      R.Timestamp := KAFKA_INTEROP_BASE_TIMESTAMP + 1000 * I;
      F[I] := Pr.Send(R);
    end;
    Check(Pr.Flush(60000), 'not flushed in 60 s', []);
    for I := 0 to High(F) do
    begin
      Rep := F[I].Get(0);
      System.WriteLn(Format('R %d %d %d', [I, Rep.Partition, Rep.Offset]));
    end;
    F := nil;
    Result := Format('OK pascal produced %d records with %s', [Length(AFixture),
      KafkaCompressionName(ACodec)]);
  finally
    Pr.Free;
  end;
end;

function GroupNumber(const ARecord: TKafkaConsumerRecord): Integer;
var
  LText: string;
begin
  if not PcTryUtf8BytesToString(ARecord.Value, LText) or (Copy(LText, 1, 2) <> 'g ') or
    not TryStrToInt(Copy(LText, 3, MaxInt), Result) then
    raise EInteropFailure.CreateFmt('partition %d, offset %d: not a record of this test',
      [ARecord.Partition, ARecord.Offset]);
end;

function PartitionList(const A: TKafkaTopicPartitionArray): string;
var
  I: Integer;
begin
  Result := '';
  for I := 0 to High(A) do
  begin
    if I > 0 then
      Result := Result + ',';
    Result := Result + IntToStr(A[I].Partition);
  end;
end;

function RunGroup(const ABootstrap, ATopic, AGroup: string; AFrom, ATo,
  AWant: Integer): string;
var
  P: TKafkaConsumerParams;
  PP: TKafkaProducerParams;
  C: TKafkaConsumer;
  Pr: TKafkaProducer;
  LGot: TKafkaConsumerRecordArray;
  LHeld: TKafkaTopicPartitionArray;
  LEnds: TKafkaInt64Array;
  LDeadline: UInt64;
  I, LCount: Integer;
  LDone: Boolean;
  LText: string;

  procedure Take(const ARecords: TKafkaConsumerRecordArray);
  var
    K: Integer;
  begin
    for K := 0 to High(ARecords) do
    begin
      System.WriteLn(Format('G %d %d', [GroupNumber(ARecords[K]), ARecords[K].Partition]));
      Inc(LCount);
    end;
  end;

begin
  LCount := 0;
  P := KafkaConsumerParams(ABootstrap);
  P.Cluster.Connection.ClientId := 'pascal-kafka-faa-interop-group';
  P.GroupId := AGroup;
  P.AutoOffsetReset := korEarliest;
  P.FetchMaxWaitMs := 200;
  PP := KafkaProducerParams(ABootstrap);
  PP.Cluster.Connection.ClientId := 'pascal-kafka-faa-interop-group';
  Pr := nil;
  C := TKafkaConsumer.Create(P);
  try
    C.Open;
    C.Subscribe([ATopic]);
    // The broker's uniform assignor splits the partitions between the two
    // members once the Java one has revoked them (two heartbeats of 5 s).
    LDeadline := PcTickMs + 60000;
    while (Length(C.Assignment) <> AWant) and (PcTickMs < LDeadline) do
      Take(C.Poll(500));
    Check(Length(C.Assignment) = AWant, 'given %d partitions in 60 s, not %d',
      [Length(C.Assignment), AWant]);
    LHeld := C.Assignment;
    Pr := TKafkaProducer.Create(PP);
    Pr.Open;
    for I := AFrom to ATo - 1 do
      Pr.Send(KafkaProducerRecord(ATopic, PcStringToUtf8Bytes(IntToStr(I)),
        PcStringToUtf8Bytes('g ' + IntToStr(I))));
    Check(Pr.Flush(60000), 'not flushed in 60 s', []);
    // Its partitions up to their end.
    LEnds := C.EndOffsets(LHeld);
    LDeadline := PcTickMs + 60000;
    repeat
      Take(C.Poll(500));
      Check(PartitionList(C.Assignment) = PartitionList(LHeld),
        'the assignment changed while reading: %s, then %s',
        [PartitionList(LHeld), PartitionList(C.Assignment)]);
      LDone := True;
      for I := 0 to High(LHeld) do
        if C.Position(LHeld[I].Topic, LHeld[I].Partition) < LEnds[I] then
          LDone := False;
    until LDone or (PcTickMs >= LDeadline);
    Check(LDone, 'its partitions were not read to their end in 60 s', []);
    LText := PartitionList(LHeld);
    C.Close; // the auto-commit, then the leave
    System.WriteLn('A ' + LText);
    Result := Format('OK pascal group: given partitions %s next to the Java member, read %d ' +
      'records, committed and left', [LText, LCount]);
  finally
    Pr.Free;
    C.Free;
  end;
end;

function KafkaInteropMain: Integer;
var
  LMode: string;
begin
  LMode := ParamStr(1);
  if not (((ParamCount = 4) and (LMode = 'consume')) or
    (((ParamCount = 4) or (ParamCount = 5)) and (LMode = 'produce')) or
    ((ParamCount = 7) and (LMode = 'group'))) then
  begin
    System.WriteLn('usage: KafkaInterop consume <bootstrap> <topic> <fixture>');
    System.WriteLn('       KafkaInterop produce <bootstrap> <topic> <fixture> [gzip|snappy|lz4]');
    System.WriteLn('       KafkaInterop group <bootstrap> <topic> <group> <from> <to> <partitions>');
    Exit(2);
  end;
  try
    if LMode = 'group' then
      System.WriteLn(RunGroup(ParamStr(2), ParamStr(3), ParamStr(4), StrToInt(ParamStr(5)),
        StrToInt(ParamStr(6)), StrToInt(ParamStr(7))))
    else if LMode = 'consume' then
      System.WriteLn(RunConsume(ParamStr(2), ParamStr(3), LoadFixture(ParamStr(4))))
    else if (LMode = 'produce') and (ParamCount = 5) then
      System.WriteLn(RunProduce(ParamStr(2), ParamStr(3), LoadFixture(ParamStr(4)),
        CodecNamed(ParamStr(5))))
    else if LMode = 'produce' then
      System.WriteLn(RunProduce(ParamStr(2), ParamStr(3), LoadFixture(ParamStr(4)), kcGzip))
    else
    begin
      System.WriteLn('unknown mode: ', LMode);
      Exit(2);
    end;
    Result := 0;
  except
    on E: Exception do
    begin
      System.WriteLn('FAIL ', E.ClassName, ': ', E.Message);
      Result := 1;
    end;
  end;
end;

end.
