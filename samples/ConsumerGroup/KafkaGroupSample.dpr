program KafkaGroupSample;

{ A consumer in a consumer group (KIP-848) over TLS and SASL: phase K2's
  sample. It joins the group, shows the partitions the broker gives it,
  sends a few records, reads them, commits as it goes (asynchronously, the
  outcome through a callback) and once more at the end (synchronously),
  then closes, which leaves the group.

  Run it twice: the second run starts where the first committed and reads
  only its own records. Run two at once (each with a larger --count) and
  the broker splits the topic's partitions between them; each one prints
  its share, and the one that joins second sees the first one's partitions
  revoked and given to it (a run then waits only for the records sent to
  its own partitions).

  The same source builds on Delphi (KafkaGroupSample.dproj) and on
  FPC/Lazarus (KafkaGroupSample.lpi, or fpc -Mdelphi with the library's
  paths). With no arguments it talks to the SASL_SSL listener of
  docker/docker-compose.yml (localhost:9097, SCRAM-SHA-256, bob) and trusts
  the CA of pascal-common-faa's test PKI. The options, all optional, are
  samples/ProduceConsume's (--bootstrap, --ca, --system-ca, --sasl, --user, --password,
  --password-env, --topic, --count, --no-tls) and --group.

  Delphi x FPC: the handlers are methods of an object ("of object"), not
  closures, which FPC 3.2.2 doesn't have. }

{$APPTYPE CONSOLE}

uses
  {$IFDEF FPC}
  {$IFDEF UNIX}
  cthreads,
  cwstring,
  {$ENDIF}
  {$ENDIF}
  SysUtils,
  PascalCommon.Utf8,
  PascalCommon.Threading,
  Kafka.Types,
  Kafka.Compression.Zstd,
  Kafka.Compression,
  Kafka.Sasl,
  Kafka.Connection,
  Kafka.Group,
  Kafka.Producer,
  Kafka.Consumer;

type
  TOptions = record
    Bootstrap: string;
    UseTls: Boolean;
    CaFile: string;
    Mechanism: TKafkaSaslMechanism;
    User: string;
    Password: string;
    Topic: string;
    Group: string;
    Count: Integer;
  end;

  { The consumer's handlers: they run on the thread that calls Poll (and
    Close), inside those calls. }
  TSampleEvents = class
  public
    Commits: Integer;
    procedure PartitionsAssigned(ASender: TKafkaConsumer; const APartitions: TKafkaTopicPartitionArray);
    procedure PartitionsRevoked(ASender: TKafkaConsumer; const APartitions: TKafkaTopicPartitionArray);
    procedure PartitionsLost(ASender: TKafkaConsumer; const APartitions: TKafkaTopicPartitionArray);
    procedure CommitDone(const AReport: TKafkaCommitReport);
  end;

function PartitionsText(const A: TKafkaTopicPartitionArray): string;
var
  I: Integer;
begin
  Result := '';
  for I := 0 to High(A) do
  begin
    if I > 0 then
      Result := Result + ', ';
    Result := Result + IntToStr(A[I].Partition);
  end;
  if Result = '' then
    Result := 'none new';
end;

procedure TSampleEvents.PartitionsAssigned(ASender: TKafkaConsumer;
  const APartitions: TKafkaTopicPartitionArray);
begin
  System.WriteLn('assigned partitions ', PartitionsText(APartitions));
end;

procedure TSampleEvents.PartitionsRevoked(ASender: TKafkaConsumer;
  const APartitions: TKafkaTopicPartitionArray);
begin
  // With EnableAutoCommit off, this is the place to commit what was
  // processed: the broker gives the partitions to another member only
  // after this returns.
  ASender.Commit;
  System.WriteLn('revoked partitions ', PartitionsText(APartitions), ' (committed first)');
end;

procedure TSampleEvents.PartitionsLost(ASender: TKafkaConsumer;
  const APartitions: TKafkaTopicPartitionArray);
begin
  // Out of the group (Poll not called for max.poll.interval.ms, or fenced):
  // someone else may own them already, so no commit here.
  System.WriteLn('lost partitions ', PartitionsText(APartitions));
end;

procedure TSampleEvents.CommitDone(const AReport: TKafkaCommitReport);
begin
  Inc(Commits);
  if not AReport.Succeeded then
    System.WriteLn('an asynchronous commit failed: ', AReport.ErrorMessage);
end;

function DefaultCaFile: string;
begin
  // The repository's test CA, found from the executable's folder upwards.
  Result := ExtractFileDir(ExpandFileName(ParamStr(0)));
  while (Result <> '') and (Result <> ExtractFileDir(Result)) do
  begin
    if FileExists(IncludeTrailingPathDelimiter(Result) + 'external' + PathDelim +
      'pascal-common-faa' + PathDelim + 'tests' + PathDelim + 'pki' + PathDelim + 'ca_cert.pem') then
      Exit(IncludeTrailingPathDelimiter(Result) + 'external' + PathDelim + 'pascal-common-faa' +
        PathDelim + 'tests' + PathDelim + 'pki' + PathDelim + 'ca_cert.pem');
    Result := ExtractFileDir(Result);
  end;
  Result := '';
end;

function ParseOptions: TOptions;
var
  I: Integer;
  Name, Value: string;
begin
  Result.Bootstrap := 'localhost:9097';
  Result.UseTls := True;
  Result.CaFile := DefaultCaFile;
  Result.Mechanism := ksmScramSha256;
  Result.User := 'bob';
  Result.Password := 'bob-secret';
  Result.Topic := 'pascal-kafka-faa-sample-group';
  Result.Group := 'pascal-kafka-faa-sample';
  Result.Count := 5;
  I := 1;
  while I <= ParamCount do
  begin
    Name := ParamStr(I);
    if Name = '--no-tls' then
    begin
      Result.UseTls := False;
      Inc(I);
      Continue;
    end;
    if Name = '--system-ca' then
    begin
      Result.CaFile := '';
      Inc(I);
      Continue;
    end;
    if I = ParamCount then
      raise Exception.CreateFmt('%s needs a value', [Name]);
    Value := ParamStr(I + 1);
    if Name = '--bootstrap' then
      Result.Bootstrap := Value
    else if Name = '--ca' then
      Result.CaFile := Value
    else if Name = '--sasl' then
    begin
      if SameText(Value, 'plain') then
        Result.Mechanism := ksmPlain
      else if SameText(Value, 'scram-sha-256') then
        Result.Mechanism := ksmScramSha256
      else if SameText(Value, 'scram-sha-512') then
        Result.Mechanism := ksmScramSha512
      else if SameText(Value, 'none') then
        Result.Mechanism := ksmNone
      else
        raise Exception.CreateFmt('Unknown SASL mechanism "%s"', [Value]);
    end
    else if Name = '--user' then
      Result.User := Value
    else if Name = '--password' then
      Result.Password := Value
    else if Name = '--password-env' then
      Result.Password := GetEnvironmentVariable(Value)
    else if Name = '--topic' then
      Result.Topic := Value
    else if Name = '--group' then
      Result.Group := Value
    else if Name = '--count' then
      Result.Count := StrToInt(Value)
    else
      raise Exception.CreateFmt('Unknown option "%s"', [Name]);
    Inc(I, 2);
  end;
end;

function ConnectionTemplate(const O: TOptions): TKafkaConnectionParams;
begin
  Result := KafkaConnectionParams('', 9092);
  Result.ClientId := 'pascal-kafka-faa-group-sample';
  Result.UseTls := O.UseTls;
  Result.TlsCaFile := O.CaFile;           // '' = the system's trust store
  Result.SaslMechanism := O.Mechanism;
  Result.SaslUsername := O.User;
  Result.SaslPassword := O.Password;
end;

function Text(const ABytes: TBytes): string;
begin
  if not PcTryUtf8BytesToString(ABytes, Result) then
    Result := '<not UTF-8>';
end;

function Holds(const AAssignment: TKafkaTopicPartitionArray; APartition: Int32): Boolean;
var
  I: Integer;
begin
  for I := 0 to High(AAssignment) do
    if AAssignment[I].Partition = APartition then
      Exit(True);
  Result := False;
end;

procedure Run(const O: TOptions);
var
  PP: TKafkaProducerParams;
  CP: TKafkaConsumerParams;
  Producer: TKafkaProducer;
  Consumer: TKafkaConsumer;
  Events: TSampleEvents;
  Futures: array of IKafkaDeliveryFuture;
  Sent: array of Int32;
  Held: TKafkaTopicPartitionArray;
  Records: TKafkaConsumerRecordArray;
  RunTag, Value: string;
  I, Expected, Received: Integer;
  Deadline: UInt64;
begin
  System.WriteLn(Format('%s, TLS %s, SASL %s as "%s", topic %s, group %s', [O.Bootstrap,
    BoolToStr(O.UseTls, True), KafkaSaslMechanismName(O.Mechanism), O.User, O.Topic, O.Group]));
  Consumer := nil;
  Events := TSampleEvents.Create;
  PP := KafkaProducerParams(O.Bootstrap);
  PP.Cluster.Connection := ConnectionTemplate(O);
  PP.Compression := kcGzip;
  Producer := TKafkaProducer.Create(PP);
  try
    Producer.Open;
    // A broker that allows it creates the topic on this first request.
    Producer.Cluster.Topic(O.Topic);

    CP := KafkaConsumerParams(O.Bootstrap);
    CP.Cluster.Connection := ConnectionTemplate(O);
    CP.GroupId := O.Group;
    // Commits made by hand here (EnableAutoCommit on would commit on its
    // interval inside Poll, before a revoke and at Close).
    CP.EnableAutoCommit := False;
    // A new group starts at the end of each partition (Java's default).
    CP.AutoOffsetReset := korLatest;
    Consumer := TKafkaConsumer.Create(CP);
    Consumer.OnPartitionsAssigned := Events.PartitionsAssigned;
    Consumer.OnPartitionsRevoked := Events.PartitionsRevoked;
    Consumer.OnPartitionsLost := Events.PartitionsLost;
    Consumer.Open;
    Consumer.Subscribe([O.Topic]);  // the first Poll joins

    // Until the broker gave this member its partitions (two heartbeats at
    // most, 5 s each).
    Deadline := PcTickMs + 30000;
    while (Length(Consumer.Assignment) = 0) and (PcTickMs < Deadline) do
      Consumer.Poll(500);
    Held := Consumer.Assignment;
    if Length(Held) = 0 then
      raise Exception.Create('No partition given in 30 s');
    // Where each partition starts: the group's commit, or the end for a
    // new group. Asked now, so that the records sent next come after it.
    for I := 0 to High(Held) do
      System.WriteLn(Format('partition %d starts at offset %d', [Held[I].Partition,
        Consumer.Position(O.Topic, Held[I].Partition)]));

    RunTag := FormatDateTime('hhnnss', Now);
    SetLength(Futures, O.Count);
    SetLength(Sent, O.Count);
    for I := 0 to O.Count - 1 do
      Futures[I] := Producer.Send(KafkaProducerRecord(O.Topic,
        PcStringToUtf8Bytes('customer-' + IntToStr(I)),
        PcStringToUtf8Bytes(Format('order %d of run %s', [I, RunTag]))));
    Producer.Flush(30000);
    Expected := 0;
    for I := 0 to O.Count - 1 do
    begin
      Sent[I] := Futures[I].Get(0).Partition;
      if Holds(Held, Sent[I]) then
        Inc(Expected);
    end;
    Futures := nil;
    System.WriteLn(Format('sent %d records, %d to this member''s partitions', [O.Count, Expected]));

    Received := 0;
    Deadline := PcTickMs + 30000;
    while (Received < Expected) and (PcTickMs < Deadline) do
    begin
      Records := Consumer.Poll(1000);
      for I := 0 to High(Records) do
      begin
        Value := Text(Records[I].Value);
        System.WriteLn(Format('received partition %d offset %d  %s = %s', [Records[I].Partition,
          Records[I].Offset, Text(Records[I].Key), Value]));
        if Pos('of run ' + RunTag, Value) > 0 then
          Inc(Received);
      end;
      // What this Poll returned is processed: commit it without waiting.
      if Length(Records) > 0 then
        Consumer.CommitAsync(Events.CommitDone);
    end;
    // Waits for the asynchronous commits (their callbacks run here), then
    // commits the positions once more.
    Consumer.Commit;
    System.WriteLn(Format('committed (%d asynchronous commits reported)', [Events.Commits]));
    Consumer.Close;                       // leaves the group
  finally
    Consumer.Free;
    Producer.Free;
    Events.Free;
  end;
  if Received < Expected then
    raise Exception.CreateFmt('Received %d of %d records in 30 s', [Received, Expected]);
end;

begin
  {$IFDEF FPC}
  SetMultiByteConversionCodePage(CP_UTF8);
  {$ELSE}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}
  try
    Run(ParseOptions);
    System.WriteLn('done');
  except
    on E: EKafkaAuthentication do
    begin
      System.WriteLn('authentication failed: ', E.Message);
      ExitCode := 1;
    end;
    on E: Exception do
    begin
      System.WriteLn(E.ClassName, ': ', E.Message);
      ExitCode := 1;
    end;
  end;
end.
