program KafkaSample;

{ A producer and a consumer over TLS and SASL: phase K1's sample
  (docs/design.md, section 7). It sends a few keyed records with a header,
  then reads them back from the partitions they went to, and prints both
  sides.

  The same source builds on Delphi (KafkaSample.dproj) and on FPC/Lazarus
  (KafkaSample.lpi, or fpc -Mdelphi with the library's paths).

  With no arguments it talks to the SASL_SSL listener of
  docker/docker-compose.yml (localhost:9097, SCRAM-SHA-256, bob) and trusts
  the CA of pascal-common-faa's test PKI. Against Confluent Cloud:

    KafkaSample --bootstrap pkc-xxxxx.region.provider.confluent.cloud:9092
      --sasl plain --user <API key> --password-env CC_SECRET --system-ca
      --topic <an existing topic>

  --system-ca trusts the system's store, as for any public broker. It is a
  flag, not --ca with an empty value: Delphi's ParamStr drops an empty
  argument (FPC keeps it), so --ca "" took the next option as its value on
  Delphi. The password
  comes from an environment variable (--password-env), so it doesn't land in
  the shell's history; --password exists for the lab.

  OAUTHBEARER: --sasl oauthbearer --token-env NAME. The token is whatever
  the environment variable NAME holds when a connection authenticates:
  TEnvTokenProvider.Token is the TKafkaOAuthBearerTokenProvider, called at
  each new connection. An application's provider would hand out the token
  it got from its issuer and fetch the next one before that one expires.
  --extension name=value (repeatable) adds a SASL extension. Against the
  OAUTH listener of docker/docker-compose.yml (9099, Kafka's unsecured
  validator, test only), with a token as tests/Unit/Kafka.TestHelpers
  writes them (KafkaTestUnsecuredJws: sub, scope kafka, exp):

    KafkaSample --bootstrap localhost:9099 --no-tls --sasl oauthbearer
      --token-env KAFKA_TOKEN

  The options, all optional: --bootstrap, --ca, --system-ca, --sasl plain|scram-sha-256|
  scram-sha-512|oauthbearer|none, --user, --password, --password-env,
  --token-env, --extension, --topic, --count, --no-tls. }

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
  Kafka.Protocol.Primitives,
  Kafka.Compression.Zstd,
  Kafka.Compression,
  Kafka.Sasl,
  Kafka.Connection,
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
    TokenEnv: string;
    Extensions: TKafkaSaslExtensionArray;
    Topic: string;
    Count: Integer;
  end;

  { OAUTHBEARER's token provider: the token is read from an environment
    variable at each authentication. Thread-safe as it is: it keeps nothing. }
  TEnvTokenProvider = class
  private
    FName: string;
  public
    constructor Create(const AName: string);
    function Token: string;
  end;

constructor TEnvTokenProvider.Create(const AName: string);
begin
  inherited Create;
  FName := AName;
end;

function TEnvTokenProvider.Token: string;
begin
  Result := GetEnvironmentVariable(FName);
  if Result = '' then
    raise Exception.CreateFmt('the environment variable %s holds no token', [FName]);
end;

var
  Options: TOptions;
  TokenProvider: TEnvTokenProvider;

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
  Result.TokenEnv := '';
  Result.Extensions := nil;
  Result.Topic := 'pascal-kafka-faa-sample';
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
      else if SameText(Value, 'oauthbearer') then
        Result.Mechanism := ksmOAuthBearer
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
    else if Name = '--token-env' then
      Result.TokenEnv := Value
    else if Name = '--extension' then
    begin
      if Pos('=', Value) < 2 then
        raise Exception.CreateFmt('--extension "%s": name=value expected', [Value]);
      SetLength(Result.Extensions, Length(Result.Extensions) + 1);
      Result.Extensions[High(Result.Extensions)] := KafkaSaslExtension(
        Copy(Value, 1, Pos('=', Value) - 1), Copy(Value, Pos('=', Value) + 1, MaxInt));
    end
    else if Name = '--topic' then
      Result.Topic := Value
    else if Name = '--count' then
      Result.Count := StrToInt(Value)
    else
      raise Exception.CreateFmt('Unknown option "%s"', [Name]);
    Inc(I, 2);
  end;
  if Result.Mechanism = ksmOAuthBearer then
  begin
    if Result.TokenEnv = '' then
      raise Exception.Create('--sasl oauthbearer needs --token-env');
    Result.User := '(token from ' + Result.TokenEnv + ')';
  end;
end;

/// What every connection of the producer and the consumer uses: Host and
/// Port come from the bootstrap and then from each broker's metadata.
function ConnectionTemplate(const O: TOptions): TKafkaConnectionParams;
begin
  Result := KafkaConnectionParams('', 9092);
  Result.ClientId := 'pascal-kafka-faa-sample';
  Result.UseTls := O.UseTls;
  Result.TlsCaFile := O.CaFile;           // '' = the system's trust store
  Result.SaslMechanism := O.Mechanism;
  Result.SaslUsername := O.User;
  Result.SaslPassword := O.Password;
  if O.Mechanism = ksmOAuthBearer then
  begin
    Result.SaslOAuthBearerTokenProvider := TokenProvider.Token;
    Result.SaslOAuthBearerExtensions := O.Extensions;
  end;
end;

function Text(const ABytes: TBytes): string;
begin
  if not PcTryUtf8BytesToString(ABytes, Result) then
    Result := '<not UTF-8>';
end;

procedure Run(const O: TOptions);
var
  PP: TKafkaProducerParams;
  CP: TKafkaConsumerParams;
  Producer: TKafkaProducer;
  Consumer: TKafkaConsumer;
  Partitions: array of TKafkaTopicPartition;
  Ends: TKafkaInt64Array;
  Futures: array of IKafkaDeliveryFuture;
  Rec: TKafkaProducerRecord;
  Report: TKafkaDeliveryReport;
  Records: TKafkaConsumerRecordArray;
  Tag: string;
  I, Received: Integer;
  Deadline: UInt64;
begin
  System.WriteLn(Format('%s, TLS %s, SASL %s as "%s", topic %s', [O.Bootstrap,
    BoolToStr(O.UseTls, True), KafkaSaslMechanismName(O.Mechanism), O.User, O.Topic]));
  Consumer := nil;
  PP := KafkaProducerParams(O.Bootstrap);   // Java's defaults: acks=all, linger.ms=5...
  PP.Cluster.Connection := ConnectionTemplate(O);
  PP.Compression := kcGzip;
  Producer := TKafkaProducer.Create(PP);
  try
    Producer.Open;
    // The topic's partitions (a broker that allows it creates the topic on
    // this first request; Confluent Cloud doesn't: create it beforehand).
    SetLength(Partitions, Length(Producer.Cluster.Topic(O.Topic).Partitions));

    // The consumer at the end of every partition: it reads only what this
    // run sends.
    CP := KafkaConsumerParams(O.Bootstrap);
    CP.Cluster.Connection := ConnectionTemplate(O);
    Consumer := TKafkaConsumer.Create(CP);
    Consumer.Open;
    for I := 0 to High(Partitions) do
      Partitions[I] := KafkaTopicPartition(O.Topic, I);
    Consumer.Assign(Partitions);
    Ends := Consumer.EndOffsets(Partitions);
    for I := 0 to High(Partitions) do
      Consumer.Seek(O.Topic, I, Ends[I]);

    Tag := FormatDateTime('hhnnss', Now);
    SetLength(Futures, O.Count);
    for I := 0 to O.Count - 1 do
    begin
      Rec := KafkaProducerRecord(O.Topic, PcStringToUtf8Bytes('customer-' + IntToStr(I mod 3)),
        PcStringToUtf8Bytes(Format('order %d of run %s', [I, Tag])));
      SetLength(Rec.Headers, 1);
      Rec.Headers[0].Key := 'run';
      Rec.Headers[0].Value := PcStringToUtf8Bytes(Tag);
      Futures[I] := Producer.Send(Rec);   // queued; the sender thread sends it
    end;
    Producer.Flush(30000);
    for I := 0 to O.Count - 1 do
    begin
      Report := Futures[I].Get(0);        // raises the record's failure, if any
      System.WriteLn(Format('sent     partition %d offset %d', [Report.Partition, Report.Offset]));
    end;
    Futures := nil;

    Received := 0;
    Deadline := PcTickMs + 30000;
    while (Received < O.Count) and (PcTickMs < Deadline) do
    begin
      Records := Consumer.Poll(1000);
      for I := 0 to High(Records) do
      begin
        System.WriteLn(Format('received partition %d offset %d  %s = %s', [Records[I].Partition,
          Records[I].Offset, Text(Records[I].Key), Text(Records[I].Value)]));
        Inc(Received);
      end;
    end;
  finally
    Consumer.Free;
    Producer.Free;                        // Close: flushes, then stops
  end;
  if Received < O.Count then
    raise Exception.CreateFmt('Received %d of %d records in 30 s', [Received, O.Count]);
end;

begin
  {$IFDEF FPC}
  SetMultiByteConversionCodePage(CP_UTF8);
  {$ELSE}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}
  try
    Options := ParseOptions;
    TokenProvider := TEnvTokenProvider.Create(Options.TokenEnv);
    try
      Run(Options);
    finally
      FreeAndNil(TokenProvider);
    end;
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
