program Kafka.UnitTests;

{ DUnitX runner for the unit tests.

  The sibling FPCUnit suite lives in tests/Unit/fpc, with the same coverage
  and identical test bodies: the files there are GENERATED from these by
  tools/gen_fpc_mirror.py (Kafka.DUnitXCompat exists for that). Always edit
  the DUnitX masters here and regenerate the mirror.

  pascal-common-faa's units come from the search path
  (external/pascal-common-faa/src), as an application would find them. }

{$APPTYPE CONSOLE}
{$STRONGLINKTYPES ON}

uses
  System.SysUtils,
  DUnitX.Loggers.Console,
  DUnitX.Loggers.Xml.NUnit,
  DUnitX.TestFramework,
  Kafka.Types in '..\..\src\Kafka.Types.pas',
  Kafka.Protocol.Crc32c in '..\..\src\Kafka.Protocol.Crc32c.pas',
  Kafka.Protocol.Primitives in '..\..\src\Kafka.Protocol.Primitives.pas',
  Kafka.Protocol.Envelope in '..\..\src\Kafka.Protocol.Envelope.pas',
  Kafka.Compression in '..\..\src\Kafka.Compression.pas',
  Kafka.Protocol.RecordBatch in '..\..\src\Kafka.Protocol.RecordBatch.pas',
  Kafka.Errors in '..\..\src\Kafka.Errors.pas',
  Kafka.Messages.Apis in '..\..\src\Kafka.Messages.Apis.pas',
  Kafka.Messages.ApiVersions in '..\..\src\Kafka.Messages.ApiVersions.pas',
  Kafka.Messages.Metadata in '..\..\src\Kafka.Messages.Metadata.pas',
  Kafka.Messages.Produce in '..\..\src\Kafka.Messages.Produce.pas',
  Kafka.Messages.Fetch in '..\..\src\Kafka.Messages.Fetch.pas',
  Kafka.Messages.ListOffsets in '..\..\src\Kafka.Messages.ListOffsets.pas',
  Kafka.Messages.SaslHandshake in '..\..\src\Kafka.Messages.SaslHandshake.pas',
  Kafka.Messages.SaslAuthenticate in '..\..\src\Kafka.Messages.SaslAuthenticate.pas',
  Kafka.Messages.FindCoordinator in '..\..\src\Kafka.Messages.FindCoordinator.pas',
  Kafka.Messages.ConsumerGroupHeartbeat in '..\..\src\Kafka.Messages.ConsumerGroupHeartbeat.pas',
  Kafka.Messages.OffsetCommit in '..\..\src\Kafka.Messages.OffsetCommit.pas',
  Kafka.Messages.OffsetFetch in '..\..\src\Kafka.Messages.OffsetFetch.pas',
  Kafka.Messages.ConsumerGroupDescribe in '..\..\src\Kafka.Messages.ConsumerGroupDescribe.pas',
  Kafka.Transport in '..\..\src\Kafka.Transport.pas',
  Kafka.Sasl in '..\..\src\Kafka.Sasl.pas',
  Kafka.Connection in '..\..\src\Kafka.Connection.pas',
  Kafka.Partitioner in '..\..\src\Kafka.Partitioner.pas',
  Kafka.Cluster in '..\..\src\Kafka.Cluster.pas',
  Kafka.Producer in '..\..\src\Kafka.Producer.pas',
  Kafka.Consumer in '..\..\src\Kafka.Consumer.pas',
  Kafka.DUnitXCompat in 'Kafka.DUnitXCompat.pas',
  Kafka.TestHelpers in 'Kafka.TestHelpers.pas',
  Kafka.TestCaptures in 'Kafka.TestCaptures.pas',
  Kafka.TestFakeBroker in 'Kafka.TestFakeBroker.pas',
  Kafka.TestMessages in 'Kafka.TestMessages.pas',
  Kafka.TestPartitions in 'Kafka.TestPartitions.pas',
  Kafka.PrimitivesTests in 'Kafka.PrimitivesTests.pas',
  Kafka.Crc32cTests in 'Kafka.Crc32cTests.pas',
  Kafka.EnvelopeTests in 'Kafka.EnvelopeTests.pas',
  Kafka.MessagesTests in 'Kafka.MessagesTests.pas',
  Kafka.GroupMessagesTests in 'Kafka.GroupMessagesTests.pas',
  Kafka.ProduceFetchTests in 'Kafka.ProduceFetchTests.pas',
  Kafka.RecordBatchTests in 'Kafka.RecordBatchTests.pas',
  Kafka.PartitionerTests in 'Kafka.PartitionerTests.pas',
  Kafka.ClusterTests in 'Kafka.ClusterTests.pas',
  Kafka.ProducerTests in 'Kafka.ProducerTests.pas',
  Kafka.ConsumerTests in 'Kafka.ConsumerTests.pas',
  Kafka.SaslTests in 'Kafka.SaslTests.pas',
  Kafka.SaslConnectionTests in 'Kafka.SaslConnectionTests.pas',
  Kafka.ConnectionTests in 'Kafka.ConnectionTests.pas';

var
  runner: ITestRunner;
  results: IRunResults;
  logger: ITestLogger;
  nunitLogger: ITestLogger;
begin
  // Acceptance criterion on both sides: 0 leaks (FastMM here, heaptrc on FPC).
  ReportMemoryLeaksOnShutdown := True;
  try
    TDUnitX.CheckCommandLine;

    if TDUnitX.Options.Include = '' then
      TDUnitX.Options.Include := '.';

    runner := TDUnitX.CreateRunner;
    runner.UseRTTI := True;
    runner.FailsOnNoAsserts := False;

    if TDUnitX.Options.ConsoleMode <> TDunitXConsoleMode.Off then
    begin
      logger := TDUnitXConsoleLogger.Create(
        TDUnitX.Options.ConsoleMode = TDunitXConsoleMode.Quiet);
      runner.AddLogger(logger);
    end;

    nunitLogger := TDUnitXXMLNUnitFileLogger.Create(TDUnitX.Options.XMLOutputFile);
    runner.AddLogger(nunitLogger);

    results := runner.Execute;

    if not results.AllPassed then
      System.ExitCode := EXIT_ERRORS;

    if (TDUnitX.Options.ExitBehavior = TDUnitXExitBehavior.Pause) and IsConsole then
    begin
      System.Write('Done.. press <Enter> key to quit.');
      System.Readln;
    end;
  except
    on E: Exception do
      System.Writeln(E.ClassName, ': ', E.Message);
  end;
end.
