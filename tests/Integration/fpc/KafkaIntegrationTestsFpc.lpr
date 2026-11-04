program KafkaIntegrationTestsFpc;

{ FPCUnit runner for the integration tests. Same coverage as the DUnitX suite
  (tests/Integration/Kafka.IntegrationSuite.dpr): the fixtures in tests/Integration/fpc are
  generated from the DUnitX masters by tools/gen_fpc_mirror.py. Needs the
  broker of docker/docker-compose.yml.

  Console (text output), when called with any parameter:
    .\KafkaIntegrationTestsFpc.exe --all --format=plain
  GUI (test tree + green/red bar), with no parameters:
    .\KafkaIntegrationTestsFpc.exe
  Outside Windows it always runs in console mode (no LCL/widgetset). }

{$mode delphi}{$H+}

uses
  {$IFDEF UNIX}
  cthreads,
  // Without cwstring, FPC on Unix converts strings between code pages one
  // byte per character (the skill's rtl-gotchas.md, "Encoding").
  cwstring,
  {$ENDIF}
  {$IFDEF MSWINDOWS}
  Interfaces, Forms, GuiTestRunner,
  {$ENDIF}
  Classes, consoletestrunner, testregistry,
  Kafka.IntegrationTests,
  Kafka.SaslIntegrationTests,
  Kafka.GroupIntegrationTests,
  Kafka.ConsumerGroupIntegrationTests,
  Kafka.IdempotenceIntegrationTests,
  Kafka.TracingIntegrationTests,
  Kafka.CloudIntegrationTests;

var
  ConsoleApp: TTestRunner;
begin
  // Plain FPC console: DefaultSystemCodePage isn't UTF-8 by default, and the
  // tests' non-ASCII literals would be transcoded wrongly.
  SetMultiByteConversionCodePage(CP_UTF8);

  {$IFDEF MSWINDOWS}
  if ParamCount = 0 then
  begin
    Application.Initialize;
    Application.CreateForm(TGUITestRunner, TestRunner);
    Application.Run;
  end
  else
  {$ENDIF}
  begin
    DefaultFormat := fPlain;
    DefaultRunAllTests := True;
    ConsoleApp := TTestRunner.Create(nil);
    try
      ConsoleApp.Initialize;
      ConsoleApp.Title := 'pascal-kafka-faa - integration tests (FPCUnit)';
      ConsoleApp.Run;
    finally
      ConsoleApp.Free;
    end;
  end;
end.
