program KafkaInterop;

{ The interoperability test's program on Delphi (Kafka.Interop has what it
  does; tools/interop.py runs it between the Java console tools). The FPC
  build is fpc/KafkaInteropFpc.lpr, over the same unit.

  Leaks: ReportMemoryLeaksOnShutdown, as the test runners. }

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
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
  Kafka.Transport in '..\..\src\Kafka.Transport.pas',
  Kafka.Sasl in '..\..\src\Kafka.Sasl.pas',
  Kafka.Connection in '..\..\src\Kafka.Connection.pas',
  Kafka.Partitioner in '..\..\src\Kafka.Partitioner.pas',
  Kafka.Cluster in '..\..\src\Kafka.Cluster.pas',
  Kafka.Producer in '..\..\src\Kafka.Producer.pas',
  Kafka.Consumer in '..\..\src\Kafka.Consumer.pas',
  Kafka.Interop in 'Kafka.Interop.pas';

begin
  ReportMemoryLeaksOnShutdown := True;
  ExitCode := KafkaInteropMain;
end.
