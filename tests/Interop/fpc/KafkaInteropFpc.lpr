program KafkaInteropFpc;

{ The interoperability test's program on FPC (Kafka.Interop has what it
  does; tools/interop.py runs it between the Java console tools). The Delphi
  build is ..\KafkaInterop.dpr, over the same unit. Built with heaptrc. }

{$mode delphi}{$H+}

uses
  {$IFDEF UNIX}
  cthreads,
  // Without cwstring, FPC on Unix converts strings between code pages one
  // byte per character (the skill's rtl-gotchas.md, "Encoding").
  cwstring,
  {$ENDIF}
  Kafka.Interop;

begin
  // DefaultSystemCodePage isn't UTF-8 on a plain FPC console.
  SetMultiByteConversionCodePage(CP_UTF8);
  ExitCode := KafkaInteropMain;
end.
