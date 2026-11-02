{ Shared compiler directives (dual Delphi / Free Pascal).

  Every unit of the library includes this file right after 'unit ...;'. On
  Delphi it doesn't change the mode; on FPC it enables Delphi mode (long
  strings, Delphi-style generics, advanced records).

  KAFKA_WINDOWS: defined when the target is Windows, on either compiler
  (Delphi defines MSWINDOWS; FPC defines WINDOWS).

  There is no define for the TLS backend: it is chosen at run time
  (TKafkaConnectionParams.TlsBackend), as pascal-common-faa's transport does
  (its docs/transport-design.md, T3). }

{$IFDEF FPC}
  {$MODE DELPHI}
  {$H+}
{$ENDIF}

{$IFDEF MSWINDOWS}
  {$DEFINE KAFKA_WINDOWS}
{$ENDIF}
{$IFDEF WINDOWS}
  {$IFNDEF KAFKA_WINDOWS}
    {$DEFINE KAFKA_WINDOWS}
  {$ENDIF}
{$ENDIF}
