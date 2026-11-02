unit Kafka.Compression;

{$I kafka.inc}

{ The record batch codecs: none and gzip today; snappy, lz4 and zstd raise
  EKafkaCompression until phase K3 (docs/design.md, section 5).

  The codec is the low three bits of a record batch's attributes
  (TKafkaCompression's ordinals are those values). What gets compressed is
  the batch's records section only, after the record count
  (Kafka.Protocol.RecordBatch).

  Kafka's gzip is gzip (RFC 1952: a 10-byte header, deflate, then CRC-32 and
  the size), not zlib (RFC 1950), which is what pascal-named-pipes-faa's
  Pipes.Compression writes. Both RTLs can make a raw deflate stream, with no
  header at all: System.ZLib with windowBits -15 (Delphi), ZStream with
  Askipheader True (FPC, paszlib, pure Pascal). So the gzip framing is
  written and read here, the same on both compilers, rather than through each
  RTL's way of asking zlib for gzip (windowBits 31 in Delphi; FPC's ZStream
  has no gzip mode except for files).

  Reading accepts what RFC 1952 allows in a header (FEXTRA, FNAME, FCOMMENT,
  FHCRC) and checks the trailer: the CRC-32 and the size of what came out,
  so a truncated or corrupt member fails here and not later in a record.
  One member only: Java's GZIPOutputStream, librdkafka and the Go clients
  write one. A second member (concatenated gzip files) fails the trailer
  check with a message that names the possibility.

  Decompression stops at AMaxSize, checked as the data comes out, so a small
  payload that inflates to gigabytes fails without allocating them (the same
  care as Pipes.Compression).

  CRC-32 here is the IEEE one of gzip (reflected 0xEDB88320), not the
  CRC-32C of record batches (Kafka.Protocol.Crc32c). Its table is filled in
  the initialization section; overflow and range checks are off for the
  unit because the arithmetic is meant to wrap. }

{$OVERFLOWCHECKS OFF}
{$RANGECHECKS OFF}

interface

uses
  SysUtils,
  Kafka.Types;

type
  /// The codec of a record batch: the values of its attributes' low bits.
  TKafkaCompression = (kcNone, kcGzip, kcSnappy, kcLz4, kcZstd);

/// 'none', 'gzip', 'snappy', 'lz4', 'zstd': the names of Java's
/// compression.type.
function KafkaCompressionName(ACodec: TKafkaCompression): string;
/// The codec for attribute bits 0-2; EKafkaCompression for 5 to 7, which
/// no Kafka version defines.
function KafkaCompressionFromAttributes(AAttributes: Int16): TKafkaCompression;
/// Whether this build can compress and decompress with ACodec.
function KafkaCompressionSupported(ACodec: TKafkaCompression): Boolean;

/// AData compressed with ACodec; kcNone gives AData back.
/// EKafkaCompression for a codec this build doesn't have.
function KafkaCompress(ACodec: TKafkaCompression; const AData: TBytes): TBytes;
/// AData decompressed with ACodec; kcNone gives AData back.
/// EKafkaCompression for a codec this build doesn't have, for data that
/// doesn't decompress, and for output larger than AMaxSize bytes.
function KafkaDecompress(ACodec: TKafkaCompression; const AData: TBytes;
  AMaxSize: Integer): TBytes;

/// One gzip member (RFC 1952) holding AData: raw deflate at zlib's default
/// level, a header with no optional fields (MTIME 0, OS 255 "unknown").
function KafkaGzipCompress(const AData: TBytes): TBytes;
/// The content of one gzip member, its CRC-32 and size checked.
function KafkaGzipDecompress(const AData: TBytes; AMaxSize: Integer): TBytes;

/// The CRC-32 of gzip and zip (IEEE 802.3, reflected 0xEDB88320).
function KafkaCrc32(const AData: TBytes; AOffset, ACount: Integer): Cardinal;

implementation

uses
  Classes,
  {$IFDEF FPC}
  ZStream
  {$ELSE}
  System.ZLib
  {$ENDIF};

const
  GZIP_ID1 = $1F;
  GZIP_ID2 = $8B;
  GZIP_CM_DEFLATE = 8;
  GZIP_FHCRC = $02;
  GZIP_FEXTRA = $04;
  GZIP_FNAME = $08;
  GZIP_FCOMMENT = $10;
  GZIP_FRESERVED = $E0;
  GZIP_OS_UNKNOWN = $FF;
  GZIP_HEADER_SIZE = 10;
  GZIP_TRAILER_SIZE = 8;
  INFLATE_CHUNK = 65536;

var
  GCrc32Table: array[0..255] of Cardinal;

procedure FillCrc32Table;
var
  I, K: Integer;
  C: Cardinal;
begin
  for I := 0 to 255 do
  begin
    C := Cardinal(I);
    for K := 0 to 7 do
      if (C and 1) <> 0 then
        C := (C shr 1) xor $EDB88320
      else
        C := C shr 1;
    GCrc32Table[I] := C;
  end;
end;

function KafkaCrc32(const AData: TBytes; AOffset, ACount: Integer): Cardinal;
var
  I: Integer;
  C: Cardinal;
begin
  C := $FFFFFFFF;
  for I := AOffset to AOffset + ACount - 1 do
    C := GCrc32Table[(C xor AData[I]) and $FF] xor (C shr 8);
  Result := C xor $FFFFFFFF;
end;

function KafkaCompressionName(ACodec: TKafkaCompression): string;
begin
  case ACodec of
    kcNone: Result := 'none';
    kcGzip: Result := 'gzip';
    kcSnappy: Result := 'snappy';
    kcLz4: Result := 'lz4';
    kcZstd: Result := 'zstd';
  else
    Result := Format('codec(%d)', [Ord(ACodec)]);
  end;
end;

function KafkaCompressionFromAttributes(AAttributes: Int16): TKafkaCompression;
var
  LBits: Integer;
begin
  LBits := AAttributes and 7;
  if LBits > Ord(High(TKafkaCompression)) then
    raise EKafkaCompression.CreateFmt('Unknown compression codec %d in a record batch',
      [LBits]);
  Result := TKafkaCompression(LBits);
end;

function KafkaCompressionSupported(ACodec: TKafkaCompression): Boolean;
begin
  Result := ACodec in [kcNone, kcGzip];
end;

procedure RaiseUnsupported(ACodec: TKafkaCompression);
begin
  raise EKafkaCompression.CreateFmt('Compression %s is not supported yet (none and gzip are)',
    [KafkaCompressionName(ACodec)]);
end;

function KafkaCompress(ACodec: TKafkaCompression; const AData: TBytes): TBytes;
begin
  case ACodec of
    kcNone: Result := AData;
    kcGzip: Result := KafkaGzipCompress(AData);
  else
    Result := nil;
    RaiseUnsupported(ACodec);
  end;
end;

function KafkaDecompress(ACodec: TKafkaCompression; const AData: TBytes;
  AMaxSize: Integer): TBytes;
begin
  case ACodec of
    kcNone: Result := AData;
    kcGzip: Result := KafkaGzipDecompress(AData, AMaxSize);
  else
    Result := nil;
    RaiseUnsupported(ACodec);
  end;
end;

function StreamToBytes(AStream: TMemoryStream): TBytes;
begin
  Result := nil;
  SetLength(Result, AStream.Size);
  if AStream.Size > 0 then
  begin
    AStream.Position := 0;
    AStream.ReadBuffer(Result[0], AStream.Size);
  end;
end;

function RawDeflate(const AData: TBytes): TBytes;
var
  LOut: TMemoryStream;
  LComp: {$IFDEF FPC}TCompressionStream{$ELSE}TZCompressionStream{$ENDIF};
begin
  LOut := TMemoryStream.Create;
  try
    {$IFDEF FPC}
    LComp := TCompressionStream.Create(cldefault, LOut, True);
    {$ELSE}
    LComp := TZCompressionStream.Create(LOut, zcDefault, -15);
    {$ENDIF}
    try
      if Length(AData) > 0 then
        LComp.WriteBuffer(AData[0], Length(AData));
    finally
      // Both destructors finish the stream (Z_FINISH): the last bytes reach
      // LOut here.
      LComp.Free;
    end;
    Result := StreamToBytes(LOut);
  finally
    LOut.Free;
  end;
end;

function RawInflate(const AData: TBytes; AOffset, ACount, AMaxSize: Integer): TBytes;
var
  LIn, LOut: TMemoryStream;
  LDecomp: {$IFDEF FPC}TDecompressionStream{$ELSE}TZDecompressionStream{$ENDIF};
  LBuf: TBytes;
  LRead: Integer;
  LTotal: Int64;
begin
  LIn := TMemoryStream.Create;
  LOut := TMemoryStream.Create;
  try
    if ACount > 0 then
      LIn.WriteBuffer(AData[AOffset], ACount);
    LIn.Position := 0;
    LBuf := nil;
    SetLength(LBuf, INFLATE_CHUNK);
    {$IFDEF FPC}
    LDecomp := TDecompressionStream.Create(LIn, True);
    {$ELSE}
    LDecomp := TZDecompressionStream.Create(LIn, -15);
    {$ENDIF}
    try
      LTotal := 0;
      repeat
        try
          LRead := LDecomp.Read(LBuf[0], INFLATE_CHUNK);
        except
          on E: Exception do
            // EDecompressionError (FPC) or EZDecompressionError (Delphi).
            raise EKafkaCompression.CreateFmt('gzip data does not decompress: %s',
              [E.Message]);
        end;
        if LRead > 0 then
        begin
          Inc(LTotal, LRead);
          if LTotal > AMaxSize then
            raise EKafkaCompression.CreateFmt(
              'gzip data decompresses to more than %d bytes', [AMaxSize]);
          LOut.WriteBuffer(LBuf[0], LRead);
        end;
      until LRead <= 0;
    finally
      LDecomp.Free;
    end;
    Result := StreamToBytes(LOut);
  finally
    LOut.Free;
    LIn.Free;
  end;
end;

procedure PutUInt32LE(var AData: TBytes; AOffset: Integer; AValue: Cardinal);
begin
  AData[AOffset] := Byte(AValue);
  AData[AOffset + 1] := Byte(AValue shr 8);
  AData[AOffset + 2] := Byte(AValue shr 16);
  AData[AOffset + 3] := Byte(AValue shr 24);
end;

function GetUInt32LE(const AData: TBytes; AOffset: Integer): Cardinal;
begin
  Result := Cardinal(AData[AOffset]) or (Cardinal(AData[AOffset + 1]) shl 8) or
    (Cardinal(AData[AOffset + 2]) shl 16) or (Cardinal(AData[AOffset + 3]) shl 24);
end;

function KafkaGzipCompress(const AData: TBytes): TBytes;
var
  LDeflated: TBytes;
  N: Integer;
begin
  LDeflated := RawDeflate(AData);
  N := Length(LDeflated);
  Result := nil;
  SetLength(Result, GZIP_HEADER_SIZE + N + GZIP_TRAILER_SIZE);
  FillChar(Result[0], GZIP_HEADER_SIZE, 0);
  Result[0] := GZIP_ID1;
  Result[1] := GZIP_ID2;
  Result[2] := GZIP_CM_DEFLATE;
  // FLG 0, MTIME 0, XFL 0 (bytes 3 to 8, already zero).
  Result[9] := GZIP_OS_UNKNOWN;
  if N > 0 then
    Move(LDeflated[0], Result[GZIP_HEADER_SIZE], N);
  PutUInt32LE(Result, GZIP_HEADER_SIZE + N, KafkaCrc32(AData, 0, Length(AData)));
  PutUInt32LE(Result, GZIP_HEADER_SIZE + N + 4, Cardinal(Length(AData)));
end;

procedure GzipError(const AFormat: string; const AArgs: array of const);
begin
  raise EKafkaCompression.Create('gzip: ' + Format(AFormat, AArgs));
end;

function KafkaGzipDecompress(const AData: TBytes; AMaxSize: Integer): TBytes;
var
  N, P, LEnd: Integer;
  LFlags: Byte;
  LExtra: Integer;
  LCrc, LSize: Cardinal;
begin
  N := Length(AData);
  if N < GZIP_HEADER_SIZE + GZIP_TRAILER_SIZE then
    GzipError('%d bytes is shorter than a header and a trailer', [N]);
  if (AData[0] <> GZIP_ID1) or (AData[1] <> GZIP_ID2) then
    GzipError('not gzip data (starts with %.2x %.2x)', [AData[0], AData[1]]);
  if AData[2] <> GZIP_CM_DEFLATE then
    GzipError('compression method %d is not deflate', [AData[2]]);
  LFlags := AData[3];
  if (LFlags and GZIP_FRESERVED) <> 0 then
    GzipError('reserved header flags set (%.2x)', [LFlags]);
  LEnd := N - GZIP_TRAILER_SIZE;
  P := GZIP_HEADER_SIZE;
  if (LFlags and GZIP_FEXTRA) <> 0 then
  begin
    if P + 2 > LEnd then
      GzipError('header cut inside FEXTRA', []);
    LExtra := AData[P] or (AData[P + 1] shl 8);
    Inc(P, 2 + LExtra);
  end;
  if (LFlags and GZIP_FNAME) <> 0 then
  begin
    while (P < LEnd) and (AData[P] <> 0) do
      Inc(P);
    Inc(P);
  end;
  if (LFlags and GZIP_FCOMMENT) <> 0 then
  begin
    while (P < LEnd) and (AData[P] <> 0) do
      Inc(P);
    Inc(P);
  end;
  if (LFlags and GZIP_FHCRC) <> 0 then
    Inc(P, 2);
  if P > LEnd then
    GzipError('header longer than the data', []);
  // FTEXT (bit 0) is only a hint about the content: nothing to do.
  Result := RawInflate(AData, P, LEnd - P, AMaxSize);
  LCrc := GetUInt32LE(AData, LEnd);
  LSize := GetUInt32LE(AData, LEnd + 4);
  if (LSize <> Cardinal(Length(Result))) or
    (LCrc <> KafkaCrc32(Result, 0, Length(Result))) then
    GzipError('the trailer doesn''t match the content (corrupt data, or more than ' +
      'one gzip member, which this library doesn''t read)', []);
end;

initialization
  FillCrc32Table;

end.
