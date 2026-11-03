unit Kafka.Compression;

{$I kafka.inc}

{ The record batch codecs: none, gzip, snappy and lz4, all in Pascal but
  for the deflate underneath gzip; zstd raises EKafkaCompression until K3's
  step 4 (docs/design.md, "K3 progress").

  The codec is the low three bits of a record batch's attributes
  (TKafkaCompression's ordinals are those values). What gets compressed is
  the batch's records section only, after the record count
  (Kafka.Protocol.RecordBatch).

  gzip. Kafka's gzip is gzip (RFC 1952: a 10-byte header, deflate, then
  CRC-32 and the size), not zlib (RFC 1950), which is what
  pascal-named-pipes-faa's Pipes.Compression writes. Both RTLs can make a
  raw deflate stream, with no header at all: System.ZLib with windowBits -15
  (Delphi), ZStream with Askipheader True (FPC, paszlib, pure Pascal). So
  the gzip framing is written and read here, the same on both compilers,
  rather than through each RTL's way of asking zlib for gzip (windowBits 31
  in Delphi; FPC's ZStream has no gzip mode except for files). Reading
  accepts what RFC 1952 allows in a header (FEXTRA, FNAME, FCOMMENT, FHCRC)
  and checks the trailer: the CRC-32 and the size of what came out, so a
  truncated or corrupt member fails here and not later in a record. One
  member only: Java's GZIPOutputStream, librdkafka and the Go clients write
  one. A second member (concatenated gzip files) fails the trailer check
  with a message that names the possibility. CRC-32 here is the IEEE one of
  gzip (reflected 0xEDB88320), not the CRC-32C of record batches
  (Kafka.Protocol.Crc32c).

  snappy. Two shapes reach a consumer. Java's producer writes xerial's
  framing (snappy-java's SnappyOutputStream, 1.1.10.7 in Kafka 4.4.0): a
  16-byte header (the magic 82 'SNAPPY' 00, then version 1 and compatible
  version 1 as big-endian int32), then each 32 KiB of input as a big-endian
  int32 length and a raw snappy block. librdkafka (and so the clients built
  on it) writes one raw snappy block with no framing at all. Java's reader,
  SnappyInputStream, tells them apart by the magic: 16 bytes or more that
  start with it are framed, anything else is a raw block; librdkafka reads
  the same way. Both are read here by that rule, and a framed stream may
  hold another header between chunks (concatenated streams, which xerial
  reads too). Writing is xerial's framing, as Java's. A raw block is the
  format of google/snappy's format_description.txt: the uncompressed length
  as a varint, then literals and copies with 1-, 2- or 4-byte offsets. The
  compressor is the simple greedy one (a hash of 4 bytes, input cut in 64
  KiB fragments, as the reference does): it gives valid snappy, not the
  reference's exact bytes, which no reader needs.

  lz4. The LZ4 frame format (lz4_Frame_format.md, version 1), as Java's
  Lz4BlockOutputStream writes it: magic 184D2204, independent blocks of up
  to 64 KiB, no block or content checksum, no content size, each block
  stored uncompressed when compressing doesn't make it smaller, and the
  header checksum over the descriptor only. That last point is KIP-57: Kafka
  0.8 to 0.9 computed it over the magic number too, and record batch v2
  (the only one Kafka 4 speaks) always has the correct one; Java refuses a
  wrong one for v2, and so does this unit, saying when it is the old broken
  form. Reading takes what Java's Lz4BlockInputStream takes (block sizes 64
  KiB to 4 MiB, a content size, block checksums, a content checksum) and
  refuses what it refuses (dependent blocks, a dictionary id, reserved bits,
  another version). Where Java reads less, this reads more: the content
  checksum and the content size are checked (Java skips both; the reference
  library checks them). The compressor is LZ4's fast one in its simplest
  form (one hash table of 4-byte sequences, the end-of-block rules: the
  last 5 bytes are literals, no match starts in the last 12).

  Bytes after the end of what a reader expects (a second gzip member, bytes
  after an lz4 frame's end mark, 1 to 3 bytes after the last snappy chunk)
  are refused. Java ignores some of them; no producer writes them, and a
  batch that has them is more likely corrupt than meant.

  Decompression stops at AMaxSize, checked before each piece of output is
  allocated, so a small payload that would inflate to gigabytes fails
  without allocating them (the same care as Pipes.Compression). A snappy
  block declares its length first: a length above the limit, or above what
  the block's bytes could hold (no element gives more than 64 bytes for 3),
  fails before anything is allocated.

  xxHash32 (lz4's checksums), CRC-32 and the compressors' hashes are meant
  to wrap: overflow and range checks are off for the unit, the
  multiplications go through UInt64 and are masked back to 32 bits (FPC on
  64 bits computes in 64), and every index into the input is checked by
  hand before it is used. Tables are filled in the initialization section. }

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

/// snappy as Java's producer writes it: xerial's framing, 32 KiB blocks.
function KafkaSnappyCompress(const AData: TBytes): TBytes;
/// xerial-framed snappy (Java) or one raw snappy block (librdkafka), told
/// apart as xerial's SnappyInputStream does.
function KafkaSnappyDecompress(const AData: TBytes; AMaxSize: Integer): TBytes;
/// One raw snappy block, with no framing.
function KafkaSnappyRawCompress(const AData: TBytes): TBytes;
/// The content of one raw snappy block.
function KafkaSnappyRawDecompress(const AData: TBytes; AMaxSize: Integer): TBytes;

/// One LZ4 frame as Java's producer writes it (64 KiB independent blocks,
/// no checksums but the header's, KIP-57's correct header checksum).
function KafkaLz4Compress(const AData: TBytes): TBytes;
/// The content of one LZ4 frame, its checksums checked.
function KafkaLz4Decompress(const AData: TBytes; AMaxSize: Integer): TBytes;
/// One LZ4 block (no frame; at most 64 KiB of AData, the frame's block).
function KafkaLz4BlockCompress(const AData: TBytes): TBytes;
/// The content of one LZ4 block, at most AMaxSize bytes.
function KafkaLz4BlockDecompress(const AData: TBytes; AMaxSize: Integer): TBytes;

/// The CRC-32 of gzip and zip (IEEE 802.3, reflected 0xEDB88320).
function KafkaCrc32(const AData: TBytes; AOffset, ACount: Integer): Cardinal;
/// xxHash32 (the hash of lz4's checksums) of ACount bytes from AOffset.
function KafkaXxHash32(const AData: TBytes; AOffset, ACount: Integer;
  ASeed: Cardinal): Cardinal;

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

  XERIAL_MAGIC: array[0..7] of Byte = ($82, $53, $4E, $41, $50, $50, $59, $00);
  XERIAL_HEADER_SIZE = 16;
  XERIAL_VERSION = 1;
  XERIAL_MIN_COMPATIBLE_VERSION = 1;
  XERIAL_BLOCK_SIZE = 32 * 1024;
  SNAPPY_FRAGMENT_SIZE = 65536;
  SNAPPY_HASH_BITS = 14;
  // The most a snappy element gives per byte it takes: 64 bytes from a
  // 3-byte copy, rounded up.
  SNAPPY_MAX_EXPANSION = 22;

  LZ4_MAGIC = Cardinal($184D2204);
  LZ4_FLG_VERSION_1 = $40;
  LZ4_FLG_INDEPENDENT = $20;
  LZ4_FLG_BLOCK_CHECKSUM = $10;
  LZ4_FLG_CONTENT_SIZE = $08;
  LZ4_FLG_CONTENT_CHECKSUM = $04;
  LZ4_FLG_RESERVED = $02;
  LZ4_FLG_DICT_ID = $01;
  LZ4_BD_64KB = $40;
  LZ4_BD_RESERVED = $8F;
  LZ4_BLOCK_UNCOMPRESSED = Cardinal($80000000);
  LZ4_WRITE_BLOCK_SIZE = 65536;
  LZ4_MIN_MATCH = 4;
  LZ4_LAST_LITERALS = 5;
  LZ4_MF_LIMIT = 12;
  LZ4_MAX_OFFSET = 65535;
  LZ4_HASH_BITS = 12;

  XXH_PRIME1 = Cardinal($9E3779B1);
  XXH_PRIME2 = Cardinal($85EBCA77);
  XXH_PRIME3 = Cardinal($C2B2AE3D);
  XXH_PRIME4 = Cardinal($27D4EB2F);
  XXH_PRIME5 = Cardinal($165667B1);

type
  TIntegerTable = array of Integer;

  { A growing output, with the decompression limit: Reserve raises before
    the buffer grows past Limit. }
  TOutput = record
    Data: TBytes;
    Len: Integer;
    Limit: Integer;
    Codec: string;
    procedure Init(const ACodec: string; ALimit, ACapacity: Integer);
    procedure Reserve(ACount: Int64);
    procedure AddByte(AValue: Byte);
    procedure AddRange(const ASource: TBytes; AOffset, ACount: Integer);
    /// ACount bytes from ADistance back (they may overlap what they write).
    procedure CopyBack(ADistance, ACount: Integer);
    function ToBytes: TBytes;
  end;

var
  GCrc32Table: array[0..255] of Cardinal;

procedure TOutput.Init(const ACodec: string; ALimit, ACapacity: Integer);
begin
  Data := nil;
  Len := 0;
  Limit := ALimit;
  Codec := ACodec;
  if ACapacity > ALimit then
    ACapacity := ALimit;
  if ACapacity > 0 then
    SetLength(Data, ACapacity);
end;

procedure TOutput.Reserve(ACount: Int64);
var
  LNeed, LCap: Int64;
begin
  LNeed := Int64(Len) + ACount;
  if LNeed > Limit then
    raise EKafkaCompression.CreateFmt('%s data decompresses to more than %d bytes',
      [Codec, Limit]);
  if LNeed > Length(Data) then
  begin
    LCap := 2 * Int64(Length(Data));
    if LCap < LNeed then
      LCap := LNeed;
    if LCap < 256 then
      LCap := 256;
    if LCap > Limit then
      LCap := Limit;
    SetLength(Data, Integer(LCap));
  end;
end;

procedure TOutput.AddByte(AValue: Byte);
begin
  if Len >= Length(Data) then
    Reserve(1);
  Data[Len] := AValue;
  Inc(Len);
end;

procedure TOutput.AddRange(const ASource: TBytes; AOffset, ACount: Integer);
begin
  if ACount <= 0 then
    Exit;
  Reserve(ACount);
  Move(ASource[AOffset], Data[Len], ACount);
  Inc(Len, ACount);
end;

procedure TOutput.CopyBack(ADistance, ACount: Integer);
var
  I, S: Integer;
begin
  Reserve(ACount);
  S := Len - ADistance;
  if ADistance >= ACount then
    Move(Data[S], Data[Len], ACount)
  else
    // Overlapping: each byte may be one this copy just wrote (a run).
    for I := 0 to ACount - 1 do
      Data[Len + I] := Data[S + I];
  Inc(Len, ACount);
end;

function TOutput.ToBytes: TBytes;
begin
  SetLength(Data, Len);
  Result := Data;
  Data := nil;
end;

function Mul(A, B: Cardinal): Cardinal; inline;
begin
  Result := Cardinal((UInt64(A) * UInt64(B)) and $FFFFFFFF);
end;

function Add(A, B: Cardinal): Cardinal; inline;
begin
  Result := Cardinal((UInt64(A) + UInt64(B)) and $FFFFFFFF);
end;

function Rotl(A: Cardinal; R: Integer): Cardinal; inline;
begin
  Result := Cardinal(((UInt64(A) shl R) or (UInt64(A) shr (32 - R))) and $FFFFFFFF);
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

procedure PutUInt32BE(var AData: TBytes; AOffset: Integer; AValue: Cardinal);
begin
  AData[AOffset] := Byte(AValue shr 24);
  AData[AOffset + 1] := Byte(AValue shr 16);
  AData[AOffset + 2] := Byte(AValue shr 8);
  AData[AOffset + 3] := Byte(AValue);
end;

function GetUInt32BE(const AData: TBytes; AOffset: Integer): Cardinal;
begin
  Result := (Cardinal(AData[AOffset]) shl 24) or (Cardinal(AData[AOffset + 1]) shl 16) or
    (Cardinal(AData[AOffset + 2]) shl 8) or Cardinal(AData[AOffset + 3]);
end;

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

function XxRound(AAcc, AInput: Cardinal): Cardinal; inline;
begin
  Result := Mul(Rotl(Add(AAcc, Mul(AInput, XXH_PRIME2)), 13), XXH_PRIME1);
end;

function KafkaXxHash32(const AData: TBytes; AOffset, ACount: Integer;
  ASeed: Cardinal): Cardinal;
var
  P, LEnd: Integer;
  V1, V2, V3, V4, H: Cardinal;
begin
  P := AOffset;
  LEnd := AOffset + ACount;
  if ACount >= 16 then
  begin
    V1 := Add(Add(ASeed, XXH_PRIME1), XXH_PRIME2);
    V2 := Add(ASeed, XXH_PRIME2);
    V3 := ASeed;
    // ASeed - PRIME1, as 2^32 - PRIME1 added.
    V4 := Add(ASeed, Cardinal($61C8864F));
    while P <= LEnd - 16 do
    begin
      V1 := XxRound(V1, GetUInt32LE(AData, P));
      V2 := XxRound(V2, GetUInt32LE(AData, P + 4));
      V3 := XxRound(V3, GetUInt32LE(AData, P + 8));
      V4 := XxRound(V4, GetUInt32LE(AData, P + 12));
      Inc(P, 16);
    end;
    H := Add(Add(Rotl(V1, 1), Rotl(V2, 7)), Add(Rotl(V3, 12), Rotl(V4, 18)));
  end
  else
    H := Add(ASeed, XXH_PRIME5);
  H := Add(H, Cardinal(ACount));
  while P + 4 <= LEnd do
  begin
    H := Mul(Rotl(Add(H, Mul(GetUInt32LE(AData, P), XXH_PRIME3)), 17), XXH_PRIME4);
    Inc(P, 4);
  end;
  while P < LEnd do
  begin
    H := Mul(Rotl(Add(H, Mul(AData[P], XXH_PRIME5)), 11), XXH_PRIME1);
    Inc(P);
  end;
  H := H xor (H shr 15);
  H := Mul(H, XXH_PRIME2);
  H := H xor (H shr 13);
  H := Mul(H, XXH_PRIME3);
  H := H xor (H shr 16);
  Result := H;
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
  Result := ACodec in [kcNone, kcGzip, kcSnappy, kcLz4];
end;

procedure RaiseUnsupported(ACodec: TKafkaCompression);
begin
  raise EKafkaCompression.CreateFmt('Compression %s is not supported yet ' +
    '(none, gzip, snappy and lz4 are)', [KafkaCompressionName(ACodec)]);
end;

function KafkaCompress(ACodec: TKafkaCompression; const AData: TBytes): TBytes;
begin
  case ACodec of
    kcNone: Result := AData;
    kcGzip: Result := KafkaGzipCompress(AData);
    kcSnappy: Result := KafkaSnappyCompress(AData);
    kcLz4: Result := KafkaLz4Compress(AData);
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
    kcSnappy: Result := KafkaSnappyDecompress(AData, AMaxSize);
    kcLz4: Result := KafkaLz4Decompress(AData, AMaxSize);
  else
    Result := nil;
    RaiseUnsupported(ACodec);
  end;
end;

{ gzip }

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

{ snappy }

procedure SnappyError(const AFormat: string; const AArgs: array of const);
begin
  raise EKafkaCompression.Create('snappy: ' + Format(AFormat, AArgs));
end;

/// Decodes the raw block AIn[AStart..AEnd) onto AOut. Copies reach only
/// into this block's own output.
procedure SnappyDecodeBlock(const AIn: TBytes; AStart, AEnd: Integer; var AOut: TOutput);
var
  P, LShift, N, K, LBlockStart: Integer;
  B, LTag: Byte;
  LDeclared, LEndOut, LLen, LOffset: Int64;
begin
  P := AStart;
  LDeclared := 0;
  LShift := 0;
  repeat
    if P >= AEnd then
      SnappyError('the block ends inside its length', []);
    B := AIn[P];
    Inc(P);
    if (LShift = 28) and (B > $0F) then
      SnappyError('the block''s length doesn''t fit 32 bits', []);
    LDeclared := LDeclared or (Int64(B and $7F) shl LShift);
    Inc(LShift, 7);
  until (B and $80) = 0;
  if LDeclared > Int64(AEnd - P) * SNAPPY_MAX_EXPANSION then
    SnappyError('a block of %d bytes says it holds %d, more than such a block can',
      [AEnd - AStart, LDeclared]);
  // Above the limit this raises before anything is allocated.
  AOut.Reserve(LDeclared);
  LBlockStart := AOut.Len;
  LEndOut := Int64(LBlockStart) + LDeclared;
  while P < AEnd do
  begin
    LTag := AIn[P];
    Inc(P);
    case LTag and 3 of
      0:
        begin
          LLen := LTag shr 2;
          if LLen >= 60 then
          begin
            N := Integer(LLen) - 59;
            if AEnd - P < N then
              SnappyError('a literal''s length is cut short', []);
            LLen := 0;
            for K := 0 to N - 1 do
              LLen := LLen or (Int64(AIn[P + K]) shl (8 * K));
            Inc(P, N);
          end;
          Inc(LLen);
          if LLen > AEnd - P then
            SnappyError('a literal of %d bytes runs past the block', [LLen]);
          if LLen > LEndOut - AOut.Len then
            SnappyError('more than the %d bytes the block declares', [LDeclared]);
          Move(AIn[P], AOut.Data[AOut.Len], Integer(LLen));
          Inc(AOut.Len, Integer(LLen));
          Inc(P, Integer(LLen));
          Continue;
        end;
      1:
        begin
          if P >= AEnd then
            SnappyError('a copy is cut short', []);
          LLen := ((LTag shr 2) and 7) + 4;
          LOffset := (Int64(LTag shr 5) shl 8) or AIn[P];
          Inc(P);
        end;
      2:
        begin
          if AEnd - P < 2 then
            SnappyError('a copy is cut short', []);
          LLen := (LTag shr 2) + 1;
          LOffset := AIn[P] or (Int64(AIn[P + 1]) shl 8);
          Inc(P, 2);
        end;
    else
      begin
        if AEnd - P < 4 then
          SnappyError('a copy is cut short', []);
        LLen := (LTag shr 2) + 1;
        LOffset := GetUInt32LE(AIn, P);
        Inc(P, 4);
      end;
    end;
    if (LOffset = 0) or (LOffset > AOut.Len - LBlockStart) then
      SnappyError('a copy from %d bytes back, with %d bytes before it',
        [LOffset, AOut.Len - LBlockStart]);
    if LLen > LEndOut - AOut.Len then
      SnappyError('more than the %d bytes the block declares', [LDeclared]);
    AOut.CopyBack(Integer(LOffset), Integer(LLen));
  end;
  if AOut.Len <> LEndOut then
    SnappyError('the block gives %d bytes and declares %d',
      [AOut.Len - LBlockStart, LDeclared]);
end;

procedure SnappyEmitLiteral(const AIn: TBytes; AStart, ACount: Integer; var AOut: TOutput);
var
  N: Cardinal;
begin
  N := Cardinal(ACount - 1);
  if N < 60 then
    AOut.AddByte(Byte(N shl 2))
  else if N < $100 then
  begin
    AOut.AddByte(60 shl 2);
    AOut.AddByte(Byte(N));
  end
  else if N < $10000 then
  begin
    AOut.AddByte(61 shl 2);
    AOut.AddByte(Byte(N));
    AOut.AddByte(Byte(N shr 8));
  end
  else if N < $1000000 then
  begin
    AOut.AddByte(62 shl 2);
    AOut.AddByte(Byte(N));
    AOut.AddByte(Byte(N shr 8));
    AOut.AddByte(Byte(N shr 16));
  end
  else
  begin
    AOut.AddByte(63 shl 2);
    AOut.AddByte(Byte(N));
    AOut.AddByte(Byte(N shr 8));
    AOut.AddByte(Byte(N shr 16));
    AOut.AddByte(Byte(N shr 24));
  end;
  AOut.AddRange(AIn, AStart, ACount);
end;

/// One copy element of 4 to 64 bytes (1 to 64 with a 2-byte offset).
procedure SnappyEmitCopyElement(AOffset, ALength: Integer; var AOut: TOutput);
begin
  if (ALength >= 4) and (ALength <= 11) and (AOffset < 2048) then
  begin
    AOut.AddByte(Byte(1 or ((ALength - 4) shl 2) or ((AOffset shr 8) shl 5)));
    AOut.AddByte(Byte(AOffset));
  end
  else
  begin
    AOut.AddByte(Byte(2 or ((ALength - 1) shl 2)));
    AOut.AddByte(Byte(AOffset));
    AOut.AddByte(Byte(AOffset shr 8));
  end;
end;

procedure SnappyEmitCopy(AOffset, ALength: Integer; var AOut: TOutput);
begin
  // Pieces of 64, leaving at least 4 for the last (a 1-byte-offset copy
  // can't be shorter), as the reference does.
  while ALength >= 68 do
  begin
    SnappyEmitCopyElement(AOffset, 64, AOut);
    Dec(ALength, 64);
  end;
  if ALength > 64 then
  begin
    SnappyEmitCopyElement(AOffset, 60, AOut);
    Dec(ALength, 60);
  end;
  SnappyEmitCopyElement(AOffset, ALength, AOut);
end;

function Load32(const AData: TBytes; AOffset: Integer): Cardinal; inline;
begin
  Result := Cardinal(AData[AOffset]) or (Cardinal(AData[AOffset + 1]) shl 8) or
    (Cardinal(AData[AOffset + 2]) shl 16) or (Cardinal(AData[AOffset + 3]) shl 24);
end;

/// AIn[AStart..AEnd), at most 64 KiB, as snappy elements: offsets stay
/// inside the fragment, so a 2-byte offset always fits.
procedure SnappyEncodeFragment(const AIn: TBytes; AStart, AEnd: Integer;
  var ATable: TIntegerTable; var AOut: TOutput);
var
  I, LLiteral, LCandidate, LLength, LSkip, H: Integer;
  V: Cardinal;
begin
  for I := 0 to High(ATable) do
    ATable[I] := -1;
  LLiteral := AStart;
  I := AStart;
  LSkip := 32;
  while AEnd - I >= 4 do
  begin
    V := Load32(AIn, I);
    H := Integer(Mul(V, $1E35A7BD) shr (32 - SNAPPY_HASH_BITS));
    LCandidate := ATable[H];
    ATable[H] := I;
    if (LCandidate >= 0) and (Load32(AIn, LCandidate) = V) then
    begin
      if I > LLiteral then
        SnappyEmitLiteral(AIn, LLiteral, I - LLiteral, AOut);
      LLength := 4;
      while (I + LLength < AEnd) and (AIn[LCandidate + LLength] = AIn[I + LLength]) do
        Inc(LLength);
      SnappyEmitCopy(I - LCandidate, LLength, AOut);
      Inc(I, LLength);
      LLiteral := I;
      LSkip := 32;
    end
    else
    begin
      // After many misses look less often (the reference's heuristic):
      // incompressible data goes through fast.
      Inc(I, LSkip shr 5);
      Inc(LSkip);
    end;
  end;
  if AEnd > LLiteral then
    SnappyEmitLiteral(AIn, LLiteral, AEnd - LLiteral, AOut);
end;

procedure SnappyEncodeBlock(const AIn: TBytes; AStart, ACount: Integer;
  var ATable: TIntegerTable; var AOut: TOutput);
var
  V: Cardinal;
  P, LEnd, LFragmentEnd: Integer;
begin
  V := Cardinal(ACount);
  while V >= $80 do
  begin
    AOut.AddByte(Byte(V or $80));
    V := V shr 7;
  end;
  AOut.AddByte(Byte(V));
  P := AStart;
  LEnd := AStart + ACount;
  while P < LEnd do
  begin
    LFragmentEnd := P + SNAPPY_FRAGMENT_SIZE;
    if LFragmentEnd > LEnd then
      LFragmentEnd := LEnd;
    SnappyEncodeFragment(AIn, P, LFragmentEnd, ATable, AOut);
    P := LFragmentEnd;
  end;
end;

function NewTable(ABits: Integer): TIntegerTable;
begin
  Result := nil;
  SetLength(Result, 1 shl ABits);
end;

function KafkaSnappyRawCompress(const AData: TBytes): TBytes;
var
  LOut: TOutput;
  LTable: TIntegerTable;
  N: Integer;
begin
  N := Length(AData);
  LTable := NewTable(SNAPPY_HASH_BITS);
  LOut.Init('snappy', MaxInt, 32 + N + N div 6);
  SnappyEncodeBlock(AData, 0, N, LTable, LOut);
  Result := LOut.ToBytes;
end;

function KafkaSnappyRawDecompress(const AData: TBytes; AMaxSize: Integer): TBytes;
var
  LOut: TOutput;
begin
  LOut.Init('snappy', AMaxSize, 0);
  SnappyDecodeBlock(AData, 0, Length(AData), LOut);
  Result := LOut.ToBytes;
end;

function HasXerialHeader(const AData: TBytes; AOffset: Integer): Boolean;
var
  I: Integer;
begin
  Result := Length(AData) - AOffset >= XERIAL_HEADER_SIZE;
  if Result then
    for I := 0 to High(XERIAL_MAGIC) do
      if AData[AOffset + I] <> XERIAL_MAGIC[I] then
        Exit(False);
end;

function KafkaSnappyCompress(const AData: TBytes): TBytes;
var
  LOut: TOutput;
  LTable: TIntegerTable;
  N, P, LCount, LLengthAt, I: Integer;
begin
  N := Length(AData);
  LTable := NewTable(SNAPPY_HASH_BITS);
  LOut.Init('snappy', MaxInt, XERIAL_HEADER_SIZE + 40 * (N div XERIAL_BLOCK_SIZE + 1) +
    N + N div 6);
  for I := 0 to High(XERIAL_MAGIC) do
    LOut.AddByte(XERIAL_MAGIC[I]);
  for I := 1 to 8 do
    LOut.AddByte(0);
  PutUInt32BE(LOut.Data, 8, XERIAL_VERSION);
  PutUInt32BE(LOut.Data, 12, XERIAL_MIN_COMPATIBLE_VERSION);
  P := 0;
  while P < N do
  begin
    LCount := N - P;
    if LCount > XERIAL_BLOCK_SIZE then
      LCount := XERIAL_BLOCK_SIZE;
    LLengthAt := LOut.Len;
    for I := 1 to 4 do
      LOut.AddByte(0);
    SnappyEncodeBlock(AData, P, LCount, LTable, LOut);
    PutUInt32BE(LOut.Data, LLengthAt, Cardinal(LOut.Len - LLengthAt - 4));
    Inc(P, LCount);
  end;
  Result := LOut.ToBytes;
end;

function KafkaSnappyDecompress(const AData: TBytes; AMaxSize: Integer): TBytes;
var
  LOut: TOutput;
  N, P: Integer;
  LChunk, LVersion: Cardinal;
begin
  N := Length(AData);
  LOut.Init('snappy', AMaxSize, 0);
  // xerial's SnappyInputStream: framed only when the whole 16-byte header
  // is there and starts with the magic; a raw block otherwise (librdkafka's
  // producers write those).
  if not HasXerialHeader(AData, 0) then
  begin
    SnappyDecodeBlock(AData, 0, N, LOut);
    Exit(LOut.ToBytes);
  end;
  P := 0;
  while P < N do
  begin
    if HasXerialHeader(AData, P) then
    begin
      // The first header, or another between chunks (a concatenated
      // stream, which xerial reads on).
      LVersion := GetUInt32BE(AData, P + 8);
      // An int32 to Java: a version with the high bit set is negative.
      if Integer(LVersion) < XERIAL_MIN_COMPATIBLE_VERSION then
        SnappyError('xerial framing version %d, %d or later expected',
          [Integer(LVersion), XERIAL_MIN_COMPATIBLE_VERSION]);
      Inc(P, XERIAL_HEADER_SIZE);
      Continue;
    end;
    if N - P < 4 then
      SnappyError('%d bytes after the last chunk', [N - P]);
    LChunk := GetUInt32BE(AData, P);
    if LChunk > Cardinal(N - P - 4) then
      SnappyError('a chunk of %d bytes with %d left', [Int64(LChunk), N - P - 4]);
    SnappyDecodeBlock(AData, P + 4, P + 4 + Integer(LChunk), LOut);
    Inc(P, 4 + Integer(LChunk));
  end;
  Result := LOut.ToBytes;
end;

{ lz4 }

procedure Lz4Error(const AFormat: string; const AArgs: array of const);
begin
  raise EKafkaCompression.Create('lz4: ' + Format(AFormat, AArgs));
end;

/// Decodes the block AIn[AStart..AEnd) onto AOut, at most AMaxBlock bytes;
/// matches reach only into this block's output (independent blocks).
procedure Lz4DecodeBlock(const AIn: TBytes; AStart, AEnd, AMaxBlock: Integer;
  var AOut: TOutput);
var
  P, LBlockStart: Integer;
  LToken, B: Byte;
  LLen, LOffset: Int64;
begin
  LBlockStart := AOut.Len;
  P := AStart;
  if P >= AEnd then
    Lz4Error('an empty compressed block', []);
  while True do
  begin
    LToken := AIn[P];
    Inc(P);
    LLen := LToken shr 4;
    if LLen = 15 then
      repeat
        if P >= AEnd then
          Lz4Error('a literal length is cut short', []);
        B := AIn[P];
        Inc(P);
        Inc(LLen, B);
      until B <> 255;
    if LLen > AEnd - P then
      Lz4Error('%d literals with %d bytes left in the block', [LLen, AEnd - P]);
    if Int64(AOut.Len - LBlockStart) + LLen > AMaxBlock then
      Lz4Error('a block decompresses to more than its %d bytes', [AMaxBlock]);
    if LLen > 0 then
    begin
      AOut.Reserve(LLen);
      Move(AIn[P], AOut.Data[AOut.Len], Integer(LLen));
      Inc(AOut.Len, Integer(LLen));
      Inc(P, Integer(LLen));
    end;
    // The last sequence has literals only.
    if P = AEnd then
      Break;
    if AEnd - P < 2 then
      Lz4Error('a match offset is cut short', []);
    LOffset := AIn[P] or (Int64(AIn[P + 1]) shl 8);
    Inc(P, 2);
    if (LOffset = 0) or (LOffset > AOut.Len - LBlockStart) then
      Lz4Error('a match from %d bytes back, with %d bytes before it in the block',
        [LOffset, AOut.Len - LBlockStart]);
    LLen := (LToken and 15) + LZ4_MIN_MATCH;
    if (LToken and 15) = 15 then
      repeat
        if P >= AEnd then
          Lz4Error('a match length is cut short', []);
        B := AIn[P];
        Inc(P);
        Inc(LLen, B);
      until B <> 255;
    if Int64(AOut.Len - LBlockStart) + LLen > AMaxBlock then
      Lz4Error('a block decompresses to more than its %d bytes', [AMaxBlock]);
    AOut.CopyBack(Integer(LOffset), Integer(LLen));
    // A block may not end with a match.
    if P >= AEnd then
      Lz4Error('the block ends with a match, not with literals', []);
  end;
end;

procedure Lz4EmitLength(AValue: Integer; var AOut: TOutput);
begin
  while AValue >= 255 do
  begin
    AOut.AddByte(255);
    Dec(AValue, 255);
  end;
  AOut.AddByte(Byte(AValue));
end;

procedure Lz4EmitSequence(const AIn: TBytes; ALiteral, ALiteralCount, AOffset,
  AMatchLength: Integer; var AOut: TOutput);
var
  LToken, M: Integer;
begin
  M := AMatchLength - LZ4_MIN_MATCH;
  if ALiteralCount >= 15 then
    LToken := 15 shl 4
  else
    LToken := ALiteralCount shl 4;
  if M >= 15 then
    LToken := LToken or 15
  else
    LToken := LToken or M;
  AOut.AddByte(Byte(LToken));
  if ALiteralCount >= 15 then
    Lz4EmitLength(ALiteralCount - 15, AOut);
  AOut.AddRange(AIn, ALiteral, ALiteralCount);
  AOut.AddByte(Byte(AOffset));
  AOut.AddByte(Byte(AOffset shr 8));
  if M >= 15 then
    Lz4EmitLength(M - 15, AOut);
end;

procedure Lz4EmitLastLiterals(const AIn: TBytes; ALiteral, ACount: Integer;
  var AOut: TOutput);
begin
  if ACount >= 15 then
  begin
    AOut.AddByte(15 shl 4);
    Lz4EmitLength(ACount - 15, AOut);
  end
  else
    AOut.AddByte(Byte(ACount shl 4));
  AOut.AddRange(AIn, ALiteral, ACount);
end;

/// AIn[AStart..AStart + ACount), at most 64 KiB, as one LZ4 block.
procedure Lz4EncodeBlock(const AIn: TBytes; AStart, ACount: Integer;
  var ATable: TIntegerTable; var AOut: TOutput);
var
  I, LEnd, LAnchor, LMatchLimit, LLastStart, LCandidate, LLength, LSkip, H: Integer;
  V: Cardinal;
begin
  LEnd := AStart + ACount;
  LAnchor := AStart;
  // A block shorter than 13 bytes has no room for a match.
  if ACount > LZ4_MF_LIMIT then
  begin
    for I := 0 to High(ATable) do
      ATable[I] := -1;
    LMatchLimit := LEnd - LZ4_LAST_LITERALS;
    LLastStart := LEnd - LZ4_MF_LIMIT;
    I := AStart;
    LSkip := 64;
    while I <= LLastStart do
    begin
      V := Load32(AIn, I);
      H := Integer(Mul(V, XXH_PRIME1) shr (32 - LZ4_HASH_BITS));
      LCandidate := ATable[H];
      ATable[H] := I;
      if (LCandidate >= 0) and (I - LCandidate <= LZ4_MAX_OFFSET) and
        (Load32(AIn, LCandidate) = V) then
      begin
        // Back over equal bytes still in the literals.
        while (I > LAnchor) and (LCandidate > AStart) and
          (AIn[I - 1] = AIn[LCandidate - 1]) do
        begin
          Dec(I);
          Dec(LCandidate);
        end;
        LLength := LZ4_MIN_MATCH;
        while (I + LLength < LMatchLimit) and (AIn[LCandidate + LLength] = AIn[I + LLength]) do
          Inc(LLength);
        Lz4EmitSequence(AIn, LAnchor, I - LAnchor, I - LCandidate, LLength, AOut);
        Inc(I, LLength);
        LAnchor := I;
        LSkip := 64;
      end
      else
      begin
        Inc(I, LSkip shr 6);
        Inc(LSkip);
      end;
    end;
  end;
  Lz4EmitLastLiterals(AIn, LAnchor, LEnd - LAnchor, AOut);
end;

function KafkaLz4BlockCompress(const AData: TBytes): TBytes;
var
  LOut: TOutput;
  LTable: TIntegerTable;
  N: Integer;
begin
  N := Length(AData);
  if N > LZ4_WRITE_BLOCK_SIZE then
    raise EArgumentException.CreateFmt('An lz4 block holds at most %d bytes, not %d',
      [LZ4_WRITE_BLOCK_SIZE, N]);
  LTable := NewTable(LZ4_HASH_BITS);
  LOut.Init('lz4', MaxInt, 16 + N + N div 255);
  Lz4EncodeBlock(AData, 0, N, LTable, LOut);
  Result := LOut.ToBytes;
end;

function KafkaLz4BlockDecompress(const AData: TBytes; AMaxSize: Integer): TBytes;
var
  LOut: TOutput;
begin
  LOut.Init('lz4', AMaxSize, 0);
  Lz4DecodeBlock(AData, 0, Length(AData), AMaxSize, LOut);
  Result := LOut.ToBytes;
end;

function KafkaLz4Compress(const AData: TBytes): TBytes;
var
  LOut: TOutput;
  LTable: TIntegerTable;
  N, P, LCount, LSizeAt, LBefore, LCompressed: Integer;
begin
  N := Length(AData);
  LTable := NewTable(LZ4_HASH_BITS);
  LOut.Init('lz4', MaxInt, 11 + N + N div 255 + 24 * (N div LZ4_WRITE_BLOCK_SIZE + 1));
  LOut.Reserve(4);
  PutUInt32LE(LOut.Data, 0, LZ4_MAGIC);
  LOut.Len := 4;
  LOut.AddByte(LZ4_FLG_VERSION_1 or LZ4_FLG_INDEPENDENT);
  LOut.AddByte(LZ4_BD_64KB);
  // KIP-57: the descriptor's checksum, without the magic number.
  LOut.AddByte(Byte(KafkaXxHash32(LOut.Data, 4, 2, 0) shr 8));
  P := 0;
  while P < N do
  begin
    LCount := N - P;
    if LCount > LZ4_WRITE_BLOCK_SIZE then
      LCount := LZ4_WRITE_BLOCK_SIZE;
    LSizeAt := LOut.Len;
    LOut.Reserve(4);
    Inc(LOut.Len, 4);
    LBefore := LOut.Len;
    Lz4EncodeBlock(AData, P, LCount, LTable, LOut);
    LCompressed := LOut.Len - LBefore;
    if LCompressed >= LCount then
    begin
      // Java's rule: a block that doesn't shrink goes as it is.
      LOut.Len := LBefore;
      LOut.AddRange(AData, P, LCount);
      PutUInt32LE(LOut.Data, LSizeAt, Cardinal(LCount) or LZ4_BLOCK_UNCOMPRESSED);
    end
    else
      PutUInt32LE(LOut.Data, LSizeAt, Cardinal(LCompressed));
    Inc(P, LCount);
  end;
  // The end mark.
  LOut.Reserve(4);
  PutUInt32LE(LOut.Data, LOut.Len, 0);
  Inc(LOut.Len, 4);
  Result := LOut.ToBytes;
end;

function KafkaLz4Decompress(const AData: TBytes; AMaxSize: Integer): TBytes;
var
  LOut: TOutput;
  N, P, LMaxBlock, LSize, LCode: Integer;
  LFlg, LBd, LHc: Byte;
  LHasSize: Boolean;
  LContentSize: Int64;
  LBlock, LMagic: Cardinal;
begin
  N := Length(AData);
  if N < 7 then
    Lz4Error('%d bytes is shorter than a frame header', [N]);
  LMagic := GetUInt32LE(AData, 0);
  if LMagic <> LZ4_MAGIC then
    Lz4Error('not an lz4 frame (magic %.8x)', [Int64(LMagic)]);
  LFlg := AData[4];
  LBd := AData[5];
  if (LFlg shr 6) <> 1 then
    Lz4Error('frame version %d, 1 expected', [LFlg shr 6]);
  if (LFlg and LZ4_FLG_INDEPENDENT) = 0 then
    Lz4Error('dependent blocks, which Kafka doesn''t write (Java refuses them too)', []);
  if (LFlg and LZ4_FLG_DICT_ID) <> 0 then
    Lz4Error('a dictionary id, which Kafka doesn''t use (Java refuses it too)', []);
  if (LFlg and LZ4_FLG_RESERVED) <> 0 then
    Lz4Error('a reserved flag set (FLG %.2x)', [LFlg]);
  if (LBd and LZ4_BD_RESERVED) <> 0 then
    Lz4Error('reserved bits set in the block descriptor (BD %.2x)', [LBd]);
  LCode := (LBd shr 4) and 7;
  if LCode < 4 then
    Lz4Error('block size code %d, 4 to 7 expected', [LCode]);
  // 64 KiB, 256 KiB, 1 MiB, 4 MiB.
  LMaxBlock := 1 shl (2 * LCode + 8);
  P := 6;
  LHasSize := (LFlg and LZ4_FLG_CONTENT_SIZE) <> 0;
  LContentSize := 0;
  if LHasSize then
  begin
    if N < P + 9 then
      Lz4Error('the frame header is cut short', []);
    if GetUInt32LE(AData, P + 4) <> 0 then
      Lz4Error('a content size above 4 GiB', []);
    LContentSize := GetUInt32LE(AData, P);
    if LContentSize > AMaxSize then
      raise EKafkaCompression.CreateFmt('lz4 data decompresses to more than %d bytes ' +
        '(its frame says %d)', [AMaxSize, LContentSize]);
    Inc(P, 8);
  end;
  LHc := Byte(KafkaXxHash32(AData, 4, P - 4, 0) shr 8);
  if AData[P] <> LHc then
  begin
    if AData[P] = Byte(KafkaXxHash32(AData, 0, P, 0) shr 8) then
      Lz4Error('the header checksum covers the magic number: Kafka 0.8''s broken framing ' +
        '(KIP-57), which record batch v2 never has', []);
    Lz4Error('header checksum %.2x, %.2x expected', [AData[P], LHc]);
  end;
  Inc(P);
  LOut.Init('lz4', AMaxSize, 0);
  if LHasSize then
    LOut.Reserve(LContentSize);
  while True do
  begin
    if N - P < 4 then
      Lz4Error('the frame ends before its end mark', []);
    LBlock := GetUInt32LE(AData, P);
    Inc(P, 4);
    // Java reads the size without the flag bit, so 80000000 ends the frame too.
    LSize := Integer(LBlock and not LZ4_BLOCK_UNCOMPRESSED);
    if LSize = 0 then
      Break;
    if LSize > LMaxBlock then
      Lz4Error('a block of %d bytes, above the frame''s %d', [LSize, LMaxBlock]);
    if LSize > N - P then
      Lz4Error('a block of %d bytes with %d left', [LSize, N - P]);
    if (LFlg and LZ4_FLG_BLOCK_CHECKSUM) <> 0 then
    begin
      if N - P - LSize < 4 then
        Lz4Error('a block checksum is cut short', []);
      if KafkaXxHash32(AData, P, LSize, 0) <> GetUInt32LE(AData, P + LSize) then
        Lz4Error('a block checksum doesn''t match', []);
    end;
    if (LBlock and LZ4_BLOCK_UNCOMPRESSED) <> 0 then
      LOut.AddRange(AData, P, LSize)
    else
      Lz4DecodeBlock(AData, P, P + LSize, LMaxBlock, LOut);
    Inc(P, LSize);
    if (LFlg and LZ4_FLG_BLOCK_CHECKSUM) <> 0 then
      Inc(P, 4);
  end;
  if (LFlg and LZ4_FLG_CONTENT_CHECKSUM) <> 0 then
  begin
    if N - P < 4 then
      Lz4Error('the content checksum is cut short', []);
    if KafkaXxHash32(LOut.Data, 0, LOut.Len, 0) <> GetUInt32LE(AData, P) then
      Lz4Error('the content checksum doesn''t match', []);
    Inc(P, 4);
  end;
  if LHasSize and (LOut.Len <> LContentSize) then
    Lz4Error('the frame says %d bytes and holds %d', [LContentSize, LOut.Len]);
  if P <> N then
    Lz4Error('%d bytes after the frame''s end (one frame is read, as Java does)', [N - P]);
  Result := LOut.ToBytes;
end;

initialization
  FillCrc32Table;

end.
