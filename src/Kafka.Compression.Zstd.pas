unit Kafka.Compression.Zstd;

{$I kafka.inc}

{ The zstd decoder (RFC 8878) in Pascal: what a consumer needs to read the
  record batches a Java, librdkafka or Go producer compressed with zstd.
  There is no encoder: the producer refuses zstd (docs/design.md, open
  question 2, option C). Kafka.Compression calls it for kcZstd.

  What Java 4.4.0 does decides the doubtful points. Its consumer reads zstd
  through zstd-jni 1.5.7-17 (ZstdInputStreamNoFinalizer over libzstd
  1.5.7's ZSTD_decompressStream, default parameters), so:
  - every frame in the data is read, one after the other, and skippable
    frames are skipped; bytes after the last frame that aren't a whole frame
    are refused (Java's "Truncated source");
  - a frame that asks for a window above 128 MiB is refused
    (ZSTD_WINDOWLOG_LIMIT_DEFAULT, 27), although this decoder, which keeps the
    whole output, wouldn't need it;
  - a dictionary id is refused (Java sets no dictionary: "dictionary
    wrong"), and so are the legacy formats of zstd 0.x (zstd-jni builds
    libzstd without ZSTD_LEGACY_SUPPORT);
  - the content checksum, when the frame has one, is checked; so is the
    content size, when the frame says it.
  Java's producer writes level 3 with no checksum, through a 16 KiB
  buffered stream: frames with a window descriptor and no content size
  (the captures in docs/codec-captures.md); librdkafka and Python's
  zstandard write the content size. Both shapes are read.

  The format, as libzstd 1.5.7 reads it (zstd_decompress.c,
  zstd_decompress_block.c, huf_decompress.c, fse_decompress.c), where the
  RFC leaves room:
  - blocks hold at most Block_Maximum_Size, the window or 128 KiB, whichever
    is smaller, compressed and decompressed; a compressed block needs at
    least 2 bytes;
  - literals: raw, RLE, Huffman with a tree, or with the previous block's
    tree ("treeless"); 1 stream, or 4 with a jump table and at least 6
    literals; Huffman weights written directly or with FSE (accuracy log 5
    or 6, two interleaved states, ending as fse_decompress.c's tail does);
    at most 12 bits per code, as libzstd (the RFC says 11), and the weight-1
    count checked even and at least 2, as libzstd;
  - sequences: the predefined, RLE, FSE or repeated tables for literal
    lengths, offsets and match lengths, the repeated ones the last a block of
    the frame used; extra bits read offset, match, literal, then the states
    updated literal, match, offset; the three repeat offsets start at 1, 4,
    8 in each frame and live across its blocks;
  - every bitstream must end exactly where its bits end: a Huffman stream,
    the sequences, as libzstd's BIT_endOfDStream checks.
  A match reaches only into the output of its own frame (frames are
  independent); its offset isn't checked against the window, as libzstd's
  single-pass decoder doesn't.

  The output is bounded before it is allocated, as the other codecs: a
  frame that says its content size is refused from its header when that is
  above AMaxSize, and every block reserves its bytes against the limit first.

  The bitstreams of zstd are read backwards from their last byte, whose
  highest set bit marks the end; TZstdBits keeps the number of bits left and
  reads below the first bit as zeros (libzstd's container does the same), so
  a corrupt stream fails a check and never reads outside its bytes. Every
  index into the input is checked by hand before it is used. XXH64 (the
  content checksum is its low 32 bits) wraps on purpose: overflow and range
  checks are off for the unit, and its arithmetic is UInt64, whose
  multiplication wraps the same way on Delphi (Win32 too) and FPC. The
  predefined tables are built in the initialization section. }

{$OVERFLOWCHECKS OFF}
{$RANGECHECKS OFF}

interface

uses
  SysUtils,
  Kafka.Types;

/// The content of the zstd frames in AData (RFC 8878), skippable frames
/// skipped. EKafkaCompression for data that isn't zstd or doesn't decode,
/// for what Java's decoder refuses (a dictionary, a window above 128 MiB,
/// a legacy frame) and for output larger than AMaxSize bytes.
function KafkaZstdDecompress(const AData: TBytes; AMaxSize: Integer): TBytes;

/// XXH64 of ACount bytes from AOffset (zstd's content checksum is the low
/// 32 bits of XXH64 with seed 0).
function KafkaXxHash64(const AData: TBytes; AOffset, ACount: Integer;
  ASeed: UInt64): UInt64;

implementation

const
  ZSTD_MAGIC = Cardinal($FD2FB528);
  ZSTD_SKIPPABLE_MASK = Cardinal($FFFFFFF0);
  ZSTD_SKIPPABLE_MAGIC = Cardinal($184D2A50);
  // The frames of zstd 0.1 (FD2FB51E) to 0.7 (FD2FB527).
  ZSTD_LEGACY_LOW = Cardinal($FD2FB51E);
  ZSTD_LEGACY_HIGH = Cardinal($FD2FB527);
  ZSTD_BLOCK_SIZE_MAX = 128 * 1024;
  // Java's decoder: ZSTD_WINDOWLOG_LIMIT_DEFAULT, 1 shl 27.
  ZSTD_WINDOW_MAX = Int64(1) shl 27;
  ZSTD_MIN_CBLOCK_SIZE = 2;
  ZSTD_MIN_LITERALS_FOR_4_STREAMS = 6;
  HUF_TABLELOG_MAX = 12;
  HUF_WEIGHTS_MAX_LOG = 6;
  FSE_MIN_LOG = 5;

  LL_MAX_SYMBOL = 35;
  ML_MAX_SYMBOL = 52;
  OF_MAX_SYMBOL = 31;
  LL_MAX_LOG = 9;
  ML_MAX_LOG = 9;
  OF_MAX_LOG = 8;

  LL_BASE: array[0..LL_MAX_SYMBOL] of Cardinal = (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
    13, 14, 15, 16, 18, 20, 22, 24, 28, 32, 40, 48, 64, 128, 256, 512, 1024, 2048, 4096,
    8192, 16384, 32768, 65536);
  LL_BITS: array[0..LL_MAX_SYMBOL] of Byte = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 1, 1, 1, 1, 2, 2, 3, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);
  ML_BASE: array[0..ML_MAX_SYMBOL] of Cardinal = (3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 37,
    39, 41, 43, 47, 51, 59, 67, 83, 99, 131, 259, 515, 1027, 2051, 4099, 8195, 16387, 32771,
    65539);
  ML_BITS: array[0..ML_MAX_SYMBOL] of Byte = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 7, 8,
    9, 10, 11, 12, 13, 14, 15, 16);

  // The predefined distributions (RFC 8878, 3.1.1.3.2.2): -1 is a
  // probability "less than 1".
  LL_DEFAULT_LOG = 6;
  LL_DEFAULT: array[0..35] of Integer = (4, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 3, 2, 1, 1, 1, 1, 1, -1, -1, -1, -1);
  ML_DEFAULT_LOG = 6;
  ML_DEFAULT: array[0..52] of Integer = (1, 4, 3, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1,
    -1, -1, -1, -1, -1, -1);
  OF_DEFAULT_LOG = 5;
  OF_DEFAULT: array[0..28] of Integer = (1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1);

  XXH64_P1 = UInt64($9E3779B185EBCA87);
  XXH64_P2 = UInt64($C2B2AE3D27D4EB4F);
  XXH64_P3 = UInt64($165667B19E3779F9);
  XXH64_P4 = UInt64($85EBCA77C2B2AE63);
  XXH64_P5 = UInt64($27D4EB2F165667C5);

type
  TZstdCounts = array[0..255] of Integer;

  TFseEntry = record
    Symbol: Byte;
    NbBits: Byte;
    NewState: Word;
  end;

  /// An FSE decoding table: 1 shl Log entries.
  TFseTable = record
    Log: Integer;
    Entries: array of TFseEntry;
  end;

  THufEntry = record
    Symbol: Byte;
    NbBits: Byte;
  end;

  /// A Huffman decoding table: 1 shl MaxBits entries, looked up by the
  /// next MaxBits bits.
  THufTable = record
    MaxBits: Integer;
    Entries: array of THufEntry;
  end;

  /// A backward bitstream (RFC 8878, 4.1): Data[Base..Base + Bits div 8]
  /// read from its last bit down; below bit 0 it reads zeros.
  TZstdBits = record
    Data: TBytes;
    Base: Integer;
    Bits: Integer;
    procedure Init(const AData: TBytes; AStart, AEnd: Integer; const AWhat: string);
    function Peek(N: Integer): Cardinal;
    function Read(N: Integer): Cardinal;
  end;

  /// The output of all the frames, with the limit.
  TZstdOutput = record
    Data: TBytes;
    Len: Integer;
    Limit: Integer;
    procedure Reserve(ACount: Int64);
    procedure Append(const ASource: TBytes; AOffset, ACount: Integer);
    procedure Fill(AValue: Byte; ACount: Integer);
    procedure CopyBack(ADistance, ACount: Integer);
  end;

  /// What lives across the blocks of a frame.
  TZstdFrame = record
    Start: Integer;
    BlockMax: Integer;
    Huffman: THufTable;
    HasHuffman: Boolean;
    LL, OF_, ML: TFseTable;
    HasTables: Boolean;
    Rep: array[0..2] of Int64;
    Literals: TBytes;
    LiteralCount: Integer;
  end;

var
  GLlDefault, GMlDefault, GOfDefault: TFseTable;

procedure ZstdError(const AFormat: string; const AArgs: array of const);
begin
  raise EKafkaCompression.Create('zstd: ' + Format(AFormat, AArgs));
end;

procedure LimitError(ALimit: Integer; ASays: Int64);
begin
  if ASays >= 0 then
    raise EKafkaCompression.CreateFmt('zstd data decompresses to more than %d bytes ' +
      '(its frame says %d)', [ALimit, ASays]);
  raise EKafkaCompression.CreateFmt('zstd data decompresses to more than %d bytes', [ALimit]);
end;

function HighBit(AValue: Cardinal): Integer;
begin
  // The index of the highest set bit; AValue > 0.
  Result := -1;
  while AValue <> 0 do
  begin
    AValue := AValue shr 1;
    Inc(Result);
  end;
end;

function GetLE16(const AData: TBytes; AOffset: Integer): Cardinal; inline;
begin
  Result := Cardinal(AData[AOffset]) or (Cardinal(AData[AOffset + 1]) shl 8);
end;

function GetLE32(const AData: TBytes; AOffset: Integer): Cardinal; inline;
begin
  Result := Cardinal(AData[AOffset]) or (Cardinal(AData[AOffset + 1]) shl 8) or
    (Cardinal(AData[AOffset + 2]) shl 16) or (Cardinal(AData[AOffset + 3]) shl 24);
end;

function GetLE64(const AData: TBytes; AOffset: Integer): UInt64; inline;
begin
  Result := UInt64(GetLE32(AData, AOffset)) or (UInt64(GetLE32(AData, AOffset + 4)) shl 32);
end;

{ XXH64 }

function Rotl64(A: UInt64; R: Integer): UInt64; inline;
begin
  Result := (A shl R) or (A shr (64 - R));
end;

function Xxh64Round(AAcc, AInput: UInt64): UInt64; inline;
begin
  Result := Rotl64(AAcc + AInput * XXH64_P2, 31) * XXH64_P1;
end;

function Xxh64Merge(AAcc, AValue: UInt64): UInt64; inline;
begin
  Result := (AAcc xor Xxh64Round(0, AValue)) * XXH64_P1 + XXH64_P4;
end;

function KafkaXxHash64(const AData: TBytes; AOffset, ACount: Integer;
  ASeed: UInt64): UInt64;
var
  P, LEnd: Integer;
  V1, V2, V3, V4, H: UInt64;
begin
  P := AOffset;
  LEnd := AOffset + ACount;
  if ACount >= 32 then
  begin
    V1 := ASeed + XXH64_P1 + XXH64_P2;
    V2 := ASeed + XXH64_P2;
    V3 := ASeed;
    V4 := ASeed - XXH64_P1;
    while P <= LEnd - 32 do
    begin
      V1 := Xxh64Round(V1, GetLE64(AData, P));
      V2 := Xxh64Round(V2, GetLE64(AData, P + 8));
      V3 := Xxh64Round(V3, GetLE64(AData, P + 16));
      V4 := Xxh64Round(V4, GetLE64(AData, P + 24));
      Inc(P, 32);
    end;
    H := Rotl64(V1, 1) + Rotl64(V2, 7) + Rotl64(V3, 12) + Rotl64(V4, 18);
    H := Xxh64Merge(H, V1);
    H := Xxh64Merge(H, V2);
    H := Xxh64Merge(H, V3);
    H := Xxh64Merge(H, V4);
  end
  else
    H := ASeed + XXH64_P5;
  H := H + UInt64(ACount);
  while P + 8 <= LEnd do
  begin
    H := H xor Xxh64Round(0, GetLE64(AData, P));
    H := Rotl64(H, 27) * XXH64_P1 + XXH64_P4;
    Inc(P, 8);
  end;
  if P + 4 <= LEnd then
  begin
    H := H xor (UInt64(GetLE32(AData, P)) * XXH64_P1);
    H := Rotl64(H, 23) * XXH64_P2 + XXH64_P3;
    Inc(P, 4);
  end;
  while P < LEnd do
  begin
    H := H xor (UInt64(AData[P]) * XXH64_P5);
    H := Rotl64(H, 11) * XXH64_P1;
    Inc(P);
  end;
  H := H xor (H shr 33);
  H := H * XXH64_P2;
  H := H xor (H shr 29);
  H := H * XXH64_P3;
  H := H xor (H shr 32);
  Result := H;
end;

{ TZstdBits }

procedure TZstdBits.Init(const AData: TBytes; AStart, AEnd: Integer; const AWhat: string);
var
  B: Byte;
begin
  if AEnd <= AStart then
    ZstdError('an empty %s', [AWhat]);
  B := AData[AEnd - 1];
  if B = 0 then
    ZstdError('a %s whose last byte is 0 (no end mark)', [AWhat]);
  Data := AData;
  Base := AStart;
  Bits := 8 * (AEnd - AStart - 1) + HighBit(B);
end;

function TZstdBits.Peek(N: Integer): Cardinal;
var
  Lo, Hi, I: Integer;
  V: UInt64;
begin
  // The N bits below the Bits read so far, the first of them the value's
  // highest; N <= 32.
  Result := 0;
  if N = 0 then
    Exit;
  Hi := Bits;
  if Hi <= 0 then
    Exit;
  Lo := Hi - N;
  V := 0;
  if Lo < 0 then
  begin
    // Fewer than N bits left: they are the value's top, zeros below.
    for I := (Hi - 1) shr 3 downto 0 do
      V := (V shl 8) or Data[Base + I];
    Result := Cardinal((V and ((UInt64(1) shl Hi) - 1)) shl (-Lo));
    Exit;
  end;
  for I := (Hi - 1) shr 3 downto Lo shr 3 do
    V := (V shl 8) or Data[Base + I];
  Result := Cardinal((V shr (Lo and 7)) and ((UInt64(1) shl N) - 1));
end;

function TZstdBits.Read(N: Integer): Cardinal;
begin
  Result := Peek(N);
  Dec(Bits, N);
end;

{ TZstdOutput }

procedure TZstdOutput.Reserve(ACount: Int64);
var
  LNeed, LCap: Int64;
begin
  LNeed := Int64(Len) + ACount;
  if LNeed > Limit then
    LimitError(Limit, -1);
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

procedure TZstdOutput.Append(const ASource: TBytes; AOffset, ACount: Integer);
begin
  if ACount <= 0 then
    Exit;
  Reserve(ACount);
  Move(ASource[AOffset], Data[Len], ACount);
  Inc(Len, ACount);
end;

procedure TZstdOutput.Fill(AValue: Byte; ACount: Integer);
begin
  if ACount <= 0 then
    Exit;
  Reserve(ACount);
  FillChar(Data[Len], ACount, AValue);
  Inc(Len, ACount);
end;

procedure TZstdOutput.CopyBack(ADistance, ACount: Integer);
var
  I, S: Integer;
begin
  Reserve(ACount);
  S := Len - ADistance;
  if ADistance >= ACount then
    Move(Data[S], Data[Len], ACount)
  else
    // Overlapping: a byte this copy wrote is read again (a run).
    for I := 0 to ACount - 1 do
      Data[Len + I] := Data[S + I];
  Inc(Len, ACount);
end;

{ FSE }

/// Fills ATable from the normalized counts ACounts[0..ASymbols - 1] of a
/// table of 1 shl ALog states (RFC 8878, 4.1.1; fse_decompress.c).
procedure BuildFseTable(const ACounts: TZstdCounts; ASymbols, ALog: Integer;
  var ATable: TFseTable);
var
  LSize, LHigh, LMask, LStep, LPos, S, I, U, LNext, LBits: Integer;
  LNextState: TZstdCounts;
begin
  LSize := 1 shl ALog;
  ATable.Log := ALog;
  ATable.Entries := nil;
  SetLength(ATable.Entries, LSize);
  // "Less than 1" symbols take the last states, one each.
  LHigh := LSize - 1;
  for S := 0 to ASymbols - 1 do
    if ACounts[S] = -1 then
    begin
      ATable.Entries[LHigh].Symbol := Byte(S);
      Dec(LHigh);
      LNextState[S] := 1;
    end
    else
      LNextState[S] := ACounts[S];
  LMask := LSize - 1;
  LStep := (LSize shr 1) + (LSize shr 3) + 3;
  LPos := 0;
  for S := 0 to ASymbols - 1 do
    for I := 1 to ACounts[S] do
    begin
      ATable.Entries[LPos].Symbol := Byte(S);
      repeat
        LPos := (LPos + LStep) and LMask;
      until LPos <= LHigh;
    end;
  if LPos <> 0 then
    ZstdError('an FSE table whose symbols don''t spread over it', []);
  for U := 0 to LSize - 1 do
  begin
    S := ATable.Entries[U].Symbol;
    LNext := LNextState[S];
    Inc(LNextState[S]);
    LBits := ALog - HighBit(Cardinal(LNext));
    ATable.Entries[U].NbBits := Byte(LBits);
    ATable.Entries[U].NewState := Word((LNext shl LBits) - LSize);
  end;
end;

/// Reads an FSE table description (RFC 8878, 4.1.1) from AIn[P..AEnd) and
/// moves P past it. Its accuracy log is at most AMaxLog, its symbols at
/// most AMaxSymbol.
procedure ReadFseTable(const AIn: TBytes; var P: Integer; AEnd, AMaxLog, AMaxSymbol: Integer;
  var ATable: TFseTable; const AWhat: string);
var
  LBitPos: Int64;

  function Bits(N: Integer): Cardinal;
  var
    I, K: Integer;
    LByte: Int64;
  begin
    // The next N bits, the first the lowest; past AEnd, zeros.
    Result := 0;
    for K := 0 to N - 1 do
    begin
      LByte := P + ((LBitPos + K) shr 3);
      if LByte < AEnd then
      begin
        I := Integer(LByte);
        Result := Result or (Cardinal((AIn[I] shr ((LBitPos + K) and 7)) and 1) shl K);
      end;
    end;
  end;

var
  LCounts: TZstdCounts;
  LLog, LRemaining, LThreshold, LNbBits, LSymbol, LMax, LCount, LRepeat: Integer;
  V: Cardinal;
  LPrevious0: Boolean;
  LUsed: Int64;
begin
  if P >= AEnd then
    ZstdError('the %s table is cut short', [AWhat]);
  LBitPos := 0;
  LLog := Integer(Bits(4)) + FSE_MIN_LOG;
  Inc(LBitPos, 4);
  if LLog > AMaxLog then
    ZstdError('the %s table has accuracy log %d, at most %d allowed', [AWhat, LLog, AMaxLog]);
  LRemaining := (1 shl LLog) + 1;
  LThreshold := 1 shl LLog;
  LNbBits := LLog + 1;
  LSymbol := 0;
  LPrevious0 := False;
  while (LRemaining > 1) and (LSymbol <= AMaxSymbol) do
  begin
    if LPrevious0 then
    begin
      // After a zero, 2-bit counts of more zeros (3 means "and more").
      LRepeat := LSymbol;
      while Bits(2) = 3 do
      begin
        Inc(LRepeat, 3);
        Inc(LBitPos, 2);
        if LRepeat > AMaxSymbol then
          Break;
      end;
      Inc(LRepeat, Integer(Bits(2)));
      Inc(LBitPos, 2);
      if LRepeat > AMaxSymbol then
        ZstdError('the %s table has a symbol above %d', [AWhat, AMaxSymbol]);
      while LSymbol < LRepeat do
      begin
        LCounts[LSymbol] := 0;
        Inc(LSymbol);
      end;
    end;
    LMax := (2 * LThreshold - 1) - LRemaining;
    V := Bits(LNbBits);
    if Integer(V and Cardinal(LThreshold - 1)) < LMax then
    begin
      LCount := Integer(V and Cardinal(LThreshold - 1));
      Inc(LBitPos, LNbBits - 1);
    end
    else
    begin
      LCount := Integer(V and Cardinal(2 * LThreshold - 1));
      if LCount >= LThreshold then
        Dec(LCount, LMax);
      Inc(LBitPos, LNbBits);
    end;
    Dec(LCount);
    if LCount < 0 then
      Dec(LRemaining, -LCount)
    else
      Dec(LRemaining, LCount);
    LCounts[LSymbol] := LCount;
    Inc(LSymbol);
    LPrevious0 := LCount = 0;
    if LRemaining < LThreshold then
    begin
      if LRemaining <= 1 then
        Break;
      LNbBits := HighBit(Cardinal(LRemaining)) + 1;
      LThreshold := 1 shl (LNbBits - 1);
    end;
  end;
  if LRemaining <> 1 then
    ZstdError('the %s table''s probabilities don''t add up to %d', [AWhat, 1 shl LLog]);
  LUsed := (LBitPos + 7) shr 3;
  if LUsed > AEnd - P then
    ZstdError('the %s table is cut short', [AWhat]);
  Inc(P, Integer(LUsed));
  BuildFseTable(LCounts, LSymbol, LLog, ATable);
end;

procedure RleTable(ASymbol: Byte; var ATable: TFseTable);
begin
  ATable.Log := 0;
  ATable.Entries := nil;
  SetLength(ATable.Entries, 1);
  ATable.Entries[0].Symbol := ASymbol;
  ATable.Entries[0].NbBits := 0;
  ATable.Entries[0].NewState := 0;
end;

procedure DefaultTable(const ADistribution: array of Integer; ALog: Integer;
  var ATable: TFseTable);
var
  LCounts: TZstdCounts;
  I: Integer;
begin
  for I := 0 to High(ADistribution) do
    LCounts[I] := ADistribution[I];
  BuildFseTable(LCounts, Length(ADistribution), ALog, ATable);
end;

{ Huffman }

/// Reads a Huffman tree description (RFC 8878, 4.2.1; libzstd's
/// HUF_readStats) from AIn[P..AEnd) into ATable and moves P past it.
procedure ReadHuffmanTree(const AIn: TBytes; var P: Integer; AEnd: Integer;
  var ATable: THufTable);
var
  LWeights: array[0..255] of Byte;
  LRank: array[0..HUF_TABLELOG_MAX] of Integer;
  LStart: array[0..HUF_TABLELOG_MAX + 1] of Integer;
  LHeader, LCount, LBytes, I, W, LMaxBits, LLength, LRest, LTotal, LEnd: Integer;
  LWeightTotal: Int64;
  LFse: TFseTable;
  LBits: TZstdBits;
  S1, S2: Cardinal;
  E: TFseEntry;
begin
  if P >= AEnd then
    ZstdError('the Huffman tree is cut short', []);
  LHeader := AIn[P];
  Inc(P);
  if LHeader >= 128 then
  begin
    // The weights as they are, two per byte, the first in the high half.
    LCount := LHeader - 127;
    LBytes := (LCount + 1) div 2;
    if LBytes > AEnd - P then
      ZstdError('the Huffman weights are cut short', []);
    for I := 0 to LCount - 1 do
      if (I and 1) = 0 then
        LWeights[I] := AIn[P + I div 2] shr 4
      else
        LWeights[I] := AIn[P + I div 2] and 15;
    Inc(P, LBytes);
  end
  else
  begin
    // FSE-compressed weights: two states over one bitstream, taking turns,
    // until it runs out (fse_decompress.c's tail).
    if LHeader > AEnd - P then
      ZstdError('the Huffman weights (%d bytes) are cut short', [LHeader]);
    LEnd := P + LHeader;
    ReadFseTable(AIn, P, LEnd, HUF_WEIGHTS_MAX_LOG, 255, LFse, 'Huffman weights');
    LBits.Init(AIn, P, LEnd, 'Huffman weights bitstream');
    P := LEnd;
    S1 := LBits.Read(LFse.Log);
    S2 := LBits.Read(LFse.Log);
    LCount := 0;
    while True do
    begin
      if LCount > 253 then
        ZstdError('more than 255 Huffman weights', []);
      E := LFse.Entries[S1];
      LWeights[LCount] := E.Symbol;
      Inc(LCount);
      S1 := E.NewState + LBits.Read(E.NbBits);
      if LBits.Bits < 0 then
      begin
        LWeights[LCount] := LFse.Entries[S2].Symbol;
        Inc(LCount);
        Break;
      end;
      E := LFse.Entries[S2];
      LWeights[LCount] := E.Symbol;
      Inc(LCount);
      S2 := E.NewState + LBits.Read(E.NbBits);
      if LBits.Bits < 0 then
      begin
        LWeights[LCount] := LFse.Entries[S1].Symbol;
        Inc(LCount);
        Break;
      end;
    end;
  end;
  for W := 0 to HUF_TABLELOG_MAX do
    LRank[W] := 0;
  LWeightTotal := 0;
  for I := 0 to LCount - 1 do
  begin
    W := LWeights[I];
    if W > HUF_TABLELOG_MAX then
      ZstdError('a Huffman weight of %d, at most %d allowed', [W, HUF_TABLELOG_MAX]);
    Inc(LRank[W]);
    Inc(LWeightTotal, (Int64(1) shl W) shr 1);
  end;
  if LWeightTotal = 0 then
    ZstdError('Huffman weights that are all 0', []);
  // The last symbol's weight is implied: what brings the total to a power
  // of 2.
  LMaxBits := HighBit(Cardinal(LWeightTotal)) + 1;
  if LMaxBits > HUF_TABLELOG_MAX then
    ZstdError('Huffman codes of %d bits, at most %d allowed', [LMaxBits, HUF_TABLELOG_MAX]);
  LTotal := 1 shl LMaxBits;
  LRest := LTotal - Integer(LWeightTotal);
  if (1 shl HighBit(Cardinal(LRest))) <> LRest then
    ZstdError('Huffman weights that leave no power of 2 for the last symbol', []);
  W := HighBit(Cardinal(LRest)) + 1;
  LWeights[LCount] := Byte(W);
  Inc(LRank[W]);
  Inc(LCount);
  if (LRank[1] < 2) or ((LRank[1] and 1) <> 0) then
    ZstdError('Huffman weights that make no prefix code', []);
  // Each symbol of weight W takes 1 shl (W - 1) entries, the lower weights
  // first, in symbol order within a weight (huf_decompress.c's X1 table).
  LStart[1] := 0;
  for W := 1 to LMaxBits do
    LStart[W + 1] := LStart[W] + (LRank[W] shl (W - 1));
  ATable.MaxBits := LMaxBits;
  ATable.Entries := nil;
  SetLength(ATable.Entries, LTotal);
  for I := 0 to LCount - 1 do
  begin
    W := LWeights[I];
    if W = 0 then
      Continue;
    LLength := 1 shl (W - 1);
    for LBytes := LStart[W] to LStart[W] + LLength - 1 do
    begin
      ATable.Entries[LBytes].Symbol := Byte(I);
      ATable.Entries[LBytes].NbBits := Byte(LMaxBits + 1 - W);
    end;
    Inc(LStart[W], LLength);
  end;
end;

/// Decodes ACount literals from the Huffman stream AIn[AStart..AEnd) into
/// ALiterals from ADest; the stream must end exactly there.
procedure DecodeHuffmanStream(const AIn: TBytes; AStart, AEnd: Integer;
  const ATable: THufTable; var ALiterals: TBytes; ADest, ACount: Integer);
var
  LBits: TZstdBits;
  I: Integer;
  E: THufEntry;
begin
  LBits.Init(AIn, AStart, AEnd, 'Huffman stream');
  for I := ADest to ADest + ACount - 1 do
  begin
    E := ATable.Entries[LBits.Peek(ATable.MaxBits)];
    ALiterals[I] := E.Symbol;
    Dec(LBits.Bits, E.NbBits);
    if LBits.Bits < 0 then
      ZstdError('a Huffman stream ends before its %d literals', [ACount]);
  end;
  if LBits.Bits <> 0 then
    ZstdError('a Huffman stream has %d bits left after its literals', [LBits.Bits]);
end;

{ Blocks }

/// Reads the literals section of the compressed block AIn[P..AEnd) into
/// AFrame.Literals and moves P past it (RFC 8878, 3.1.1.3.1).
procedure DecodeLiterals(const AIn: TBytes; var P: Integer; AEnd: Integer;
  var AFrame: TZstdFrame);
var
  B0, LType, LFormat, LHeaderSize, LStreams, LSizeBits, I, Q, LStreamsEnd, LSegment: Integer;
  LRegenerated, LCompressed: Integer;
  LSizes: array[0..3] of Integer;
  H: UInt64;
begin
  B0 := AIn[P];
  LType := B0 and 3;
  LFormat := (B0 shr 2) and 3;
  if LType <= 1 then
  begin
    // Raw (0) or RLE (1): a size of 5, 12 or 20 bits.
    case LFormat of
      1:
        begin
          LHeaderSize := 2;
          if AEnd - P < 2 then
            ZstdError('a literals header is cut short', []);
          LRegenerated := Integer(GetLE16(AIn, P) shr 4);
        end;
      3:
        begin
          LHeaderSize := 3;
          if AEnd - P < 3 then
            ZstdError('a literals header is cut short', []);
          LRegenerated := Integer((GetLE16(AIn, P) or (Cardinal(AIn[P + 2]) shl 16)) shr 4);
        end;
    else
      begin
        LHeaderSize := 1;
        LRegenerated := B0 shr 3;
      end;
    end;
    if LRegenerated > AFrame.BlockMax then
      ZstdError('%d literals in a block of at most %d bytes', [LRegenerated, AFrame.BlockMax]);
    Inc(P, LHeaderSize);
    if Length(AFrame.Literals) < LRegenerated then
      SetLength(AFrame.Literals, ZSTD_BLOCK_SIZE_MAX);
    AFrame.LiteralCount := LRegenerated;
    if LType = 0 then
    begin
      if LRegenerated > AEnd - P then
        ZstdError('%d raw literals with %d bytes left in the block', [LRegenerated, AEnd - P]);
      if LRegenerated > 0 then
        Move(AIn[P], AFrame.Literals[0], LRegenerated);
      Inc(P, LRegenerated);
    end
    else
    begin
      if P >= AEnd then
        ZstdError('RLE literals with no byte', []);
      if LRegenerated > 0 then
        FillChar(AFrame.Literals[0], LRegenerated, AIn[P]);
      Inc(P);
    end;
    Exit;
  end;
  // Huffman: with its tree (2) or the previous block's (3, "treeless").
  case LFormat of
    0:
      begin
        LStreams := 1;
        LHeaderSize := 3;
        LSizeBits := 10;
      end;
    1:
      begin
        LStreams := 4;
        LHeaderSize := 3;
        LSizeBits := 10;
      end;
    2:
      begin
        LStreams := 4;
        LHeaderSize := 4;
        LSizeBits := 14;
      end;
  else
    begin
      LStreams := 4;
      LHeaderSize := 5;
      LSizeBits := 18;
    end;
  end;
  if AEnd - P < LHeaderSize then
    ZstdError('a literals header is cut short', []);
  H := 0;
  for I := LHeaderSize - 1 downto 0 do
    H := (H shl 8) or AIn[P + I];
  LRegenerated := Integer((H shr 4) and ((UInt64(1) shl LSizeBits) - 1));
  LCompressed := Integer((H shr (4 + LSizeBits)) and ((UInt64(1) shl LSizeBits) - 1));
  Inc(P, LHeaderSize);
  if LRegenerated > AFrame.BlockMax then
    ZstdError('%d literals in a block of at most %d bytes', [LRegenerated, AFrame.BlockMax]);
  if (LStreams = 4) and (LRegenerated < ZSTD_MIN_LITERALS_FOR_4_STREAMS) then
    ZstdError('%d literals in 4 Huffman streams, at least %d needed',
      [LRegenerated, ZSTD_MIN_LITERALS_FOR_4_STREAMS]);
  if LCompressed > AEnd - P then
    ZstdError('%d bytes of Huffman literals with %d left in the block', [LCompressed, AEnd - P]);
  Q := P;
  LStreamsEnd := P + LCompressed;
  if LType = 2 then
  begin
    ReadHuffmanTree(AIn, Q, LStreamsEnd, AFrame.Huffman);
    AFrame.HasHuffman := True;
  end
  else if not AFrame.HasHuffman then
    ZstdError('literals with the previous Huffman tree, and there was none in the frame', []);
  if Length(AFrame.Literals) < LRegenerated then
    SetLength(AFrame.Literals, ZSTD_BLOCK_SIZE_MAX);
  AFrame.LiteralCount := LRegenerated;
  if LStreams = 1 then
    DecodeHuffmanStream(AIn, Q, LStreamsEnd, AFrame.Huffman, AFrame.Literals, 0, LRegenerated)
  else
  begin
    // A jump table of three sizes; the fourth stream takes the rest.
    if LStreamsEnd - Q < 10 then
      ZstdError('4 Huffman streams in %d bytes', [LStreamsEnd - Q]);
    LSizes[0] := Integer(GetLE16(AIn, Q));
    LSizes[1] := Integer(GetLE16(AIn, Q + 2));
    LSizes[2] := Integer(GetLE16(AIn, Q + 4));
    Inc(Q, 6);
    LSizes[3] := LStreamsEnd - Q - LSizes[0] - LSizes[1] - LSizes[2];
    if LSizes[3] < 1 then
      ZstdError('a Huffman jump table larger than its streams', []);
    LSegment := (LRegenerated + 3) div 4;
    for I := 0 to 3 do
    begin
      if I < 3 then
        DecodeHuffmanStream(AIn, Q, Q + LSizes[I], AFrame.Huffman, AFrame.Literals,
          I * LSegment, LSegment)
      else
        DecodeHuffmanStream(AIn, Q, Q + LSizes[I], AFrame.Huffman, AFrame.Literals,
          3 * LSegment, LRegenerated - 3 * LSegment);
      Inc(Q, LSizes[I]);
    end;
  end;
  P := LStreamsEnd;
end;

/// One of the sequences section's three tables, by its mode (RFC 8878,
/// 3.1.1.3.2.1).
procedure ReadSequenceTable(const AIn: TBytes; var P: Integer; AEnd, AMode, AMaxLog,
  AMaxSymbol: Integer; const ADefault: TFseTable; AHasTables: Boolean;
  var ATable: TFseTable; const AWhat: string);
begin
  case AMode of
    0:
      ATable := ADefault;
    1:
      begin
        if P >= AEnd then
          ZstdError('the %s RLE symbol is cut short', [AWhat]);
        if AIn[P] > AMaxSymbol then
          ZstdError('the %s RLE symbol %d, at most %d allowed', [AWhat, AIn[P], AMaxSymbol]);
        RleTable(AIn[P], ATable);
        Inc(P);
      end;
    2:
      ReadFseTable(AIn, P, AEnd, AMaxLog, AMaxSymbol, ATable, AWhat);
  else
    if not AHasTables then
      ZstdError('the %s table repeated, and there was none before it in the frame', [AWhat]);
  end;
end;

/// The block AIn[AStart..AEnd) of type compressed: literals, then
/// sequences executed onto AOut.
procedure DecodeCompressedBlock(const AIn: TBytes; AStart, AEnd: Integer;
  var AFrame: TZstdFrame; var AOut: TZstdOutput);
var
  P, B0, LModes, I, LLiteral, LBlockStart: Integer;
  LCount: Integer;
  LBits: TZstdBits;
  LLState, LOfState, LMlState, LOfCode, LMlCode, LLlCode: Cardinal;
  LOffsetValue, LOffset, LMatch, LLength: Int64;
  E: TFseEntry;
begin
  if AEnd - AStart < ZSTD_MIN_CBLOCK_SIZE then
    ZstdError('a compressed block of %d bytes', [AEnd - AStart]);
  P := AStart;
  LBlockStart := AOut.Len;
  DecodeLiterals(AIn, P, AEnd, AFrame);
  if P >= AEnd then
    ZstdError('a block ends before its sequences', []);
  B0 := AIn[P];
  if B0 < 128 then
  begin
    LCount := B0;
    Inc(P);
  end
  else if B0 < 255 then
  begin
    if AEnd - P < 2 then
      ZstdError('the number of sequences is cut short', []);
    LCount := ((B0 - 128) shl 8) + AIn[P + 1];
    Inc(P, 2);
  end
  else
  begin
    if AEnd - P < 3 then
      ZstdError('the number of sequences is cut short', []);
    LCount := Integer(GetLE16(AIn, P + 1)) + $7F00;
    Inc(P, 3);
  end;
  LLiteral := 0;
  if LCount > 0 then
  begin
    if P >= AEnd then
      ZstdError('the sequences'' modes are cut short', []);
    LModes := AIn[P];
    Inc(P);
    if (LModes and 3) <> 0 then
      ZstdError('reserved bits set in the sequences'' modes (%.2x)', [LModes]);
    ReadSequenceTable(AIn, P, AEnd, LModes shr 6, LL_MAX_LOG, LL_MAX_SYMBOL, GLlDefault,
      AFrame.HasTables, AFrame.LL, 'literal lengths');
    ReadSequenceTable(AIn, P, AEnd, (LModes shr 4) and 3, OF_MAX_LOG, OF_MAX_SYMBOL,
      GOfDefault, AFrame.HasTables, AFrame.OF_, 'offsets');
    ReadSequenceTable(AIn, P, AEnd, (LModes shr 2) and 3, ML_MAX_LOG, ML_MAX_SYMBOL,
      GMlDefault, AFrame.HasTables, AFrame.ML, 'match lengths');
    LBits.Init(AIn, P, AEnd, 'sequences bitstream');
    LLState := LBits.Read(AFrame.LL.Log);
    LOfState := LBits.Read(AFrame.OF_.Log);
    LMlState := LBits.Read(AFrame.ML.Log);
    for I := 1 to LCount do
    begin
      LOfCode := AFrame.OF_.Entries[LOfState].Symbol;
      LMlCode := AFrame.ML.Entries[LMlState].Symbol;
      LLlCode := AFrame.LL.Entries[LLState].Symbol;
      if (LOfCode > OF_MAX_SYMBOL) or (LMlCode > ML_MAX_SYMBOL) or (LLlCode > LL_MAX_SYMBOL) then
        ZstdError('a sequence code out of range', []);
      // Extra bits: offset, match length, literal length.
      LOffsetValue := (Int64(1) shl LOfCode) + LBits.Read(Integer(LOfCode));
      LMatch := Int64(ML_BASE[LMlCode]) + LBits.Read(ML_BITS[LMlCode]);
      LLength := Int64(LL_BASE[LLlCode]) + LBits.Read(LL_BITS[LLlCode]);
      // The states, but after the last sequence: literal, match, offset.
      if I < LCount then
      begin
        E := AFrame.LL.Entries[LLState];
        LLState := E.NewState + LBits.Read(E.NbBits);
        E := AFrame.ML.Entries[LMlState];
        LMlState := E.NewState + LBits.Read(E.NbBits);
        E := AFrame.OF_.Entries[LOfState];
        LOfState := E.NewState + LBits.Read(E.NbBits);
      end;
      if LBits.Bits < 0 then
        ZstdError('the sequences bitstream ends before its %d sequences', [LCount]);
      // The repeat offsets (RFC 8878, 3.1.1.5).
      if LOffsetValue > 3 then
      begin
        LOffset := LOffsetValue - 3;
        AFrame.Rep[2] := AFrame.Rep[1];
        AFrame.Rep[1] := AFrame.Rep[0];
        AFrame.Rep[0] := LOffset;
      end
      else
      begin
        if LLength = 0 then
          Inc(LOffsetValue);
        // 1 to 4 here; a case selector of 32 bits for Delphi.
        case Integer(LOffsetValue) of
          1:
            LOffset := AFrame.Rep[0];
          2:
            begin
              LOffset := AFrame.Rep[1];
              AFrame.Rep[1] := AFrame.Rep[0];
              AFrame.Rep[0] := LOffset;
            end;
          3:
            begin
              LOffset := AFrame.Rep[2];
              AFrame.Rep[2] := AFrame.Rep[1];
              AFrame.Rep[1] := AFrame.Rep[0];
              AFrame.Rep[0] := LOffset;
            end;
        else
          begin
            LOffset := AFrame.Rep[0] - 1;
            AFrame.Rep[2] := AFrame.Rep[1];
            AFrame.Rep[1] := AFrame.Rep[0];
            AFrame.Rep[0] := LOffset;
          end;
        end;
      end;
      // Execute: the literals, then the match.
      if LLength > AFrame.LiteralCount - LLiteral then
        ZstdError('a sequence takes %d literals with %d left', [LLength,
          AFrame.LiteralCount - LLiteral]);
      if Int64(AOut.Len - LBlockStart) + LLength + LMatch > AFrame.BlockMax then
        ZstdError('a block decompresses to more than its %d bytes', [AFrame.BlockMax]);
      AOut.Append(AFrame.Literals, LLiteral, Integer(LLength));
      Inc(LLiteral, Integer(LLength));
      if (LOffset <= 0) or (LOffset > AOut.Len - AFrame.Start) then
        ZstdError('a match from %d bytes back, with %d bytes before it in the frame',
          [LOffset, AOut.Len - AFrame.Start]);
      AOut.CopyBack(Integer(LOffset), Integer(LMatch));
    end;
    if LBits.Bits <> 0 then
      ZstdError('the sequences bitstream has %d bits left after its sequences', [LBits.Bits]);
    AFrame.HasTables := True;
  end
  else if P <> AEnd then
    ZstdError('%d bytes after a block''s sequences section', [AEnd - P]);
  // The literals no sequence took come last.
  if Int64(AOut.Len - LBlockStart) + AFrame.LiteralCount - LLiteral > AFrame.BlockMax then
    ZstdError('a block decompresses to more than its %d bytes', [AFrame.BlockMax]);
  AOut.Append(AFrame.Literals, LLiteral, AFrame.LiteralCount - LLiteral);
end;

{ Frames }

/// The frame at AIn[P..) (its magic number checked): its blocks onto AOut,
/// and P past it.
procedure DecodeFrame(const AIn: TBytes; var P: Integer; var AOut: TZstdOutput);
var
  N, LHeaderSize, LDictSize, LFcsSize, LBlockSize, LType, I: Integer;
  FHD, WD: Byte;
  LSingle, LChecksum, LHasSize, LLast: Boolean;
  LWindow, LSize: Int64;
  LDictId, H: Cardinal;
  LFcs: UInt64;
  LFrame: TZstdFrame;
begin
  N := Length(AIn);
  Inc(P, 4);
  if P >= N then
    ZstdError('the frame header is cut short', []);
  FHD := AIn[P];
  Inc(P);
  if (FHD and $08) <> 0 then
    ZstdError('a reserved bit set in the frame header (%.2x)', [FHD]);
  LSingle := (FHD and $20) <> 0;
  LChecksum := (FHD and $04) <> 0;
  case FHD and 3 of
    0: LDictSize := 0;
    1: LDictSize := 1;
    2: LDictSize := 2;
  else
    LDictSize := 4;
  end;
  case FHD shr 6 of
    0:
      if LSingle then
        LFcsSize := 1
      else
        LFcsSize := 0;
    1: LFcsSize := 2;
    2: LFcsSize := 4;
  else
    LFcsSize := 8;
  end;
  LHeaderSize := LDictSize + LFcsSize;
  if not LSingle then
    Inc(LHeaderSize);
  if LHeaderSize > N - P then
    ZstdError('the frame header is cut short', []);
  LWindow := 0;
  if not LSingle then
  begin
    WD := AIn[P];
    Inc(P);
    LWindow := Int64(1) shl (10 + (WD shr 3));
    LWindow := LWindow + (LWindow div 8) * (WD and 7);
  end;
  LDictId := 0;
  for I := LDictSize - 1 downto 0 do
    LDictId := (LDictId shl 8) or AIn[P + I];
  Inc(P, LDictSize);
  if LDictId <> 0 then
    ZstdError('a dictionary (id %d), which Kafka doesn''t use (Java refuses it too)',
      [Int64(LDictId)]);
  LHasSize := LFcsSize > 0;
  LFcs := 0;
  case LFcsSize of
    1: LFcs := AIn[P];
    2: LFcs := GetLE16(AIn, P) + 256;
    4: LFcs := GetLE32(AIn, P);
    8: LFcs := GetLE64(AIn, P);
  end;
  Inc(P, LFcsSize);
  if LHasSize and (LFcs > UInt64(AOut.Limit - AOut.Len)) then
  begin
    if LFcs > UInt64(High(Int64)) then
      LimitError(AOut.Limit, High(Int64));
    LimitError(AOut.Limit, Int64(LFcs));
  end;
  if LSingle then
    LWindow := Int64(LFcs);
  if LWindow > ZSTD_WINDOW_MAX then
    ZstdError('a window of %d bytes, above the %d Java''s decoder takes', [LWindow,
      ZSTD_WINDOW_MAX]);
  LFrame.Start := AOut.Len;
  if LWindow < ZSTD_BLOCK_SIZE_MAX then
    LFrame.BlockMax := Integer(LWindow)
  else
    LFrame.BlockMax := ZSTD_BLOCK_SIZE_MAX;
  LFrame.HasHuffman := False;
  LFrame.HasTables := False;
  LFrame.Rep[0] := 1;
  LFrame.Rep[1] := 4;
  LFrame.Rep[2] := 8;
  LFrame.Literals := nil;
  LFrame.LiteralCount := 0;
  // Allocated once when the frame says its size (bounded above).
  if LHasSize then
    AOut.Reserve(Int64(LFcs));
  repeat
    if N - P < 3 then
      ZstdError('a block header is cut short', []);
    H := Cardinal(AIn[P]) or (Cardinal(AIn[P + 1]) shl 8) or (Cardinal(AIn[P + 2]) shl 16);
    Inc(P, 3);
    LLast := (H and 1) <> 0;
    LType := (H shr 1) and 3;
    LBlockSize := Integer(H shr 3);
    if LType = 3 then
      ZstdError('a block of the reserved type 3', []);
    if LBlockSize > LFrame.BlockMax then
      ZstdError('a block of %d bytes, above the frame''s %d', [LBlockSize, LFrame.BlockMax]);
    case LType of
      0:
        begin
          if LBlockSize > N - P then
            ZstdError('a raw block of %d bytes with %d left', [LBlockSize, N - P]);
          AOut.Append(AIn, P, LBlockSize);
          Inc(P, LBlockSize);
        end;
      1:
        begin
          if P >= N then
            ZstdError('an RLE block with no byte', []);
          AOut.Fill(AIn[P], LBlockSize);
          Inc(P);
        end;
    else
      begin
        if LBlockSize > N - P then
          ZstdError('a compressed block of %d bytes with %d left', [LBlockSize, N - P]);
        DecodeCompressedBlock(AIn, P, P + LBlockSize, LFrame, AOut);
        Inc(P, LBlockSize);
      end;
    end;
    if LHasSize and (UInt64(AOut.Len - LFrame.Start) > LFcs) then
      ZstdError('the frame holds more than the %d bytes it says', [Int64(LFcs)]);
  until LLast;
  LSize := AOut.Len - LFrame.Start;
  if LHasSize and (UInt64(LSize) <> LFcs) then
    ZstdError('the frame says %d bytes and holds %d', [Int64(LFcs), LSize]);
  if LChecksum then
  begin
    if N - P < 4 then
      ZstdError('the content checksum is cut short', []);
    if Cardinal(KafkaXxHash64(AOut.Data, LFrame.Start, Integer(LSize), 0)) <>
      GetLE32(AIn, P) then
      ZstdError('the content checksum doesn''t match', []);
    Inc(P, 4);
  end;
end;

function KafkaZstdDecompress(const AData: TBytes; AMaxSize: Integer): TBytes;
var
  LOut: TZstdOutput;
  N, P, LFrames: Integer;
  LMagic, LSkip: Cardinal;
begin
  N := Length(AData);
  if N < 4 then
    ZstdError('%d bytes is shorter than a frame''s magic number', [N]);
  LOut.Data := nil;
  LOut.Len := 0;
  LOut.Limit := AMaxSize;
  P := 0;
  LFrames := 0;
  while P < N do
  begin
    if N - P < 4 then
      ZstdError('%d bytes after the last frame', [N - P]);
    LMagic := GetLE32(AData, P);
    if (LMagic and ZSTD_SKIPPABLE_MASK) = ZSTD_SKIPPABLE_MAGIC then
    begin
      // A skippable frame: a size and that many bytes, read past.
      if N - P < 8 then
        ZstdError('a skippable frame''s header is cut short', []);
      LSkip := GetLE32(AData, P + 4);
      if LSkip > Cardinal(N - P - 8) then
        ZstdError('a skippable frame of %d bytes with %d left', [Int64(LSkip), N - P - 8]);
      Inc(P, 8 + Integer(LSkip));
      Inc(LFrames);
      Continue;
    end;
    if LMagic <> ZSTD_MAGIC then
    begin
      if (LMagic >= ZSTD_LEGACY_LOW) and (LMagic <= ZSTD_LEGACY_HIGH) then
        ZstdError('a frame of zstd 0.x''s legacy format (magic %.8x), which no Kafka ' +
          'producer writes (Java refuses it too)', [Int64(LMagic)]);
      if LFrames = 0 then
        ZstdError('not a zstd frame (magic %.8x)', [Int64(LMagic)]);
      ZstdError('%d bytes after the last frame, not a frame (magic %.8x)', [N - P,
        Int64(LMagic)]);
    end;
    DecodeFrame(AData, P, LOut);
    Inc(LFrames);
  end;
  SetLength(LOut.Data, LOut.Len);
  Result := LOut.Data;
end;

initialization
  DefaultTable(LL_DEFAULT, LL_DEFAULT_LOG, GLlDefault);
  DefaultTable(ML_DEFAULT, ML_DEFAULT_LOG, GMlDefault);
  DefaultTable(OF_DEFAULT, OF_DEFAULT_LOG, GOfDefault);

end.
