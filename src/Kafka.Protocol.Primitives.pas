unit Kafka.Protocol.Primitives;

{$I kafka.inc}

{ The protocol's primitive types: a writer that appends them to a growing
  buffer and a reader that takes them from one.

  What the Kafka protocol guide calls the primitive types
  (https://kafka.apache.org/protocol#protocol_types):

    INT8..INT64, UINT16, UINT32  big-endian, two's complement
    FLOAT64                      IEEE 754 double, big-endian
    BOOLEAN                      one byte, 0 or 1 (any non-zero reads as True)
    UNSIGNED_VARINT              7 bits per byte, least significant group
                                 first, high bit = more bytes (at most 5)
    VARINT, VARLONG              zigzag over the unsigned form (at most 5 and
                                 10 bytes); records and their headers use them
    UUID                         16 bytes
    STRING / NULLABLE_STRING     INT16 length (-1 = null), UTF-8 bytes
    COMPACT_[NULLABLE_]STRING    UNSIGNED_VARINT length + 1 (0 = null)
    BYTES / COMPACT_BYTES        the same with INT32 / UNSIGNED_VARINT + 1
    ARRAY / COMPACT_ARRAY        INT32 count (-1 = null) / UNSIGNED_VARINT
                                 count + 1 (0 = null), then the elements
    tagged fields                UNSIGNED_VARINT count, then per field its
                                 tag and size (both UNSIGNED_VARINT) and data

  The compact forms are those of the "flexible" versions of a message
  (KIP-482). Which form a field takes depends on the message's version, so
  every string, bytes and array call takes ACompact; the generated codecs
  (Kafka.Messages.*) pass it.

  Integers are written byte by byte, never with Swap/SwapEndian (their names
  and types differ between the two RTLs). Zigzag is written without shifting
  a signed value: shr on a negative Integer is a logical shift on both
  compilers, not Java's arithmetic >>, and each compiler is free in how it
  sign-extends an intermediate. Overflow and range checks are off for this
  unit: Delphi's Debug configuration turns both on, and the casts between
  signed and unsigned here reinterpret bits on purpose.

  Strings are UTF-8 on the wire and string in Pascal, converted with
  pascal-common-faa's PcStringToUtf8Bytes/PcTryUtf8BytesToString, never
  TEncoding.UTF8 (which raises on invalid bytes on Delphi and not on FPC).
  Invalid UTF-8 read from the wire raises EKafkaProtocol. Bytes fields
  (keys, values) are TBytes and never go through a string.

  A null string, bytes or array has no Pascal value of its own (an empty
  string and a nil array are also the empty value), so the nullable forms take
  and return an explicit AIsNull. }

{$OVERFLOWCHECKS OFF}
{$RANGECHECKS OFF}

interface

uses
  SysUtils,
  Kafka.Types;

type
  TKafkaInt16Array = array of Int16;
  TKafkaInt32Array = array of Int32;
  TKafkaInt64Array = array of Int64;
  TKafkaStringArray = array of string;

  { A Kafka UUID: 16 bytes, all zero for "none" (Uuid.ZERO_UUID in Java). }
  TKafkaUuid = record
    Bytes: array[0..15] of Byte;
    function IsZero: Boolean;
    function Equals(const AOther: TKafkaUuid): Boolean;
    /// The form Kafka's tools print: URL-safe base64 of the 16 bytes, without
    /// padding (22 characters), as Java's Uuid.toString.
    function ToString: string;
    class function Zero: TKafkaUuid; static;
    /// Parses ToString's form; False when AText isn't 22 characters of
    /// URL-safe base64 that decode to 16 bytes.
    class function TryParse(const AText: string; out AUuid: TKafkaUuid): Boolean; static;
  end;
  TKafkaUuidArray = array of TKafkaUuid;

  { A tagged field this library doesn't know (a newer broker's), kept as raw
    bytes so that decoding and encoding again gives back the same bytes, as
    Java's generated messages do. }
  TKafkaRawTaggedField = record
    Tag: Cardinal;
    Data: TBytes;
  end;
  TKafkaRawTaggedFields = array of TKafkaRawTaggedField;

  { Appends primitives to a buffer that grows as needed. }
  TKafkaWriter = class
  private
    FBuffer: TBytes;
    FLength: Integer;
    procedure Grow(AExtra: Integer);
  public
    constructor Create(AInitialCapacity: Integer = 256);
    procedure Clear;

    procedure WriteInt8(AValue: Int8);
    procedure WriteInt16(AValue: Int16);
    procedure WriteInt32(AValue: Int32);
    procedure WriteInt64(AValue: Int64);
    procedure WriteUInt16(AValue: Word);
    procedure WriteUInt32(AValue: Cardinal);
    procedure WriteFloat64(AValue: Double);
    procedure WriteBool(AValue: Boolean);
    procedure WriteUuid(const AValue: TKafkaUuid);

    procedure WriteUnsignedVarint(AValue: Cardinal);
    procedure WriteVarint(AValue: Int32);
    procedure WriteVarlong(AValue: Int64);

    /// A non-null string. EKafkaProtocol when longer than 32767 bytes in the
    /// classic form.
    procedure WriteString(const AValue: string; ACompact: Boolean);
    procedure WriteNullableString(const AValue: string; AIsNull: Boolean; ACompact: Boolean);
    procedure WriteBytes(const AValue: TBytes; ACompact: Boolean);
    procedure WriteNullableBytes(const AValue: TBytes; AIsNull: Boolean; ACompact: Boolean);
    /// An array's count (ACount >= 0); the caller writes the elements.
    procedure WriteArrayLength(ACount: Integer; ACompact: Boolean);
    /// A null array.
    procedure WriteNullArray(ACompact: Boolean);

    procedure WriteInt32Array(const AValue: TKafkaInt32Array; ACompact: Boolean);
    procedure WriteInt16Array(const AValue: TKafkaInt16Array; ACompact: Boolean);
    procedure WriteInt64Array(const AValue: TKafkaInt64Array; ACompact: Boolean);
    procedure WriteStringArray(const AValue: TKafkaStringArray; ACompact: Boolean);
    procedure WriteUuidArray(const AValue: TKafkaUuidArray; ACompact: Boolean);

    /// One tagged field: tag, size and AData.
    procedure WriteTaggedField(ATag: Cardinal; const AData: TBytes);
    /// The unknown fields from AIndex whose tag is below ABelowTag, advancing
    /// AIndex; with ABelowTag = High(Int64), all that remain. The generated
    /// codecs interleave them with the known ones so that every tag goes out
    /// in ascending order, as the protocol requires.
    procedure WriteRawTaggedFieldsBelow(const AFields: TKafkaRawTaggedFields;
      var AIndex: Integer; ABelowTag: Int64);

    procedure WriteRaw(const ABuffer; ACount: Integer); overload;
    procedure WriteRaw(const AData: TBytes); overload;
    /// Overwrites 4 bytes at APosition (a size written before its content).
    procedure PatchInt32(APosition: Integer; AValue: Int32);

    /// A copy of what was written.
    function ToBytes: TBytes;
    property Length: Integer read FLength;
  end;

  { Reads primitives from a byte range. Reading past the end raises
    EKafkaProtocol, as does any malformed length. The reader doesn't copy
    AData (a dynamic array is reference-counted). }
  TKafkaReader = class
  private
    FData: TBytes;
    FPosition: Integer;
    FLimit: Integer;
    procedure Need(ACount: Integer);
    function ReadLength16Or32(AWide: Boolean): Integer;
    function ReadUtf8(ALength: Integer): string;
  public
    /// Reads ACount bytes of AData from AOffset; ACount < 0 = to the end.
    constructor Create(const AData: TBytes; AOffset: Integer = 0; ACount: Integer = -1);

    function ReadInt8: Int8;
    function ReadInt16: Int16;
    function ReadInt32: Int32;
    function ReadInt64: Int64;
    function ReadUInt16: Word;
    function ReadUInt32: Cardinal;
    function ReadFloat64: Double;
    function ReadBool: Boolean;
    function ReadUuid: TKafkaUuid;

    function ReadUnsignedVarint: Cardinal;
    function ReadVarint: Int32;
    function ReadVarlong: Int64;

    /// A non-null string; a null one raises EKafkaProtocol.
    function ReadString(ACompact: Boolean): string;
    function ReadNullableString(ACompact: Boolean; out AIsNull: Boolean): string;
    function ReadBytes(ACompact: Boolean): TBytes;
    function ReadNullableBytes(ACompact: Boolean; out AIsNull: Boolean): TBytes;
    /// An array's count, -1 for null. A count larger than the bytes left
    /// raises EKafkaProtocol: every element of every array the library reads
    /// takes at least one byte, and the check keeps a corrupt count from
    /// allocating gigabytes.
    function ReadArrayLength(ACompact: Boolean): Integer;
    /// ReadArrayLength that refuses null.
    function ReadNonNullArrayLength(ACompact: Boolean): Integer;

    function ReadInt32Array(ACompact: Boolean): TKafkaInt32Array;
    function ReadInt16Array(ACompact: Boolean): TKafkaInt16Array;
    function ReadInt64Array(ACompact: Boolean): TKafkaInt64Array;
    function ReadStringArray(ACompact: Boolean): TKafkaStringArray;
    function ReadUuidArray(ACompact: Boolean): TKafkaUuidArray;

    function ReadRaw(ACount: Integer): TBytes;
    procedure Skip(ACount: Integer);

    /// Reads a whole tagged-field section, all as raw (for a header, whose
    /// tags the library doesn't use).
    function ReadRawTaggedFields: TKafkaRawTaggedFields;

    property Position: Integer read FPosition;
    function Remaining: Integer;
    function AtEnd: Boolean;
  end;

/// Appends one raw tagged field to AFields.
procedure KafkaAddRawTaggedField(var AFields: TKafkaRawTaggedFields; ATag: Cardinal;
  const AData: TBytes);

/// Raises EKafkaProtocol with a formatted message.
procedure KafkaProtocolError(const AFormat: string; const AArgs: array of const);

implementation

uses
  PascalCommon.Utf8,
  PascalCommon.Crypto;

procedure KafkaProtocolError(const AFormat: string; const AArgs: array of const);
begin
  raise EKafkaProtocol.CreateFmt(AFormat, AArgs);
end;

procedure KafkaAddRawTaggedField(var AFields: TKafkaRawTaggedFields; ATag: Cardinal;
  const AData: TBytes);
var
  N: Integer;
begin
  N := System.Length(AFields);
  SetLength(AFields, N + 1);
  AFields[N].Tag := ATag;
  AFields[N].Data := AData;
end;

{ TKafkaUuid }

function TKafkaUuid.IsZero: Boolean;
var
  I: Integer;
begin
  for I := 0 to 15 do
    if Bytes[I] <> 0 then
      Exit(False);
  Result := True;
end;

function TKafkaUuid.Equals(const AOther: TKafkaUuid): Boolean;
begin
  Result := CompareMem(@Bytes[0], @AOther.Bytes[0], 16);
end;

function TKafkaUuid.ToString: string;
var
  B: TBytes;
begin
  SetLength(B, 16);
  Move(Bytes[0], B[0], 16);
  Result := PcBase64UrlEncode(B);
end;

class function TKafkaUuid.Zero: TKafkaUuid;
begin
  FillChar(Result, SizeOf(Result), 0);
end;

class function TKafkaUuid.TryParse(const AText: string; out AUuid: TKafkaUuid): Boolean;
var
  B: TBytes;
begin
  FillChar(AUuid, SizeOf(AUuid), 0);
  Result := (System.Length(AText) = 22) and PcBase64UrlDecodeStrict(AText, B)
    and (System.Length(B) = 16);
  if Result then
    Move(B[0], AUuid.Bytes[0], 16);
end;

{ TKafkaWriter }

constructor TKafkaWriter.Create(AInitialCapacity: Integer);
begin
  inherited Create;
  if AInitialCapacity < 16 then
    AInitialCapacity := 16;
  SetLength(FBuffer, AInitialCapacity);
end;

procedure TKafkaWriter.Clear;
begin
  FLength := 0;
end;

procedure TKafkaWriter.Grow(AExtra: Integer);
var
  NewCap: Integer;
begin
  if FLength + AExtra <= System.Length(FBuffer) then
    Exit;
  NewCap := System.Length(FBuffer) * 2;
  if NewCap < FLength + AExtra then
    NewCap := FLength + AExtra;
  SetLength(FBuffer, NewCap);
end;

procedure TKafkaWriter.WriteInt8(AValue: Int8);
begin
  Grow(1);
  FBuffer[FLength] := Byte(AValue);
  Inc(FLength);
end;

procedure TKafkaWriter.WriteInt16(AValue: Int16);
var
  U: Word;
begin
  U := Word(AValue);
  Grow(2);
  FBuffer[FLength] := Byte(U shr 8);
  FBuffer[FLength + 1] := Byte(U);
  Inc(FLength, 2);
end;

procedure TKafkaWriter.WriteUInt16(AValue: Word);
begin
  WriteInt16(Int16(AValue));
end;

procedure TKafkaWriter.WriteUInt32(AValue: Cardinal);
begin
  Grow(4);
  FBuffer[FLength] := Byte(AValue shr 24);
  FBuffer[FLength + 1] := Byte(AValue shr 16);
  FBuffer[FLength + 2] := Byte(AValue shr 8);
  FBuffer[FLength + 3] := Byte(AValue);
  Inc(FLength, 4);
end;

procedure TKafkaWriter.WriteInt32(AValue: Int32);
begin
  WriteUInt32(Cardinal(AValue));
end;

procedure TKafkaWriter.WriteInt64(AValue: Int64);
var
  U: UInt64;
  I: Integer;
begin
  U := UInt64(AValue);
  Grow(8);
  for I := 7 downto 0 do
  begin
    FBuffer[FLength + I] := Byte(U);
    U := U shr 8;
  end;
  Inc(FLength, 8);
end;

procedure TKafkaWriter.WriteFloat64(AValue: Double);
var
  Bits: Int64;
begin
  Move(AValue, Bits, 8);
  WriteInt64(Bits);
end;

procedure TKafkaWriter.WriteBool(AValue: Boolean);
begin
  if AValue then
    WriteInt8(1)
  else
    WriteInt8(0);
end;

procedure TKafkaWriter.WriteUuid(const AValue: TKafkaUuid);
begin
  WriteRaw(AValue.Bytes[0], 16);
end;

procedure TKafkaWriter.WriteUnsignedVarint(AValue: Cardinal);
begin
  Grow(5);
  while (AValue and not Cardinal($7F)) <> 0 do
  begin
    FBuffer[FLength] := Byte((AValue and $7F) or $80);
    Inc(FLength);
    AValue := AValue shr 7;
  end;
  FBuffer[FLength] := Byte(AValue);
  Inc(FLength);
end;

procedure TKafkaWriter.WriteVarint(AValue: Int32);
var
  U: Cardinal;
begin
  // Zigzag: 0, -1, 1, -2... -> 0, 1, 2, 3... without shifting a signed value.
  if AValue < 0 then
    U := (Cardinal(not AValue) shl 1) or 1
  else
    U := Cardinal(AValue) shl 1;
  WriteUnsignedVarint(U);
end;

procedure TKafkaWriter.WriteVarlong(AValue: Int64);
var
  U: UInt64;
begin
  if AValue < 0 then
    U := (UInt64(not AValue) shl 1) or 1
  else
    U := UInt64(AValue) shl 1;
  Grow(10);
  while (U and not UInt64($7F)) <> 0 do
  begin
    FBuffer[FLength] := Byte((U and $7F) or $80);
    Inc(FLength);
    U := U shr 7;
  end;
  FBuffer[FLength] := Byte(U);
  Inc(FLength);
end;

procedure TKafkaWriter.WriteString(const AValue: string; ACompact: Boolean);
var
  B: TBytes;
begin
  B := PcStringToUtf8Bytes(AValue);
  if ACompact then
    WriteUnsignedVarint(Cardinal(System.Length(B)) + 1)
  else
  begin
    if System.Length(B) > High(Int16) then
      KafkaProtocolError('String of %d bytes is longer than 32767, the most a ' +
        'non-compact string can hold', [System.Length(B)]);
    WriteInt16(Int16(System.Length(B)));
  end;
  WriteRaw(B);
end;

procedure TKafkaWriter.WriteNullableString(const AValue: string; AIsNull: Boolean;
  ACompact: Boolean);
begin
  if not AIsNull then
    WriteString(AValue, ACompact)
  else if ACompact then
    WriteUnsignedVarint(0)
  else
    WriteInt16(-1);
end;

procedure TKafkaWriter.WriteBytes(const AValue: TBytes; ACompact: Boolean);
begin
  if ACompact then
    WriteUnsignedVarint(Cardinal(System.Length(AValue)) + 1)
  else
    WriteInt32(System.Length(AValue));
  WriteRaw(AValue);
end;

procedure TKafkaWriter.WriteNullableBytes(const AValue: TBytes; AIsNull: Boolean;
  ACompact: Boolean);
begin
  if not AIsNull then
    WriteBytes(AValue, ACompact)
  else if ACompact then
    WriteUnsignedVarint(0)
  else
    WriteInt32(-1);
end;

procedure TKafkaWriter.WriteArrayLength(ACount: Integer; ACompact: Boolean);
begin
  if ACompact then
    WriteUnsignedVarint(Cardinal(ACount) + 1)
  else
    WriteInt32(ACount);
end;

procedure TKafkaWriter.WriteNullArray(ACompact: Boolean);
begin
  if ACompact then
    WriteUnsignedVarint(0)
  else
    WriteInt32(-1);
end;

procedure TKafkaWriter.WriteInt32Array(const AValue: TKafkaInt32Array; ACompact: Boolean);
var
  I: Integer;
begin
  WriteArrayLength(System.Length(AValue), ACompact);
  for I := 0 to High(AValue) do
    WriteInt32(AValue[I]);
end;

procedure TKafkaWriter.WriteInt16Array(const AValue: TKafkaInt16Array; ACompact: Boolean);
var
  I: Integer;
begin
  WriteArrayLength(System.Length(AValue), ACompact);
  for I := 0 to High(AValue) do
    WriteInt16(AValue[I]);
end;

procedure TKafkaWriter.WriteInt64Array(const AValue: TKafkaInt64Array; ACompact: Boolean);
var
  I: Integer;
begin
  WriteArrayLength(System.Length(AValue), ACompact);
  for I := 0 to High(AValue) do
    WriteInt64(AValue[I]);
end;

procedure TKafkaWriter.WriteStringArray(const AValue: TKafkaStringArray; ACompact: Boolean);
var
  I: Integer;
begin
  WriteArrayLength(System.Length(AValue), ACompact);
  for I := 0 to High(AValue) do
    WriteString(AValue[I], ACompact);
end;

procedure TKafkaWriter.WriteUuidArray(const AValue: TKafkaUuidArray; ACompact: Boolean);
var
  I: Integer;
begin
  WriteArrayLength(System.Length(AValue), ACompact);
  for I := 0 to High(AValue) do
    WriteUuid(AValue[I]);
end;

procedure TKafkaWriter.WriteTaggedField(ATag: Cardinal; const AData: TBytes);
begin
  WriteUnsignedVarint(ATag);
  WriteUnsignedVarint(Cardinal(System.Length(AData)));
  WriteRaw(AData);
end;

procedure TKafkaWriter.WriteRawTaggedFieldsBelow(const AFields: TKafkaRawTaggedFields;
  var AIndex: Integer; ABelowTag: Int64);
begin
  while (AIndex <= High(AFields)) and (Int64(AFields[AIndex].Tag) < ABelowTag) do
  begin
    WriteTaggedField(AFields[AIndex].Tag, AFields[AIndex].Data);
    Inc(AIndex);
  end;
end;

procedure TKafkaWriter.WriteRaw(const ABuffer; ACount: Integer);
begin
  if ACount <= 0 then
    Exit;
  Grow(ACount);
  Move(ABuffer, FBuffer[FLength], ACount);
  Inc(FLength, ACount);
end;

procedure TKafkaWriter.WriteRaw(const AData: TBytes);
begin
  if System.Length(AData) > 0 then
    WriteRaw(AData[0], System.Length(AData));
end;

procedure TKafkaWriter.PatchInt32(APosition: Integer; AValue: Int32);
var
  U: Cardinal;
begin
  if (APosition < 0) or (APosition + 4 > FLength) then
    raise ERangeError.CreateFmt('PatchInt32 at %d outside %d bytes', [APosition, FLength]);
  U := Cardinal(AValue);
  FBuffer[APosition] := Byte(U shr 24);
  FBuffer[APosition + 1] := Byte(U shr 16);
  FBuffer[APosition + 2] := Byte(U shr 8);
  FBuffer[APosition + 3] := Byte(U);
end;

function TKafkaWriter.ToBytes: TBytes;
begin
  Result := nil;
  SetLength(Result, FLength);
  if FLength > 0 then
    Move(FBuffer[0], Result[0], FLength);
end;

{ TKafkaReader }

constructor TKafkaReader.Create(const AData: TBytes; AOffset: Integer; ACount: Integer);
begin
  inherited Create;
  if ACount < 0 then
    ACount := System.Length(AData) - AOffset;
  if (AOffset < 0) or (ACount < 0) or (AOffset + ACount > System.Length(AData)) then
    raise ERangeError.CreateFmt('TKafkaReader: range %d+%d outside %d bytes',
      [AOffset, ACount, System.Length(AData)]);
  FData := AData;
  FPosition := AOffset;
  FLimit := AOffset + ACount;
end;

procedure TKafkaReader.Need(ACount: Integer);
begin
  if (ACount < 0) or (ACount > FLimit - FPosition) then
    KafkaProtocolError('Truncated data: %d bytes needed at offset %d, %d left',
      [ACount, FPosition, FLimit - FPosition]);
end;

function TKafkaReader.Remaining: Integer;
begin
  Result := FLimit - FPosition;
end;

function TKafkaReader.AtEnd: Boolean;
begin
  Result := FPosition >= FLimit;
end;

function TKafkaReader.ReadInt8: Int8;
begin
  Need(1);
  Result := Int8(FData[FPosition]);
  Inc(FPosition);
end;

function TKafkaReader.ReadUInt16: Word;
begin
  Need(2);
  Result := (Word(FData[FPosition]) shl 8) or FData[FPosition + 1];
  Inc(FPosition, 2);
end;

function TKafkaReader.ReadInt16: Int16;
begin
  Result := Int16(ReadUInt16);
end;

function TKafkaReader.ReadUInt32: Cardinal;
begin
  Need(4);
  Result := (Cardinal(FData[FPosition]) shl 24) or (Cardinal(FData[FPosition + 1]) shl 16)
    or (Cardinal(FData[FPosition + 2]) shl 8) or Cardinal(FData[FPosition + 3]);
  Inc(FPosition, 4);
end;

function TKafkaReader.ReadInt32: Int32;
begin
  Result := Int32(ReadUInt32);
end;

function TKafkaReader.ReadInt64: Int64;
var
  U: UInt64;
  I: Integer;
begin
  Need(8);
  U := 0;
  for I := 0 to 7 do
    U := (U shl 8) or FData[FPosition + I];
  Inc(FPosition, 8);
  Result := Int64(U);
end;

function TKafkaReader.ReadFloat64: Double;
var
  Bits: Int64;
begin
  Bits := ReadInt64;
  Move(Bits, Result, 8);
end;

function TKafkaReader.ReadBool: Boolean;
begin
  Result := ReadInt8 <> 0;
end;

function TKafkaReader.ReadUuid: TKafkaUuid;
begin
  Need(16);
  Move(FData[FPosition], Result.Bytes[0], 16);
  Inc(FPosition, 16);
end;

function TKafkaReader.ReadUnsignedVarint: Cardinal;
var
  B: Byte;
  Shift: Integer;
begin
  Result := 0;
  Shift := 0;
  repeat
    Need(1);
    B := FData[FPosition];
    Inc(FPosition);
    // The fifth byte may carry only the top 4 bits of a 32-bit value.
    if (Shift = 28) and ((B and $F0) <> 0) then
      KafkaProtocolError('Unsigned varint longer than 32 bits at offset %d', [FPosition - 1]);
    Result := Result or (Cardinal(B and $7F) shl Shift);
    Inc(Shift, 7);
  until (B and $80) = 0;
end;

function TKafkaReader.ReadVarint: Int32;
var
  U: Cardinal;
begin
  U := ReadUnsignedVarint;
  if (U and 1) = 0 then
    Result := Int32(U shr 1)
  else
    Result := not Int32(U shr 1);
end;

function TKafkaReader.ReadVarlong: Int64;
var
  U: UInt64;
  B: Byte;
  Shift: Integer;
begin
  U := 0;
  Shift := 0;
  repeat
    Need(1);
    B := FData[FPosition];
    Inc(FPosition);
    // The tenth byte may carry only the top bit of a 64-bit value.
    if (Shift = 63) and ((B and $FE) <> 0) then
      KafkaProtocolError('Varlong longer than 64 bits at offset %d', [FPosition - 1]);
    U := U or (UInt64(B and $7F) shl Shift);
    Inc(Shift, 7);
  until (B and $80) = 0;
  if (U and 1) = 0 then
    Result := Int64(U shr 1)
  else
    Result := not Int64(U shr 1);
end;

function TKafkaReader.ReadLength16Or32(AWide: Boolean): Integer;
begin
  if AWide then
    Result := ReadInt32
  else
    Result := ReadInt16;
end;

function TKafkaReader.ReadUtf8(ALength: Integer): string;
var
  B: TBytes;
begin
  B := ReadRaw(ALength);
  if not PcTryUtf8BytesToString(B, Result) then
    KafkaProtocolError('String at offset %d is not valid UTF-8', [FPosition - ALength]);
end;

function TKafkaReader.ReadNullableString(ACompact: Boolean; out AIsNull: Boolean): string;
var
  N: Int64;
begin
  if ACompact then
    N := Int64(ReadUnsignedVarint) - 1
  else
    N := ReadLength16Or32(False);
  AIsNull := N < 0;
  if AIsNull then
  begin
    if N < -1 then
      KafkaProtocolError('Negative string length %d', [N]);
    Result := '';
  end
  else
    Result := ReadUtf8(Integer(N));
end;

function TKafkaReader.ReadString(ACompact: Boolean): string;
var
  IsNull: Boolean;
begin
  Result := ReadNullableString(ACompact, IsNull);
  if IsNull then
    KafkaProtocolError('Null string where the field is not nullable (offset %d)', [FPosition]);
end;

function TKafkaReader.ReadNullableBytes(ACompact: Boolean; out AIsNull: Boolean): TBytes;
var
  N: Int64;
begin
  if ACompact then
    N := Int64(ReadUnsignedVarint) - 1
  else
    N := ReadLength16Or32(True);
  AIsNull := N < 0;
  if AIsNull then
  begin
    if N < -1 then
      KafkaProtocolError('Negative bytes length %d', [N]);
    Result := nil;
  end
  else
    Result := ReadRaw(Integer(N));
end;

function TKafkaReader.ReadBytes(ACompact: Boolean): TBytes;
var
  IsNull: Boolean;
begin
  Result := ReadNullableBytes(ACompact, IsNull);
  if IsNull then
    KafkaProtocolError('Null bytes where the field is not nullable (offset %d)', [FPosition]);
end;

function TKafkaReader.ReadArrayLength(ACompact: Boolean): Integer;
var
  N: Int64;
begin
  if ACompact then
    N := Int64(ReadUnsignedVarint) - 1
  else
    N := ReadInt32;
  if N < -1 then
    KafkaProtocolError('Negative array length %d', [N]);
  if N > Remaining then
    KafkaProtocolError('Array of %d elements with only %d bytes left', [N, Remaining]);
  Result := Integer(N);
end;

function TKafkaReader.ReadNonNullArrayLength(ACompact: Boolean): Integer;
begin
  Result := ReadArrayLength(ACompact);
  if Result < 0 then
    KafkaProtocolError('Null array where the field is not nullable (offset %d)', [FPosition]);
end;

function TKafkaReader.ReadInt32Array(ACompact: Boolean): TKafkaInt32Array;
var
  I: Integer;
begin
  Result := nil;
  SetLength(Result, ReadNonNullArrayLength(ACompact));
  for I := 0 to High(Result) do
    Result[I] := ReadInt32;
end;

function TKafkaReader.ReadInt16Array(ACompact: Boolean): TKafkaInt16Array;
var
  I: Integer;
begin
  Result := nil;
  SetLength(Result, ReadNonNullArrayLength(ACompact));
  for I := 0 to High(Result) do
    Result[I] := ReadInt16;
end;

function TKafkaReader.ReadInt64Array(ACompact: Boolean): TKafkaInt64Array;
var
  I: Integer;
begin
  Result := nil;
  SetLength(Result, ReadNonNullArrayLength(ACompact));
  for I := 0 to High(Result) do
    Result[I] := ReadInt64;
end;

function TKafkaReader.ReadStringArray(ACompact: Boolean): TKafkaStringArray;
var
  I: Integer;
begin
  Result := nil;
  SetLength(Result, ReadNonNullArrayLength(ACompact));
  for I := 0 to High(Result) do
    Result[I] := ReadString(ACompact);
end;

function TKafkaReader.ReadUuidArray(ACompact: Boolean): TKafkaUuidArray;
var
  I: Integer;
begin
  Result := nil;
  SetLength(Result, ReadNonNullArrayLength(ACompact));
  for I := 0 to High(Result) do
    Result[I] := ReadUuid;
end;

function TKafkaReader.ReadRaw(ACount: Integer): TBytes;
begin
  Result := nil;
  Need(ACount);
  SetLength(Result, ACount);
  if ACount > 0 then
    Move(FData[FPosition], Result[0], ACount);
  Inc(FPosition, ACount);
end;

procedure TKafkaReader.Skip(ACount: Integer);
begin
  Need(ACount);
  Inc(FPosition, ACount);
end;

function TKafkaReader.ReadRawTaggedFields: TKafkaRawTaggedFields;
var
  Count, I: Cardinal;
  Tag, Size: Cardinal;
begin
  Result := nil;
  Count := ReadUnsignedVarint;
  if Int64(Count) > Remaining then
    KafkaProtocolError('%d tagged fields with only %d bytes left', [Int64(Count), Remaining]);
  I := 0;
  while I < Count do
  begin
    Tag := ReadUnsignedVarint;
    Size := ReadUnsignedVarint;
    if Int64(Size) > Remaining then
      KafkaProtocolError('Tagged field %d of %d bytes with only %d bytes left',
        [Int64(Tag), Int64(Size), Remaining]);
    KafkaAddRawTaggedField(Result, Tag, ReadRaw(Integer(Size)));
    Inc(I);
  end;
end;

end.
