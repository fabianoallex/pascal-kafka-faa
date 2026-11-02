unit Kafka.Protocol.Crc32c;

{$I kafka.inc}

{ CRC32C (Castagnoli), the checksum of Kafka's record batch v2.

  The reflected polynomial 0x82F63B78, initial value and final XOR $FFFFFFFF:
  the iSCSI CRC of RFC 3720 (section 12.1 and the vectors of appendix B.4,
  which the tests check), and what Java's java.util.zip.CRC32C computes. A
  record batch's CRC covers the bytes from its attributes field to the end of
  the batch (KIP-98).

  One table of 256 entries, filled in the unit's initialization (not on first
  use: the family's rule against lazily created shared state). Byte-at-a-time
  is enough for now; slicing-by-8 or the SSE4.2 instruction can come when a
  profile asks for it.

  The arithmetic is on Cardinal with overflow and range checks off for this
  unit whatever the project says: Delphi's Debug configuration turns both on,
  and the shifts and XORs here are meant to wrap. }

{$OVERFLOWCHECKS OFF}
{$RANGECHECKS OFF}

interface

uses
  SysUtils;

/// The CRC32C of ACount bytes at ABuffer.
function KafkaCrc32c(const ABuffer; ACount: Integer): Cardinal; overload;
/// The CRC32C of ACount bytes of AData from AOffset.
function KafkaCrc32c(const AData: TBytes; AOffset, ACount: Integer): Cardinal; overload;
/// The CRC32C of all of AData.
function KafkaCrc32c(const AData: TBytes): Cardinal; overload;

/// Continues a running CRC: start with KafkaCrc32cInit, feed each piece with
/// KafkaCrc32cUpdate, end with KafkaCrc32cFinal. The one-call forms above are
/// Final(Update(Init, ...)).
function KafkaCrc32cInit: Cardinal;
function KafkaCrc32cUpdate(ACrc: Cardinal; const ABuffer; ACount: Integer): Cardinal;
function KafkaCrc32cFinal(ACrc: Cardinal): Cardinal;

implementation

const
  CRC32C_POLY = Cardinal($82F63B78);

var
  GTable: array[0..255] of Cardinal;

procedure BuildTable;
var
  I, J: Integer;
  C: Cardinal;
begin
  for I := 0 to 255 do
  begin
    C := Cardinal(I);
    for J := 0 to 7 do
      if (C and 1) <> 0 then
        C := (C shr 1) xor CRC32C_POLY
      else
        C := C shr 1;
    GTable[I] := C;
  end;
end;

function KafkaCrc32cInit: Cardinal;
begin
  Result := Cardinal($FFFFFFFF);
end;

function KafkaCrc32cUpdate(ACrc: Cardinal; const ABuffer; ACount: Integer): Cardinal;
var
  P: PByte;
  I: Integer;
begin
  Result := ACrc;
  P := @ABuffer;
  for I := 0 to ACount - 1 do
  begin
    Result := GTable[(Result xor P^) and $FF] xor (Result shr 8);
    Inc(P);
  end;
end;

function KafkaCrc32cFinal(ACrc: Cardinal): Cardinal;
begin
  Result := ACrc xor Cardinal($FFFFFFFF);
end;

function KafkaCrc32c(const ABuffer; ACount: Integer): Cardinal;
begin
  Result := KafkaCrc32cFinal(KafkaCrc32cUpdate(KafkaCrc32cInit, ABuffer, ACount));
end;

function KafkaCrc32c(const AData: TBytes; AOffset, ACount: Integer): Cardinal;
begin
  if (AOffset < 0) or (ACount < 0) or (AOffset + ACount > Length(AData)) then
    raise ERangeError.CreateFmt('KafkaCrc32c: range %d+%d outside %d bytes',
      [AOffset, ACount, Length(AData)]);
  if ACount = 0 then
    Result := KafkaCrc32cFinal(KafkaCrc32cInit)
  else
    Result := KafkaCrc32c(AData[AOffset], ACount);
end;

function KafkaCrc32c(const AData: TBytes): Cardinal;
begin
  Result := KafkaCrc32c(AData, 0, Length(AData));
end;

initialization
  BuildTable;

end.
