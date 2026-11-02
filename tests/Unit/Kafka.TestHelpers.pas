unit Kafka.TestHelpers;

{ Small helpers shared by the unit and integration tests: hex in and out,
  byte literals, and finding the pascal-common-faa test PKI.

  Not a test unit (its name doesn't end in Tests), so tools/gen_fpc_mirror.py
  leaves it alone and both suites compile this one copy. }

interface

uses
  SysUtils;

/// Bytes from a hex string; spaces are ignored ('00 0a FF').
function HexToBytes(const AHex: string): TBytes;
/// Lowercase hex, no separators.
function BytesToHex(const ABytes: TBytes): string;
/// A TBytes from a literal list.
function Bytes(const AValues: array of Byte): TBytes;
/// AData[AOffset..] as a new array.
function BytesFrom(const AData: TBytes; AOffset: Integer): TBytes;
function SameBytes(const A, B: TBytes): Boolean;

/// The folder with pascal-common-faa's test certificates (its tests/pki),
/// found by walking up from the executable to external/pascal-common-faa.
/// KAFKA_TEST_PKI overrides it. Raises when not found.
function KafkaTestPkiDir: string;

implementation

function HexValue(C: Char): Integer;
begin
  case C of
    '0'..'9': Result := Ord(C) - Ord('0');
    'a'..'f': Result := Ord(C) - Ord('a') + 10;
    'A'..'F': Result := Ord(C) - Ord('A') + 10;
  else
    raise EConvertError.CreateFmt('Not a hex digit: "%s"', [C]);
  end;
end;

function HexToBytes(const AHex: string): TBytes;
var
  Clean: string;
  I: Integer;
begin
  Clean := StringReplace(AHex, ' ', '', [rfReplaceAll]);
  if Odd(Length(Clean)) then
    raise EConvertError.CreateFmt('Odd number of hex digits: "%s"', [AHex]);
  Result := nil;
  SetLength(Result, Length(Clean) div 2);
  for I := 0 to High(Result) do
    Result[I] := Byte(HexValue(Clean[2 * I + 1]) * 16 + HexValue(Clean[2 * I + 2]));
end;

function BytesToHex(const ABytes: TBytes): string;
const
  DIGITS: array[0..15] of Char = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'a', 'b', 'c', 'd', 'e', 'f');
var
  I: Integer;
begin
  Result := '';
  SetLength(Result, Length(ABytes) * 2);
  for I := 0 to High(ABytes) do
  begin
    Result[2 * I + 1] := DIGITS[ABytes[I] shr 4];
    Result[2 * I + 2] := DIGITS[ABytes[I] and 15];
  end;
end;

function Bytes(const AValues: array of Byte): TBytes;
var
  I: Integer;
begin
  Result := nil;
  SetLength(Result, Length(AValues));
  for I := 0 to High(AValues) do
    Result[I] := AValues[I];
end;

function BytesFrom(const AData: TBytes; AOffset: Integer): TBytes;
begin
  Result := Copy(AData, AOffset, Length(AData) - AOffset);
end;

function SameBytes(const A, B: TBytes): Boolean;
begin
  Result := (Length(A) = Length(B)) and ((Length(A) = 0) or CompareMem(@A[0], @B[0], Length(A)));
end;

function KafkaTestPkiDir: string;
var
  Dir, Candidate: string;
  I: Integer;
begin
  Result := GetEnvironmentVariable('KAFKA_TEST_PKI');
  if Result <> '' then
    Exit(IncludeTrailingPathDelimiter(Result));
  Dir := ExtractFileDir(ExpandFileName(ParamStr(0)));
  for I := 0 to 8 do
  begin
    Candidate := IncludeTrailingPathDelimiter(Dir) + 'external' + PathDelim +
      'pascal-common-faa' + PathDelim + 'tests' + PathDelim + 'pki' + PathDelim;
    if FileExists(Candidate + 'ca_cert.pem') then
      Exit(Candidate);
    Dir := ExtractFileDir(Dir);
  end;
  raise Exception.Create('external/pascal-common-faa/tests/pki not found above ' +
    ParamStr(0) + ' (set KAFKA_TEST_PKI)');
end;

end.
