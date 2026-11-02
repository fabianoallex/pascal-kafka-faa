unit Kafka.Partitioner;

{$I kafka.inc}

{ The partition of a keyed record, as the Java client chooses it: murmur2 of
  the key's bytes, made positive, modulo the partition count
  (BuiltInPartitioner.partitionForKey and Utils.murmur2 of Kafka 4.4.0).

  Compatibility with Java is the point (docs/design.md, section 5): a record
  produced here with key K must land in the partition a Java producer would
  pick for K, or per-key ordering breaks for any application that mixes
  clients. The tests check murmur2 against Kafka's own vectors (UtilsTest)
  and the partitions against a table the Java console producer made, for 1,
  3, 12 and 100 partitions (tests/data/partitions-java-4.4.0.tsv).

  Java's murmur2 is 32-bit int arithmetic that wraps, with >>> (unsigned
  shift) and the four-byte blocks read little-endian. Here everything is
  Cardinal: shr on Cardinal is >>>, and a product is taken in UInt64 and
  truncated, so the result doesn't depend on how each compiler sizes a
  Cardinal * Cardinal (FPC may widen it on 64-bit targets). Overflow and
  range checks are off for the unit because the wrap is the algorithm;
  Delphi's Debug configuration turns both on.

  A record without a key isn't hashed: Java's producer sends those to a
  "sticky" partition per batch (KIP-480), which is the producer's business
  (Kafka.Producer), not this unit's. }

{$OVERFLOWCHECKS OFF}
{$RANGECHECKS OFF}

interface

uses
  SysUtils;

/// Java's Utils.murmur2: seed $9747B28C, the result as a signed int.
function KafkaMurmur2(const AData: TBytes): Int32;
/// Java's Utils.toPositive: the sign bit cleared (not Abs: Low(Int32)
/// becomes 0).
function KafkaToPositive(AValue: Int32): Int32;
/// The partition, 0 to APartitionCount - 1, Java's producer gives AKey.
/// EArgumentException for a count below 1.
function KafkaPartitionForKey(const AKey: TBytes; APartitionCount: Integer): Integer;

implementation

const
  SEED = Cardinal($9747B28C);
  M = Cardinal($5BD1E995);
  R = 24;

function Mul(A, B: Cardinal): Cardinal; inline;
begin
  Result := Cardinal((UInt64(A) * UInt64(B)) and $FFFFFFFF);
end;

function KafkaMurmur2(const AData: TBytes): Int32;
var
  N, I, I4, Index: Integer;
  H, K: Cardinal;
begin
  N := Length(AData);
  H := SEED xor Cardinal(N);
  for I := 0 to (N shr 2) - 1 do
  begin
    I4 := I shl 2;
    K := Cardinal(AData[I4]) or (Cardinal(AData[I4 + 1]) shl 8) or
      (Cardinal(AData[I4 + 2]) shl 16) or (Cardinal(AData[I4 + 3]) shl 24);
    K := Mul(K, M);
    K := K xor (K shr R);
    K := Mul(K, M);
    H := Mul(H, M);
    H := H xor K;
  end;
  Index := (N shr 2) shl 2;
  // Java's switch falls through: 3 does 2 and 1 too.
  if N - Index >= 3 then
    H := H xor (Cardinal(AData[Index + 2]) shl 16);
  if N - Index >= 2 then
    H := H xor (Cardinal(AData[Index + 1]) shl 8);
  if N - Index >= 1 then
  begin
    H := H xor Cardinal(AData[Index]);
    H := Mul(H, M);
  end;
  H := H xor (H shr 13);
  H := Mul(H, M);
  H := H xor (H shr 15);
  Result := Int32(H);
end;

function KafkaToPositive(AValue: Int32): Int32;
begin
  Result := AValue and $7FFFFFFF;
end;

function KafkaPartitionForKey(const AKey: TBytes; APartitionCount: Integer): Integer;
begin
  if APartitionCount < 1 then
    raise EArgumentException.CreateFmt('A topic has at least one partition, not %d',
      [APartitionCount]);
  Result := KafkaToPositive(KafkaMurmur2(AKey)) mod APartitionCount;
end;

end.
