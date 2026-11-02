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

  A record without a key isn't hashed: it goes to a "sticky" partition
  (TKafkaStickyPartitioner), as Java's producer does since KIP-480 and in
  the form KIP-794 gave it (Kafka 3.3): every keyless record of a topic goes
  to the same partition until BatchSize bytes went there, then another is
  chosen at random among the partitions that have a leader. Batches fill up
  instead of each partition getting a record at a time, and over time the
  load spreads evenly. Two differences from Java, neither visible to a
  consumer: the next partition is never the one just left (when there is
  another), and there is no adaptive weighting by queue size
  (partitioner.adaptive.partitioning.enable, which Java turns on by default:
  it favours the brokers that drain faster). The random numbers are an
  xorshift32 of the partitioner's own, not the RTL's Random, whose state is
  global and not thread-safe. }

{$OVERFLOWCHECKS OFF}
{$RANGECHECKS OFF}

interface

uses
  SysUtils,
  Generics.Collections,
  Kafka.Protocol.Primitives;

type
  { The sticky partition of each topic for records without a key (the unit's
    header). Not thread-safe: the producer calls it under its lock. }
  TKafkaStickyPartitioner = class
  private type
    TState = record
      Partition: Int32;
      Bytes: Int64;
    end;
  private
    FBatchSize: Integer;
    FState: TDictionary<string, TState>;
    FRandom: Cardinal;
    function NextRandom: Cardinal;
    function Choose(APartitionCount: Integer; const AAvailable: TKafkaInt32Array;
      AExclude: Int32): Int32;
  public
    /// ABatchSize: the bytes after which a topic moves to another partition.
    /// ASeed = 0 seeds from the clock.
    constructor Create(ABatchSize: Integer; ASeed: Cardinal = 0);
    destructor Destroy; override;
    /// The partition for a keyless record of ARecordBytes bytes, counted
    /// towards the switch. AAvailable: the partitions with a leader (empty:
    /// none known, any of the APartitionCount is taken). EArgumentException
    /// for a count below 1.
    function Partition(const ATopic: string; APartitionCount: Integer;
      const AAvailable: TKafkaInt32Array; ARecordBytes: Integer): Int32;
  end;

/// Java's Utils.murmur2: seed $9747B28C, the result as a signed int.
function KafkaMurmur2(const AData: TBytes): Int32;
/// Java's Utils.toPositive: the sign bit cleared (not Abs: Low(Int32)
/// becomes 0).
function KafkaToPositive(AValue: Int32): Int32;
/// The partition, 0 to APartitionCount - 1, Java's producer gives AKey.
/// EArgumentException for a count below 1.
function KafkaPartitionForKey(const AKey: TBytes; APartitionCount: Integer): Integer;

implementation

uses
  PascalCommon.Threading;

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

{ TKafkaStickyPartitioner }

constructor TKafkaStickyPartitioner.Create(ABatchSize: Integer; ASeed: Cardinal);
begin
  inherited Create;
  FBatchSize := ABatchSize;
  if FBatchSize < 1 then
    FBatchSize := 1;
  FState := TDictionary<string, TState>.Create;
  FRandom := ASeed;
  if FRandom = 0 then
    FRandom := Cardinal(PcTickUs) or 1;
end;

destructor TKafkaStickyPartitioner.Destroy;
begin
  FState.Free;
  inherited;
end;

function TKafkaStickyPartitioner.NextRandom: Cardinal;
begin
  // xorshift32 (Marsaglia); never 0 from a non-zero state.
  FRandom := FRandom xor (FRandom shl 13);
  FRandom := FRandom xor (FRandom shr 17);
  FRandom := FRandom xor (FRandom shl 5);
  Result := FRandom;
end;

function TKafkaStickyPartitioner.Choose(APartitionCount: Integer;
  const AAvailable: TKafkaInt32Array; AExclude: Int32): Int32;
var
  LCandidates: TKafkaInt32Array;
  I, N: Integer;
  LExcludedIsAvailable: Boolean;
begin
  LCandidates := nil;
  SetLength(LCandidates, APartitionCount);
  N := 0;
  LExcludedIsAvailable := False;
  for I := 0 to High(AAvailable) do
    if (AAvailable[I] >= 0) and (AAvailable[I] < APartitionCount) then
    begin
      if AAvailable[I] = AExclude then
        LExcludedIsAvailable := True
      else if N < APartitionCount then
      begin
        LCandidates[N] := AAvailable[I];
        Inc(N);
      end;
    end;
  if N > 0 then
    Exit(LCandidates[NextRandom mod Cardinal(N)]);
  // The partition just left is the only one with a leader: stay on it.
  if LExcludedIsAvailable then
    Exit(AExclude);
  // No leader known: any partition but the one just left.
  for I := 0 to APartitionCount - 1 do
    if (I <> AExclude) or (APartitionCount = 1) then
    begin
      LCandidates[N] := I;
      Inc(N);
    end;
  Result := LCandidates[NextRandom mod Cardinal(N)];
end;

function TKafkaStickyPartitioner.Partition(const ATopic: string; APartitionCount: Integer;
  const AAvailable: TKafkaInt32Array; ARecordBytes: Integer): Int32;
var
  LState: TState;
  LFound: Boolean;
begin
  if APartitionCount < 1 then
    raise EArgumentException.CreateFmt('A topic has at least one partition, not %d',
      [APartitionCount]);
  LFound := FState.TryGetValue(ATopic, LState);
  if not LFound then
  begin
    LState.Partition := Choose(APartitionCount, AAvailable, -1);
    LState.Bytes := 0;
  end
  else if (LState.Bytes >= FBatchSize) or (LState.Partition >= APartitionCount) then
  begin
    LState.Partition := Choose(APartitionCount, AAvailable, LState.Partition);
    LState.Bytes := 0;
  end;
  Result := LState.Partition;
  Inc(LState.Bytes, ARecordBytes);
  FState.AddOrSetValue(ATopic, LState);
end;

end.
