unit Kafka.Protocol.RecordBatch;

{$I kafka.inc}

{ Record batch v2 (magic 2, KIP-98): what the "records" field of Produce and
  Fetch carries, written and read by hand (decision 3 of docs/design.md; the
  generated codecs hold it as raw bytes).

  A batch on the wire (offsets from its start):

     0  baseOffset            int64
     8  batchLength           int32   bytes after this field
    12  partitionLeaderEpoch  int32
    16  magic                 int8    2
    17  crc                   uint32  CRC-32C of bytes 21 to the end
    21  attributes            int16   bits 0-2 codec, 3 timestamp type,
                                      4 transactional, 5 control,
                                      6 delete horizon
    23  lastOffsetDelta       int32
    27  baseTimestamp         int64
    35  maxTimestamp          int64
    43  producerId            int64
    51  producerEpoch         int16
    53  baseSequence          int32
    57  recordCount           int32
    61  records               compressed as a whole when the codec isn't none

  and a record: length (varint), attributes (int8, unused), timestampDelta
  (varlong), offsetDelta (varint), key length (varint, -1 for null) and key,
  value length and value, header count (varint), then each header's key
  (varint length, UTF-8) and value (varint length, -1 for null). Varints
  here are zigzag (signed), unlike the unsigned ones of the flexible
  versions' lengths.

  Each record keeps its deltas as read, so decoding then encoding gives the
  same bytes back (the codec tests rely on it); decoding also fills Offset
  and Timestamp, the absolute values a consumer wants. As Java's
  DefaultRecordBatch: a record's timestamp is the batch's MaxTimestamp when
  the batch's timestamp type is LogAppendTime, BaseTimestamp +
  TimestampDelta otherwise.

  Reading a Fetch's records (KafkaDecodeRecordBatches) stops at a last batch
  that isn't whole: the broker sends a segment's bytes up to the fetch's
  byte limit, which can cut one, and Java's iterator does the same. Every
  whole batch has its CRC checked. Magic 0 and 1 (the message sets of Kafka
  before 0.11) are refused: Kafka 4 neither writes nor converts them
  (KIP-724).

  Header keys are strings (Java's API: String); bytes that aren't UTF-8
  raise EKafkaProtocol, as everywhere in this codec. Payloads (key, value,
  header values) are TBytes with an IsNull flag: Kafka tells a null key from
  an empty one. }

interface

uses
  SysUtils,
  Kafka.Protocol.Primitives,
  Kafka.Compression;

const
  KAFKA_RECORD_BATCH_MAGIC = 2;
  /// The bytes before the records: offsets 0 to 60 above.
  KAFKA_RECORD_BATCH_HEADER_SIZE = 61;
  /// baseOffset and batchLength: what batchLength doesn't count.
  KAFKA_RECORD_BATCH_LOG_OVERHEAD = 12;

  KAFKA_NO_PRODUCER_ID = -1;
  KAFKA_NO_PRODUCER_EPOCH = -1;
  KAFKA_NO_SEQUENCE = -1;
  KAFKA_NO_PARTITION_LEADER_EPOCH = -1;
  KAFKA_NO_TIMESTAMP = -1;

  KAFKA_BATCH_ATTR_COMPRESSION_MASK = $07;
  KAFKA_BATCH_ATTR_LOG_APPEND_TIME = $08;
  KAFKA_BATCH_ATTR_TRANSACTIONAL = $10;
  KAFKA_BATCH_ATTR_CONTROL = $20;
  KAFKA_BATCH_ATTR_DELETE_HORIZON = $40;

type
  TKafkaRecordHeader = record
    Key: string;
    Value: TBytes;
    ValueIsNull: Boolean;
  end;
  TKafkaRecordHeaderArray = array of TKafkaRecordHeader;

  TKafkaRecord = record
    /// Unused by Kafka (always 0); kept for the round trip.
    Attributes: Int8;
    TimestampDelta: Int64;
    OffsetDelta: Int32;
    Key: TBytes;
    KeyIsNull: Boolean;
    Value: TBytes;
    ValueIsNull: Boolean;
    Headers: TKafkaRecordHeaderArray;
    /// Absolute offset and timestamp: filled by decoding and by
    /// TKafkaRecordBatch.Append; not written (the deltas are).
    Offset: Int64;
    Timestamp: Int64;
  end;
  TKafkaRecordArray = array of TKafkaRecord;

  TKafkaRecordBatch = record
    BaseOffset: Int64;
    PartitionLeaderEpoch: Int32;
    Attributes: Int16;
    LastOffsetDelta: Int32;
    BaseTimestamp: Int64;
    MaxTimestamp: Int64;
    ProducerId: Int64;
    ProducerEpoch: Int16;
    BaseSequence: Int32;
    Records: TKafkaRecordArray;
    /// The CRC read from the wire (decoding only; Encode computes its own).
    Crc: Cardinal;
    /// An empty batch for a producer: offset 0, no producer id, epoch or
    /// sequence, no partition leader epoch, no compression, CreateTime.
    procedure Init;
    function Compression: TKafkaCompression;
    procedure SetCompression(ACodec: TKafkaCompression);
    function IsLogAppendTime: Boolean;
    function IsTransactional: Boolean;
    /// A control batch (transaction markers): its records aren't user data.
    function IsControl: Boolean;
    function HasDeleteHorizon: Boolean;
    /// The offset after this batch: where the next fetch starts, even when
    /// compaction removed records from the batch's end.
    function NextOffset: Int64;
    /// Adds a record, with ARecord.Timestamp absolute; sets its deltas from
    /// the batch's first record (as Java's MemoryRecordsBuilder) and the
    /// batch's LastOffsetDelta and MaxTimestamp. The record's Offset is
    /// BaseOffset + its position.
    procedure Append(const ARecord: TKafkaRecord);
    /// The whole batch, with batchLength and CRC computed and the records
    /// compressed with Compression.
    function Encode: TBytes;
  end;
  TKafkaRecordBatchArray = array of TKafkaRecordBatch;

/// A record with key and value, no headers; Timestamp absolute, for Append.
function KafkaRecord(const AKey, AValue: TBytes; ATimestamp: Int64): TKafkaRecord;
/// Adds a header to ARecord.
procedure KafkaAddRecordHeader(var ARecord: TKafkaRecord; const AKey: string;
  const AValue: TBytes; AValueIsNull: Boolean = False);

/// Every whole batch in AData (a Produce's or a Fetch's "records"), in
/// order. ATrailingBytes: the bytes of a last batch cut short, ignored.
/// AMaxDecompressedSize bounds each batch's decompressed records.
/// EKafkaProtocol on a CRC mismatch, a magic other than 2, or records that
/// don't fill their batch exactly; EKafkaCompression from the codec.
function KafkaDecodeRecordBatches(const AData: TBytes; AMaxDecompressedSize: Integer;
  out ATrailingBytes: Integer): TKafkaRecordBatchArray;

/// The batch at AOffset of AData, whole (ASize is its total size): the step
/// of KafkaDecodeRecordBatches.
function KafkaDecodeRecordBatch(const AData: TBytes; AOffset, ASize: Integer;
  AMaxDecompressedSize: Integer): TKafkaRecordBatch;

implementation

uses
  PascalCommon.Utf8,
  Kafka.Protocol.Crc32c;

const
  CRC_OFFSET = 17;
  ATTRIBUTES_OFFSET = 21;
  MAGIC_OFFSET = 16;

{ helpers }

function KafkaRecord(const AKey, AValue: TBytes; ATimestamp: Int64): TKafkaRecord;
begin
  Result := Default(TKafkaRecord);
  Result.Key := AKey;
  Result.Value := AValue;
  Result.Timestamp := ATimestamp;
end;

procedure KafkaAddRecordHeader(var ARecord: TKafkaRecord; const AKey: string;
  const AValue: TBytes; AValueIsNull: Boolean);
var
  N: Integer;
begin
  N := Length(ARecord.Headers);
  SetLength(ARecord.Headers, N + 1);
  ARecord.Headers[N].Key := AKey;
  ARecord.Headers[N].Value := AValue;
  ARecord.Headers[N].ValueIsNull := AValueIsNull;
end;

function GetInt32BE(const AData: TBytes; AOffset: Integer): Int32;
begin
  Result := Int32((Cardinal(AData[AOffset]) shl 24) or (Cardinal(AData[AOffset + 1]) shl 16) or
    (Cardinal(AData[AOffset + 2]) shl 8) or Cardinal(AData[AOffset + 3]));
end;

{ TKafkaRecordBatch }

procedure TKafkaRecordBatch.Init;
begin
  Self := Default(TKafkaRecordBatch);
  PartitionLeaderEpoch := KAFKA_NO_PARTITION_LEADER_EPOCH;
  BaseTimestamp := KAFKA_NO_TIMESTAMP;
  MaxTimestamp := KAFKA_NO_TIMESTAMP;
  ProducerId := KAFKA_NO_PRODUCER_ID;
  ProducerEpoch := KAFKA_NO_PRODUCER_EPOCH;
  BaseSequence := KAFKA_NO_SEQUENCE;
  LastOffsetDelta := -1;
end;

function TKafkaRecordBatch.Compression: TKafkaCompression;
begin
  Result := KafkaCompressionFromAttributes(Attributes);
end;

procedure TKafkaRecordBatch.SetCompression(ACodec: TKafkaCompression);
begin
  Attributes := Int16((Attributes and not KAFKA_BATCH_ATTR_COMPRESSION_MASK) or Ord(ACodec));
end;

function TKafkaRecordBatch.IsLogAppendTime: Boolean;
begin
  Result := (Attributes and KAFKA_BATCH_ATTR_LOG_APPEND_TIME) <> 0;
end;

function TKafkaRecordBatch.IsTransactional: Boolean;
begin
  Result := (Attributes and KAFKA_BATCH_ATTR_TRANSACTIONAL) <> 0;
end;

function TKafkaRecordBatch.IsControl: Boolean;
begin
  Result := (Attributes and KAFKA_BATCH_ATTR_CONTROL) <> 0;
end;

function TKafkaRecordBatch.HasDeleteHorizon: Boolean;
begin
  Result := (Attributes and KAFKA_BATCH_ATTR_DELETE_HORIZON) <> 0;
end;

function TKafkaRecordBatch.NextOffset: Int64;
begin
  Result := BaseOffset + LastOffsetDelta + 1;
end;

procedure TKafkaRecordBatch.Append(const ARecord: TKafkaRecord);
var
  N: Integer;
begin
  N := Length(Records);
  if N = 0 then
  begin
    BaseTimestamp := ARecord.Timestamp;
    MaxTimestamp := ARecord.Timestamp;
  end
  else if ARecord.Timestamp > MaxTimestamp then
    MaxTimestamp := ARecord.Timestamp;
  SetLength(Records, N + 1);
  Records[N] := ARecord;
  Records[N].OffsetDelta := N;
  Records[N].TimestampDelta := ARecord.Timestamp - BaseTimestamp;
  Records[N].Offset := BaseOffset + N;
  LastOffsetDelta := N;
end;

procedure WriteVarintBytes(AWriter: TKafkaWriter; const AData: TBytes; AIsNull: Boolean);
begin
  if AIsNull then
    AWriter.WriteVarint(-1)
  else
  begin
    AWriter.WriteVarint(Length(AData));
    AWriter.WriteRaw(AData);
  end;
end;

procedure WriteRecord(AWriter, ABody: TKafkaWriter; const ARecord: TKafkaRecord);
var
  I: Integer;
  LKey: TBytes;
begin
  ABody.Clear;
  ABody.WriteInt8(ARecord.Attributes);
  ABody.WriteVarlong(ARecord.TimestampDelta);
  ABody.WriteVarint(ARecord.OffsetDelta);
  WriteVarintBytes(ABody, ARecord.Key, ARecord.KeyIsNull);
  WriteVarintBytes(ABody, ARecord.Value, ARecord.ValueIsNull);
  ABody.WriteVarint(Length(ARecord.Headers));
  for I := 0 to High(ARecord.Headers) do
  begin
    LKey := PcStringToUtf8Bytes(ARecord.Headers[I].Key);
    WriteVarintBytes(ABody, LKey, False);
    WriteVarintBytes(ABody, ARecord.Headers[I].Value, ARecord.Headers[I].ValueIsNull);
  end;
  AWriter.WriteVarint(ABody.Length);
  AWriter.WriteRaw(ABody.ToBytes);
end;

function TKafkaRecordBatch.Encode: TBytes;
var
  LRecords, LBody, LOut: TKafkaWriter;
  LPayload: TBytes;
  I, N: Integer;
  LCrc: Cardinal;
begin
  LRecords := TKafkaWriter.Create;
  LBody := TKafkaWriter.Create;
  LOut := TKafkaWriter.Create;
  try
    for I := 0 to High(Records) do
      WriteRecord(LRecords, LBody, Records[I]);
    LPayload := KafkaCompress(Compression, LRecords.ToBytes);

    LOut.WriteInt64(BaseOffset);
    LOut.WriteInt32(0); // batchLength, patched below
    LOut.WriteInt32(PartitionLeaderEpoch);
    LOut.WriteInt8(KAFKA_RECORD_BATCH_MAGIC);
    LOut.WriteUInt32(0); // crc, set below
    LOut.WriteInt16(Attributes);
    LOut.WriteInt32(LastOffsetDelta);
    LOut.WriteInt64(BaseTimestamp);
    LOut.WriteInt64(MaxTimestamp);
    LOut.WriteInt64(ProducerId);
    LOut.WriteInt16(ProducerEpoch);
    LOut.WriteInt32(BaseSequence);
    LOut.WriteInt32(Length(Records));
    LOut.WriteRaw(LPayload);
    LOut.PatchInt32(8, LOut.Length - KAFKA_RECORD_BATCH_LOG_OVERHEAD);
    Result := LOut.ToBytes;
  finally
    LOut.Free;
    LBody.Free;
    LRecords.Free;
  end;
  N := Length(Result);
  LCrc := KafkaCrc32c(Result, ATTRIBUTES_OFFSET, N - ATTRIBUTES_OFFSET);
  Result[CRC_OFFSET] := Byte(LCrc shr 24);
  Result[CRC_OFFSET + 1] := Byte(LCrc shr 16);
  Result[CRC_OFFSET + 2] := Byte(LCrc shr 8);
  Result[CRC_OFFSET + 3] := Byte(LCrc);
end;

{ decoding }

function ReadVarintBytes(AReader: TKafkaReader; out AIsNull: Boolean): TBytes;
var
  N: Int32;
begin
  N := AReader.ReadVarint;
  AIsNull := N = -1;
  if N < -1 then
    KafkaProtocolError('Negative length %d in a record', [N]);
  if N <= 0 then
    Result := nil
  else
    Result := AReader.ReadRaw(N);
end;

procedure ReadRecord(AReader: TKafkaReader; const ABatch: TKafkaRecordBatch;
  var ARecord: TKafkaRecord);
var
  LSize: Int32;
  LEnd, I, LCount: Integer;
  LKey: TBytes;
  LNull: Boolean;
begin
  LSize := AReader.ReadVarint;
  if (LSize < 0) or (LSize > AReader.Remaining) then
    KafkaProtocolError('Record of %d bytes with %d left in the batch',
      [LSize, AReader.Remaining]);
  LEnd := AReader.Position + LSize;
  ARecord.Attributes := AReader.ReadInt8;
  ARecord.TimestampDelta := AReader.ReadVarlong;
  ARecord.OffsetDelta := AReader.ReadVarint;
  ARecord.Key := ReadVarintBytes(AReader, ARecord.KeyIsNull);
  ARecord.Value := ReadVarintBytes(AReader, ARecord.ValueIsNull);
  LCount := AReader.ReadVarint;
  if (LCount < 0) or (LCount > LEnd - AReader.Position) then
    KafkaProtocolError('Record with %d headers in %d bytes', [LCount, LEnd - AReader.Position]);
  SetLength(ARecord.Headers, LCount);
  for I := 0 to LCount - 1 do
  begin
    LKey := ReadVarintBytes(AReader, LNull);
    if LNull then
      KafkaProtocolError('Record header with a null key', []);
    if not PcTryUtf8BytesToString(LKey, ARecord.Headers[I].Key) then
      KafkaProtocolError('Record header key is not valid UTF-8', []);
    ARecord.Headers[I].Value := ReadVarintBytes(AReader, ARecord.Headers[I].ValueIsNull);
  end;
  if AReader.Position <> LEnd then
    KafkaProtocolError('Record of %d bytes read as %d', [LSize, AReader.Position - (LEnd - LSize)]);
  ARecord.Offset := ABatch.BaseOffset + ARecord.OffsetDelta;
  if ABatch.IsLogAppendTime then
    ARecord.Timestamp := ABatch.MaxTimestamp
  else
    ARecord.Timestamp := ABatch.BaseTimestamp + ARecord.TimestampDelta;
end;

function KafkaDecodeRecordBatch(const AData: TBytes; AOffset, ASize: Integer;
  AMaxDecompressedSize: Integer): TKafkaRecordBatch;
var
  LReader, LRecordsReader: TKafkaReader;
  LMagic: Int8;
  LComputed: Cardinal;
  LCount: Int32;
  LPayload: TBytes;
  I: Integer;
begin
  if ASize < KAFKA_RECORD_BATCH_HEADER_SIZE then
    KafkaProtocolError('Record batch of %d bytes is shorter than its header (%d)',
      [ASize, KAFKA_RECORD_BATCH_HEADER_SIZE]);
  Result.Init;
  LReader := TKafkaReader.Create(AData, AOffset, ASize);
  try
    Result.BaseOffset := LReader.ReadInt64;
    LReader.ReadInt32; // batchLength: ASize already says it
    Result.PartitionLeaderEpoch := LReader.ReadInt32;
    LMagic := LReader.ReadInt8;
    if LMagic <> KAFKA_RECORD_BATCH_MAGIC then
      KafkaProtocolError('Record batch at offset %d has magic %d; only %d (Kafka 0.11 ' +
        'and later) is supported', [Result.BaseOffset, LMagic, KAFKA_RECORD_BATCH_MAGIC]);
    Result.Crc := LReader.ReadUInt32;
    LComputed := KafkaCrc32c(AData, AOffset + ATTRIBUTES_OFFSET, ASize - ATTRIBUTES_OFFSET);
    if LComputed <> Result.Crc then
      KafkaProtocolError('Record batch at offset %d is corrupt: CRC %.8x, computed %.8x',
        [Result.BaseOffset, Result.Crc, LComputed]);
    Result.Attributes := LReader.ReadInt16;
    Result.LastOffsetDelta := LReader.ReadInt32;
    Result.BaseTimestamp := LReader.ReadInt64;
    Result.MaxTimestamp := LReader.ReadInt64;
    Result.ProducerId := LReader.ReadInt64;
    Result.ProducerEpoch := LReader.ReadInt16;
    Result.BaseSequence := LReader.ReadInt32;
    LCount := LReader.ReadInt32;
    LPayload := LReader.ReadRaw(LReader.Remaining);
  finally
    LReader.Free;
  end;
  LPayload := KafkaDecompress(Result.Compression, LPayload, AMaxDecompressedSize);
  // A record takes at least 7 bytes; a count above the bytes is a lie that
  // would otherwise allocate it.
  if (LCount < 0) or (LCount > Length(LPayload)) then
    KafkaProtocolError('Record batch at offset %d claims %d records in %d bytes',
      [Result.BaseOffset, LCount, Length(LPayload)]);
  SetLength(Result.Records, LCount);
  LRecordsReader := TKafkaReader.Create(LPayload);
  try
    for I := 0 to LCount - 1 do
      ReadRecord(LRecordsReader, Result, Result.Records[I]);
    if not LRecordsReader.AtEnd then
      KafkaProtocolError('Record batch at offset %d has %d bytes after its %d records',
        [Result.BaseOffset, LRecordsReader.Remaining, LCount]);
  finally
    LRecordsReader.Free;
  end;
end;

function KafkaDecodeRecordBatches(const AData: TBytes; AMaxDecompressedSize: Integer;
  out ATrailingBytes: Integer): TKafkaRecordBatchArray;
var
  P, N, LCount: Integer;
  LLength: Int32;
begin
  Result := nil;
  LCount := 0;
  P := 0;
  N := Length(AData);
  while N - P >= KAFKA_RECORD_BATCH_LOG_OVERHEAD do
  begin
    LLength := GetInt32BE(AData, P + 8);
    if LLength < KAFKA_RECORD_BATCH_HEADER_SIZE - KAFKA_RECORD_BATCH_LOG_OVERHEAD then
    begin
      // Not a batch length at all; a magic check gives the better message
      // when the bytes are there (an old message set).
      if (N - P > MAGIC_OFFSET) and (AData[P + MAGIC_OFFSET] <> KAFKA_RECORD_BATCH_MAGIC) then
        KafkaProtocolError('Record batch at byte %d has magic %d; only %d is supported',
          [P, AData[P + MAGIC_OFFSET], KAFKA_RECORD_BATCH_MAGIC]);
      KafkaProtocolError('Record batch at byte %d has a length of %d', [P, LLength]);
    end;
    if Int64(LLength) + KAFKA_RECORD_BATCH_LOG_OVERHEAD > N - P then
      Break; // cut short by the fetch's byte limit
    if LCount = Length(Result) then
      SetLength(Result, LCount * 2 + 4);
    Result[LCount] := KafkaDecodeRecordBatch(AData, P, LLength + KAFKA_RECORD_BATCH_LOG_OVERHEAD,
      AMaxDecompressedSize);
    Inc(LCount);
    Inc(P, LLength + KAFKA_RECORD_BATCH_LOG_OVERHEAD);
  end;
  SetLength(Result, LCount);
  ATrailingBytes := N - P;
end;

end.
