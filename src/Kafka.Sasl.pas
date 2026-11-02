unit Kafka.Sasl;

{$I kafka.inc}

{ The client side of SASL PLAIN (RFC 4616) and SCRAM-SHA-256 (RFC 5802 and
  RFC 7677): the bytes a connection sends in SaslAuthenticate, and the
  checks on what the broker answers.

  Only the mechanisms live here. The exchange itself (SaslHandshake v1, then
  SaslAuthenticate until the mechanism is complete) is Kafka.Connection's,
  inside Open, after ApiVersions; it runs the same in plaintext
  (SASL_PLAINTEXT) and over TLS (SASL_SSL). A mechanism object is used for
  one exchange: InitialResponse once, then Evaluate with each answer until
  IsComplete, as Java's SaslClient.

  PLAIN sends authzid (empty), user name and password separated by NUL, in
  UTF-8, in one message; the broker's empty answer completes it. Over
  plaintext the password crosses the network as is: SASL_SSL is the setting
  for anything but a lab.

  SCRAM-SHA-256 follows RFC 5802 without channel binding (gs2 header "n,,",
  as Java's client): client-first with a random nonce, the server's salt
  and iteration count, client-final with the proof, and the server's
  signature checked (PcSameBytes, constant time) before the exchange counts
  as complete, so a broker that doesn't know the password can't pass for
  one that does. SHA-256, HMAC and PBKDF2 are pascal-common-faa's
  (PcSha256, PcHmacSha256, PcPbkdf2HmacSha256). As Java's client, the user
  name is escaped (= and , as =3D and =2C) but neither it nor the password
  goes through SASLprep: they are sent as their UTF-8 bytes, which is what
  Kafka's credential tools store.

  Iteration count: at least 4096 (RFC 7677's minimum, Java's check) and at
  most 16384, the largest Kafka's own tools accept for a credential
  (ScramMechanism in 4.4.0). Java has no upper check; this one keeps a
  hostile broker from asking for billions of PBKDF2 rounds.

  Base64. SCRAM uses standard Base64 with padding (RFC 4648 section 4);
  pascal-common-faa has only the URL alphabet without padding (for JWT).
  KafkaBase64Encode/KafkaTryBase64Decode translate between the two around
  the common's functions instead of carrying another Base64 here. If a
  second library needs the standard alphabet, it belongs in the common.

  Failures raise EKafkaAuthentication (Kafka.Types): a malformed or wrong
  answer from the broker, a server signature that doesn't match, a nonce
  that doesn't extend the client's. }

interface

uses
  SysUtils,
  Kafka.Types;

const
  /// RFC 7677's minimum for SCRAM-SHA-256 (also Java's check).
  KAFKA_SCRAM_MIN_ITERATIONS = 4096;
  /// Kafka's own maximum for a SCRAM credential (ScramMechanism, 4.4.0).
  KAFKA_SCRAM_MAX_ITERATIONS = 16384;

type
  /// sasl.mechanism. ksmNone: no SASL (security.protocol PLAINTEXT or SSL).
  TKafkaSaslMechanism = (ksmNone, ksmPlain, ksmScramSha256);

  { One client-side exchange of one mechanism. }
  TKafkaSaslClient = class
  public
    /// The name sent in SaslHandshake ('PLAIN', 'SCRAM-SHA-256').
    function Mechanism: string; virtual; abstract;
    /// The first message. Once, before any Evaluate.
    function InitialResponse: TBytes; virtual; abstract;
    /// The next message for the broker's AChallenge; nil when there is
    /// nothing more to send (then IsComplete). Raises EKafkaAuthentication
    /// when the answer is wrong.
    function Evaluate(const AChallenge: TBytes): TBytes; virtual; abstract;
    /// The exchange succeeded on the client's side.
    function IsComplete: Boolean; virtual; abstract;
  end;

  TKafkaSaslPlainClient = class(TKafkaSaslClient)
  private
    FUsername: string;
    FPassword: string;
    FSent: Boolean;
    FComplete: Boolean;
  public
    constructor Create(const AUsername, APassword: string);
    function Mechanism: string; override;
    function InitialResponse: TBytes; override;
    function Evaluate(const AChallenge: TBytes): TBytes; override;
    function IsComplete: Boolean; override;
  end;

  TKafkaScramSha256Client = class(TKafkaSaslClient)
  private type
    TState = (ssInitial, ssFirstSent, ssFinalSent, ssComplete, ssFailed);
  private
    FUsername: string;
    FPassword: string;
    FNonce: string;
    FState: TState;
    FClientFirstBare: TBytes;
    FServerSignature: TBytes;
    function ClientFinal(const AServerFirst: TBytes): TBytes;
    procedure CheckServerFinal(const AServerFinal: TBytes);
  public
    /// A random client nonce (32 Base64 characters from 24 bytes of the OS
    /// source).
    constructor Create(const AUsername, APassword: string);
    /// A given nonce: for tests with published vectors (RFC 7677).
    constructor CreateWithNonce(const AUsername, APassword, ANonce: string);
    function Mechanism: string; override;
    function InitialResponse: TBytes; override;
    function Evaluate(const AChallenge: TBytes): TBytes; override;
    function IsComplete: Boolean; override;
  end;

/// 'PLAIN', 'SCRAM-SHA-256'; '' for ksmNone.
function KafkaSaslMechanismName(AMechanism: TKafkaSaslMechanism): string;
/// The client for AMechanism (the caller frees it). EKafka for ksmNone, and
/// EKafkaAuthentication for an empty user name.
function KafkaCreateSaslClient(AMechanism: TKafkaSaslMechanism;
  const AUsername, APassword: string): TKafkaSaslClient;

/// A SCRAM saslname: '=' as '=3D' and ',' as '=2C' (RFC 5802 section 5.1).
function KafkaScramEscapeName(const AName: string): string;

/// Standard Base64 with padding (RFC 4648 section 4).
function KafkaBase64Encode(const ABytes: TBytes): string;
/// Standard Base64 with padding; False for anything else (the URL alphabet,
/// a missing pad, a length not a multiple of 4).
function KafkaTryBase64Decode(const AText: string; out ABytes: TBytes): Boolean;

implementation

uses
  PascalCommon.Crypto,
  PascalCommon.Random,
  PascalCommon.Utf8;

function KafkaSaslMechanismName(AMechanism: TKafkaSaslMechanism): string;
begin
  case AMechanism of
    ksmPlain: Result := 'PLAIN';
    ksmScramSha256: Result := 'SCRAM-SHA-256';
  else
    Result := '';
  end;
end;

function KafkaCreateSaslClient(AMechanism: TKafkaSaslMechanism;
  const AUsername, APassword: string): TKafkaSaslClient;
begin
  if AMechanism = ksmNone then
    raise EKafka.Create('No SASL mechanism was chosen');
  if AUsername = '' then
    raise EKafkaAuthentication.CreateFmt('SASL %s: the user name is empty',
      [KafkaSaslMechanismName(AMechanism)]);
  if AMechanism = ksmPlain then
    Result := TKafkaSaslPlainClient.Create(AUsername, APassword)
  else
    Result := TKafkaScramSha256Client.Create(AUsername, APassword);
end;

function KafkaScramEscapeName(const AName: string): string;
begin
  Result := StringReplace(AName, '=', '=3D', [rfReplaceAll]);
  Result := StringReplace(Result, ',', '=2C', [rfReplaceAll]);
end;

function KafkaBase64Encode(const ABytes: TBytes): string;
begin
  Result := PcBase64UrlEncode(ABytes);
  Result := StringReplace(Result, '-', '+', [rfReplaceAll]);
  Result := StringReplace(Result, '_', '/', [rfReplaceAll]);
  while Length(Result) mod 4 <> 0 do
    Result := Result + '=';
end;

function KafkaTryBase64Decode(const AText: string; out ABytes: TBytes): Boolean;
var
  LText: string;
  LPad: Integer;
begin
  ABytes := nil;
  if (Length(AText) mod 4 <> 0) or (Pos('-', AText) > 0) or (Pos('_', AText) > 0) then
    Exit(False);
  LText := AText;
  LPad := 0;
  while (LPad < 2) and (Length(LText) > 0) and (LText[Length(LText)] = '=') do
  begin
    Delete(LText, Length(LText), 1);
    Inc(LPad);
  end;
  if Pos('=', LText) > 0 then
    Exit(False);
  LText := StringReplace(LText, '+', '-', [rfReplaceAll]);
  LText := StringReplace(LText, '/', '_', [rfReplaceAll]);
  Result := PcBase64UrlDecode(LText, ABytes);
  if not Result then
    ABytes := nil;
end;

function Utf8(const AText: string): TBytes;
begin
  Result := PcStringToUtf8Bytes(AText);
end;

function Join(const A, B: TBytes): TBytes;
begin
  Result := nil;
  SetLength(Result, Length(A) + Length(B));
  if Length(A) > 0 then
    Move(A[0], Result[0], Length(A));
  if Length(B) > 0 then
    Move(B[0], Result[Length(A)], Length(B));
end;

function Hmac(const AKey: TBytes; const AData: TBytes): TBytes;
begin
  Result := PcDigestToBytes(PcHmacSha256(AKey, AData));
end;

{ TKafkaSaslPlainClient }

constructor TKafkaSaslPlainClient.Create(const AUsername, APassword: string);
begin
  inherited Create;
  if (Pos(#0, AUsername) > 0) or (Pos(#0, APassword) > 0) then
    raise EKafkaAuthentication.Create('SASL PLAIN: the user name or password contains a NUL');
  FUsername := AUsername;
  FPassword := APassword;
end;

function TKafkaSaslPlainClient.Mechanism: string;
begin
  Result := 'PLAIN';
end;

function TKafkaSaslPlainClient.InitialResponse: TBytes;
begin
  if FSent then
    raise EKafkaAuthentication.Create('SASL PLAIN: the initial response was already sent');
  FSent := True;
  // authzid (empty) NUL authcid NUL passwd
  Result := Utf8(#0 + FUsername + #0 + FPassword);
end;

function TKafkaSaslPlainClient.Evaluate(const AChallenge: TBytes): TBytes;
begin
  if (not FSent) or FComplete then
    raise EKafkaAuthentication.Create('SASL PLAIN: unexpected answer from the broker');
  // The broker answers PLAIN with nothing; there is no second step.
  if Length(AChallenge) <> 0 then
    raise EKafkaAuthentication.CreateFmt('SASL PLAIN: the broker answered %d bytes, ' +
      'expected none', [Length(AChallenge)]);
  FComplete := True;
  Result := nil;
end;

function TKafkaSaslPlainClient.IsComplete: Boolean;
begin
  Result := FComplete;
end;

{ TKafkaScramSha256Client }

constructor TKafkaScramSha256Client.Create(const AUsername, APassword: string);
begin
  CreateWithNonce(AUsername, APassword, KafkaBase64Encode(PcRandomBytes(24)));
end;

constructor TKafkaScramSha256Client.CreateWithNonce(const AUsername, APassword,
  ANonce: string);
var
  I: Integer;
begin
  inherited Create;
  if ANonce = '' then
    raise EKafkaAuthentication.Create('SCRAM: empty client nonce');
  // RFC 5802: printable ASCII except ','.
  for I := 1 to Length(ANonce) do
    if (Ord(ANonce[I]) < $21) or (Ord(ANonce[I]) > $7E) or (ANonce[I] = ',') then
      raise EKafkaAuthentication.Create('SCRAM: the client nonce must be printable ASCII ' +
        'without commas');
  FUsername := AUsername;
  FPassword := APassword;
  FNonce := ANonce;
end;

function TKafkaScramSha256Client.Mechanism: string;
begin
  Result := 'SCRAM-SHA-256';
end;

function TKafkaScramSha256Client.InitialResponse: TBytes;
begin
  if FState <> ssInitial then
    raise EKafkaAuthentication.Create('SCRAM: the initial response was already sent');
  FClientFirstBare := Utf8('n=' + KafkaScramEscapeName(FUsername) + ',r=' + FNonce);
  // gs2-header "n,,": no channel binding, no authzid.
  Result := Join(Utf8('n,,'), FClientFirstBare);
  FState := ssFirstSent;
end;

function TKafkaScramSha256Client.Evaluate(const AChallenge: TBytes): TBytes;
begin
  try
    case FState of
      ssFirstSent:
        begin
          Result := ClientFinal(AChallenge);
          FState := ssFinalSent;
        end;
      ssFinalSent:
        begin
          CheckServerFinal(AChallenge);
          FState := ssComplete;
          Result := nil;
        end;
    else
      raise EKafkaAuthentication.Create('SCRAM: unexpected answer from the broker');
    end;
  except
    FState := ssFailed;
    raise;
  end;
end;

function TKafkaScramSha256Client.IsComplete: Boolean;
begin
  Result := FState = ssComplete;
end;

type
  // Named: two 'array of string' declarations are two types on Delphi.
  TPartArray = array of string;

function SplitCommas(const AText: string): TPartArray;
var
  I, LStart, N: Integer;
begin
  Result := nil;
  N := 0;
  LStart := 1;
  for I := 1 to Length(AText) + 1 do
    if (I > Length(AText)) or (AText[I] = ',') then
    begin
      SetLength(Result, N + 1);
      Result[N] := Copy(AText, LStart, I - LStart);
      Inc(N);
      LStart := I + 1;
    end;
end;

function DigitsOnly(const AText: string): Boolean;
var
  I: Integer;
begin
  Result := (AText <> '') and (Length(AText) <= 9);
  for I := 1 to Length(AText) do
    if (AText[I] < '0') or (AText[I] > '9') then
      Exit(False);
end;

function TKafkaScramSha256Client.ClientFinal(const AServerFirst: TBytes): TBytes;
var
  LText, LNonce, LSaltText, LIterText: string;
  LSalt, LSalted, LClientKey, LStoredKey, LServerKey, LSignature, LProof: TBytes;
  LWithoutProof, LAuthMessage: TBytes;
  LParts: TPartArray;
  LIterations, I: Integer;
begin
  if not PcTryUtf8BytesToString(AServerFirst, LText) then
    raise EKafkaAuthentication.Create('SCRAM: the server-first message is not UTF-8');
  // r=<nonce>,s=<salt>,i=<count>[,<extensions>]; an "m=" first is a
  // mandatory extension this client doesn't know.
  if Copy(LText, 1, 2) = 'm=' then
    raise EKafkaAuthentication.Create('SCRAM: the broker asks for a mandatory extension');
  LParts := SplitCommas(LText);
  if (Length(LParts) < 3) or (Copy(LParts[0], 1, 2) <> 'r=') or
    (Copy(LParts[1], 1, 2) <> 's=') or (Copy(LParts[2], 1, 2) <> 'i=') then
    raise EKafkaAuthentication.CreateFmt('SCRAM: malformed server-first message "%s"', [LText]);
  LNonce := Copy(LParts[0], 3, MaxInt);
  LSaltText := Copy(LParts[1], 3, MaxInt);
  LIterText := Copy(LParts[2], 3, MaxInt);
  if (Length(LNonce) <= Length(FNonce)) or (Copy(LNonce, 1, Length(FNonce)) <> FNonce) then
    raise EKafkaAuthentication.Create('SCRAM: the server nonce doesn''t extend the client''s');
  if not KafkaTryBase64Decode(LSaltText, LSalt) or (Length(LSalt) = 0) then
    raise EKafkaAuthentication.CreateFmt('SCRAM: the salt "%s" is not Base64', [LSaltText]);
  if not DigitsOnly(LIterText) then
    raise EKafkaAuthentication.CreateFmt('SCRAM: the iteration count "%s" is not a number',
      [LIterText]);
  LIterations := StrToInt(LIterText);
  if (LIterations < KAFKA_SCRAM_MIN_ITERATIONS) or (LIterations > KAFKA_SCRAM_MAX_ITERATIONS) then
    raise EKafkaAuthentication.CreateFmt('SCRAM: the broker asks for %d iterations ' +
      '(accepted: %d to %d)', [LIterations, KAFKA_SCRAM_MIN_ITERATIONS,
      KAFKA_SCRAM_MAX_ITERATIONS]);
  // Anything after i= is an extension: ignored, as Java's client does.

  LSalted := PcPbkdf2HmacSha256(Utf8(FPassword), LSalt, LIterations, 32);
  LClientKey := Hmac(LSalted, Utf8('Client Key'));
  LStoredKey := PcDigestToBytes(PcSha256(LClientKey));
  LServerKey := Hmac(LSalted, Utf8('Server Key'));
  // c=biws is Base64("n,,"): the gs2 header again, no channel binding.
  LWithoutProof := Utf8('c=biws,r=' + LNonce);
  LAuthMessage := Join(Join(Join(FClientFirstBare, Utf8(',')), Join(AServerFirst, Utf8(','))),
    LWithoutProof);
  LSignature := Hmac(LStoredKey, LAuthMessage);
  LProof := nil;
  SetLength(LProof, Length(LClientKey));
  for I := 0 to High(LProof) do
    LProof[I] := LClientKey[I] xor LSignature[I];
  FServerSignature := Hmac(LServerKey, LAuthMessage);
  Result := Join(LWithoutProof, Utf8(',p=' + KafkaBase64Encode(LProof)));
end;

procedure TKafkaScramSha256Client.CheckServerFinal(const AServerFinal: TBytes);
var
  LText, LValue: string;
  LSignature: TBytes;
  P: Integer;
begin
  if not PcTryUtf8BytesToString(AServerFinal, LText) then
    raise EKafkaAuthentication.Create('SCRAM: the server-final message is not UTF-8');
  if Copy(LText, 1, 2) = 'e=' then
    raise EKafkaAuthentication.CreateFmt('SCRAM: the broker refused: %s',
      [Copy(LText, 3, MaxInt)]);
  if Copy(LText, 1, 2) <> 'v=' then
    raise EKafkaAuthentication.CreateFmt('SCRAM: malformed server-final message "%s"', [LText]);
  LValue := Copy(LText, 3, MaxInt);
  P := Pos(',', LValue);
  if P > 0 then
    LValue := Copy(LValue, 1, P - 1); // extensions after the verifier
  if not KafkaTryBase64Decode(LValue, LSignature) or
    not PcSameBytes(LSignature, FServerSignature) then
    raise EKafkaAuthentication.Create('SCRAM: the broker''s signature is wrong (it doesn''t ' +
      'know the password: not the broker it claims to be)');
end;

end.
