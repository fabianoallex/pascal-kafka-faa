#!/usr/bin/env python3
"""Generates the Kafka message codecs (src/Kafka.Messages.*.pas) and the error
table (src/Kafka.Errors.pas) from Apache Kafka's own definitions.

Decision 3 of docs/design.md. Kafka publishes every request and response as
JSON (clients/src/main/resources/common/message/*.json): the fields, their
types, the versions each field exists in, the nullable and flexible versions
(compact strings and arrays, tagged fields, KIP-482) and the defaults. The
copies this script reads are committed under tools/kafka-messages/<tag>/, at
one Kafka tag (KAFKA_TAG below), so the build needs no network. The error
codes come from the same tag's Errors.java, and whether each is retriable or
calls for a metadata refresh from error-classes.tsv (each code's Java
exception and its superclasses, written by tools/fetch_error_classes.py).

What each generated unit holds, per message (e.g. MetadataRequest):

  - one record per struct, the message's own record last
    (TKafkaMetadataRequestTopic, ..., TKafkaMetadataRequest), with an array
    type per struct used as an array;
  - Init (the JSON defaults), Encode(W, AVersion) and Decode(R, AVersion) on
    every record; on the message's record also ToBytes(AVersion) and
    FromBytes(Data, AVersion), which refuses trailing bytes;
  - a field <Name>IsNull next to every field with nullableVersions: Pascal has
    no null string or array of its own;
  - UnknownTaggedFields on every record: the tagged fields of a newer
    version, kept so that Decode then Encode gives back the same bytes, as
    Java's generated classes do.

Rules taken from Kafka's generator (generator/src/main/java/.../
MessageDataGenerator.java), which is the reference when in doubt:

  - a field outside the version being written must hold its default unless it
    is "ignorable"; otherwise Encode raises EKafkaUnsupportedVersion (Java's
    UnsupportedVersionException), so a value is never dropped silently;
  - a null in a version that isn't nullable raises the same;
  - a tagged field is written only when it differs from its default, and the
    tagged section goes out in ascending tag order, the unknown ones
    interleaved;
  - a version marked "latestVersionUnstable" is not offered (Java only offers
    it with unstable.api.versions.enable).

The "records" type (Produce and Fetch) is carried as raw bytes, nullable, null
by default, as Java's generator does: the record batches inside are written
and read by hand in Kafka.Protocol.RecordBatch (decision 3).

A struct field (tagged or not) holds its default when it equals a fresh
Init'ed record, unknown tagged fields included; each record has IsDefault for
that (Java compares with equals against "new X()").

Not supported yet, and refused here with a message rather than generated
wrong: nullable struct fields. It comes with the first API that needs it.

Usage:
    python tools/gen_messages.py           write the units
    python tools/gen_messages.py --check   fail if any unit is out of date
"""

import json
import re
import sys
from pathlib import Path

KAFKA_TAG = "4.4.0"
ROOT = Path(__file__).resolve().parent.parent
DEFS = ROOT / "tools" / "kafka-messages" / KAFKA_TAG
SRC = ROOT / "src"

# The APIs generated, by the name of their JSON files without
# Request/Response. Adding one here is all it takes to generate it.
APIS = ["ApiVersions", "Metadata", "Produce", "Fetch", "ListOffsets", "SaslHandshake",
        "SaslAuthenticate"]

BOM = "﻿"

PASCAL_RESERVED = {
    "and", "array", "as", "asm", "begin", "case", "class", "const", "constructor",
    "destructor", "div", "do", "downto", "else", "end", "except", "exports", "file",
    "finalization", "finally", "for", "function", "goto", "if", "implementation", "in",
    "inherited", "initialization", "inline", "interface", "is", "label", "library", "mod",
    "nil", "not", "object", "of", "on", "operator", "or", "out", "packed", "procedure",
    "program", "property", "raise", "record", "repeat", "resourcestring", "set", "shl",
    "shr", "string", "then", "threadvar", "to", "try", "type", "unit", "until", "uses",
    "var", "while", "with", "xor", "self", "result", "length", "high", "low",
    "init", "encode", "decode", "tobytes", "frombytes", "unknowntaggedfields", "isdefault",
}

SCALARS = {
    # kafka type: (pascal type, writer method, reader method, zero literal)
    "int8": ("Int8", "WriteInt8", "ReadInt8", "0"),
    "int16": ("Int16", "WriteInt16", "ReadInt16", "0"),
    "int32": ("Int32", "WriteInt32", "ReadInt32", "0"),
    "int64": ("Int64", "WriteInt64", "ReadInt64", "0"),
    "uint16": ("Word", "WriteUInt16", "ReadUInt16", "0"),
    "uint32": ("Cardinal", "WriteUInt32", "ReadUInt32", "0"),
    "float64": ("Double", "WriteFloat64", "ReadFloat64", "0"),
    "bool": ("Boolean", "WriteBool", "ReadBool", "False"),
}

PRIMITIVE_ARRAYS = {
    # element type: (pascal array type, writer method, reader method)
    "int16": ("TKafkaInt16Array", "WriteInt16Array", "ReadInt16Array", "WriteInt16", "ReadInt16"),
    "int32": ("TKafkaInt32Array", "WriteInt32Array", "ReadInt32Array", "WriteInt32", "ReadInt32"),
    "int64": ("TKafkaInt64Array", "WriteInt64Array", "ReadInt64Array", "WriteInt64", "ReadInt64"),
    "string": ("TKafkaStringArray", "WriteStringArray", "ReadStringArray", None, None),
    "uuid": ("TKafkaUuidArray", "WriteUuidArray", "ReadUuidArray", "WriteUuid", "ReadUuid"),
}


class GenError(Exception):
    pass


# ---------------------------------------------------------------- JSON input

def strip_json_comments(text):
    """Kafka's JSON files have // comments; drop them outside strings."""
    out = []
    i, n = 0, len(text)
    in_str = False
    while i < n:
        c = text[i]
        if in_str:
            out.append(c)
            if c == "\\" and i + 1 < n:
                out.append(text[i + 1])
                i += 2
                continue
            if c == '"':
                in_str = False
            i += 1
            continue
        if c == '"':
            in_str = True
            out.append(c)
            i += 1
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            while i < n and text[i] != "\n":
                i += 1
            continue
        out.append(c)
        i += 1
    return "".join(out)


def load_def(name):
    path = DEFS / f"{name}.json"
    if not path.exists():
        raise GenError(f"{path} is missing: copy it from Kafka {KAFKA_TAG}")
    return json.loads(strip_json_comments(path.read_text(encoding="utf-8")))


# ---------------------------------------------------------------- versions

INF = 1 << 15


class Versions:
    """A Kafka version range: "0+", "1-3", "3" or "none"."""

    def __init__(self, text):
        text = (text or "none").strip()
        if text == "none":
            self.lo, self.hi = 1, 0
        elif text.endswith("+"):
            self.lo, self.hi = int(text[:-1]), INF
        elif "-" in text:
            a, b = text.split("-")
            self.lo, self.hi = int(a), int(b)
        else:
            self.lo = self.hi = int(text)

    @classmethod
    def of(cls, lo, hi):
        v = cls("none")
        v.lo, v.hi = lo, hi
        return v

    def empty(self):
        return self.lo > self.hi

    def intersect(self, other):
        return Versions.of(max(self.lo, other.lo), min(self.hi, other.hi))

    def contains(self, v):
        return self.lo <= v <= self.hi


def cond(rng, valid):
    """A Pascal condition on AVersion for rng, within the message's valid
    versions: 'True', 'False' or a comparison."""
    eff = rng.intersect(valid)
    if eff.empty():
        return "False"
    parts = []
    if eff.lo > valid.lo:
        parts.append(f"AVersion >= {eff.lo}")
    if eff.hi < valid.hi:
        parts.append(f"AVersion <= {eff.hi}")
    if not parts:
        return "True"
    if len(parts) == 1:
        return parts[0]
    return "(" + ") and (".join(parts) + ")"


def negate(c):
    if c == "True":
        return "False"
    if c == "False":
        return "True"
    return f"not ({c})"


def and_cond(a, b):
    if a == "False" or b == "False":
        return "False"
    if a == "True":
        return b
    if b == "True":
        return a
    return f"({a}) and ({b})"


def if_block(c, body, indent, else_body=None):
    """Lines for 'if c then begin body end [else begin else_body end]',
    folded when c is a constant."""
    pad = "  " * indent
    if not body and not else_body:
        return []
    if not body:
        return if_block(negate(c), else_body, indent)
    if c == "True":
        return [pad + line if line else line for line in _dedent(body)] if body else []
    if c == "False":
        return [pad + line if line else line for line in _dedent(else_body)] if else_body else []
    out = [f"{pad}if {c} then", f"{pad}begin"]
    out += [f"{pad}  {line}" if line else line for line in _dedent(body)]
    out.append(f"{pad}end")
    if else_body:
        out.append(f"{pad}else")
        out.append(f"{pad}begin")
        out += [f"{pad}  {line}" if line else line for line in _dedent(else_body)]
        out.append(f"{pad}end;")
    else:
        out[-1] += ";"
    return out


def _dedent(lines):
    return list(lines or [])


# ---------------------------------------------------------------- model

def pascal_ident(name):
    if name.lower() in PASCAL_RESERVED:
        return name + "_"
    return name


def pascal_string_literal(s):
    return "'" + s.replace("'", "''") + "'"


class Field:
    def __init__(self, raw, msg):
        self.raw = raw
        self.name = raw["name"]
        self.ident = pascal_ident(self.name)
        self.type = raw["type"]
        self.versions = Versions(raw.get("versions"))
        self.nullable = Versions(raw.get("nullableVersions"))
        self.tagged = Versions(raw.get("taggedVersions"))
        self.tag = raw.get("tag")
        self.ignorable = bool(raw.get("ignorable", False))
        self.about = raw.get("about", "")
        self.default_raw = raw.get("default")
        if raw.get("flexibleVersions") is not None:
            raise GenError(f"{msg.name}.{self.name}: per-field flexibleVersions is not supported")
        # records: raw nullable bytes, null by default (Java's generator).
        self.is_records = self.type == "records"
        if self.is_records:
            if self.nullable.empty():
                raise GenError(f"{msg.name}.{self.name}: a records field must be nullable")
            if self.default_raw is None:
                self.default_raw = "null"
        if (self.tag is None) != self.tagged.empty():
            raise GenError(f"{msg.name}.{self.name}: tag and taggedVersions go together")
        if not self.tagged.empty():
            if self.tagged.hi != INF or self.tagged.lo < self.versions.lo or self.versions.hi != INF:
                raise GenError(f"{msg.name}.{self.name}: taggedVersions must be a suffix of versions")
        self.is_array = self.type.startswith("[]")
        self.elem = self.type[2:] if self.is_array else self.type
        self.struct = None  # set by Message for struct types
        if self.is_array:
            self.kind = "array_prim" if self.elem in PRIMITIVE_ARRAYS else "array_struct"
        elif self.type in SCALARS:
            self.kind = "scalar"
        elif self.type in ("string", "bytes", "uuid"):
            self.kind = self.type
        elif self.is_records:
            self.kind = "bytes"
        else:
            self.kind = "struct"
        if self.kind == "struct" and not self.nullable.empty():
            raise GenError(f"{msg.name}.{self.name}: nullable struct fields are not supported yet")
        if self.kind in ("scalar", "uuid") and not self.nullable.empty():
            raise GenError(f"{msg.name}.{self.name}: a {self.type} can't be nullable")

    @property
    def has_null_flag(self):
        return not self.nullable.empty()

    @property
    def null_ident(self):
        return self.ident.rstrip("_") + "IsNull"

    def default_is_null(self):
        return self.default_raw == "null"

    def pascal_type(self):
        if self.kind == "scalar":
            return SCALARS[self.type][0]
        if self.kind == "string":
            return "string"
        if self.kind == "bytes":
            return "TBytes"
        if self.kind == "uuid":
            return "TKafkaUuid"
        if self.kind == "array_prim":
            return PRIMITIVE_ARRAYS[self.elem][0]
        if self.kind == "array_struct":
            return self.struct.array_type
        return self.struct.type_name

    def default_literal(self):
        """The Pascal value of the default, for scalars and strings."""
        d = self.default_raw
        if self.kind == "scalar":
            if self.type == "bool":
                if d is None:
                    return "False"
                if isinstance(d, bool):
                    return "True" if d else "False"
                return "True" if str(d).strip().lower() == "true" else "False"
            if d is None or d == "":
                return "0"
            if self.type == "float64":
                return repr(float(d))
            v = int(str(d).strip(), 0)
            bits = {"int8": 8, "int16": 16, "int32": 32, "int64": 64}.get(self.type)
            if bits and v == -(1 << (bits - 1)):
                return f"Low({SCALARS[self.type][0]})"
            return str(v)
        if self.kind == "string":
            if d is None or d == "null":
                return "''"
            return pascal_string_literal(str(d))
        return None

    def differs_from_default(self):
        """A Pascal expression: the field holds something other than its
        default (used for tagged fields and for absent, non-ignorable ones)."""
        f = self.ident
        n = self.null_ident
        if self.kind == "scalar":
            return f"{f} <> {self.default_literal()}"
        if self.kind == "uuid":
            return f"not {f}.IsZero"
        if self.kind == "string":
            if self.default_is_null():
                return f"not {n}"
            e = f"{f} <> {self.default_literal()}"
            return f"{n} or ({e})" if self.has_null_flag else e
        if self.kind in ("bytes", "array_prim", "array_struct"):
            if self.default_is_null():
                return f"not {n}"
            e = f"System.Length({f}) > 0"
            return f"{n} or ({e})" if self.has_null_flag else e
        return f"not {f}.IsDefault"  # struct


class Struct:
    def __init__(self, name, fields_raw, msg, versions):
        self.name = name
        self.msg = msg
        self.versions = versions
        base = msg.api_name
        self.type_name = f"TKafka{name}" if name.startswith(base) else f"TKafka{msg.name}{name}"
        self.array_type = self.type_name + "Array"
        self.fields = [Field(f, msg) for f in fields_raw]
        self.used_as_array = False


class Message:
    def __init__(self, raw, api_name, common_structs):
        self.raw = raw
        self.name = raw["name"]
        self.api_name = api_name
        self.api_key = raw["apiKey"]
        self.kind = raw["type"]
        self.valid = Versions(raw["validVersions"])
        if raw.get("latestVersionUnstable"):
            self.valid = Versions.of(self.valid.lo, self.valid.hi - 1)
        self.flexible = Versions(raw.get("flexibleVersions"))
        self.structs = []  # dependency order: children first
        self.common = {s["name"]: s for s in raw.get("commonStructs", [])}
        self.common.update(common_structs)
        self.common_built = {}
        self.root = Struct(self.name, [], self, self.valid)
        self.root.type_name = f"TKafka{self.name}"
        self.root.fields = [Field(f, self) for f in raw["fields"]]
        self._resolve(self.root)
        self.structs.append(self.root)

    def _resolve(self, struct):
        for f in struct.fields:
            if f.kind not in ("array_struct", "struct"):
                continue
            if "fields" in f.raw:
                child = Struct(f.elem, f.raw["fields"], self, f.versions)
                self._resolve(child)
                self.structs.append(child)
            elif f.elem in self.common_built:
                child = self.common_built[f.elem]
            elif f.elem in self.common:
                child = Struct(f.elem, self.common[f.elem]["fields"], self, Versions("0+"))
                self.common_built[f.elem] = child
                self._resolve(child)
                self.structs.append(child)
            else:
                raise GenError(f"{self.name}.{f.name}: unknown type {f.type}")
            f.struct = child
            if f.kind == "array_struct":
                child.used_as_array = True
        names = [s.type_name for s in self.structs]
        if len(names) != len(set(names)):
            raise GenError(f"{self.name}: two structs with the same name")


# ---------------------------------------------------------------- code: fields

def field_comment(f, indent):
    pad = "  " * indent
    text = f.about.strip()
    vers = describe_versions(f)
    lines = []
    if text:
        lines.append(f"{pad}/// {text}")
    lines.append(f"{pad}/// {vers}")
    return lines


def describe_versions(f):
    def r(v):
        if v.empty():
            return "none"
        return f"{v.lo}+" if v.hi == INF else (f"{v.lo}" if v.lo == v.hi else f"{v.lo}-{v.hi}")
    s = f"Versions {r(f.versions)}"
    if f.has_null_flag:
        s += f"; nullable {r(f.nullable)}"
    if f.tag is not None:
        s += f"; tagged {r(f.tagged)} (tag {f.tag})"
    if f.ignorable:
        s += "; ignorable"
    if f.default_raw is not None:
        s += f"; default {f.default_raw}"
    return s + "."


def write_field(f, w, compact, valid, ctx):
    """Lines that write field f with writer w; compact is a Pascal Boolean
    expression. Assumes the version check was done by the caller."""
    x = f.ident
    n = f.null_ident
    null_ok = cond(f.nullable, valid) if f.has_null_flag else "False"
    null_err = (f"raise EKafkaUnsupportedVersion.CreateFmt('{f.struct_owner}.{f.name} can''t be "
                f"null in version %d', [AVersion]);")
    if f.kind == "scalar":
        return [f"{w}.{SCALARS[f.type][1]}({x});"]
    if f.kind == "uuid":
        return [f"{w}.WriteUuid({x});"]
    if f.kind in ("string", "bytes"):
        kind = "String" if f.kind == "string" else "Bytes"
        if not f.has_null_flag:
            return [f"{w}.Write{kind}({x}, {compact});"]
        if null_ok == "True":
            return [f"{w}.WriteNullable{kind}({x}, {n}, {compact});"]
        nullable_branch = [f"{w}.WriteNullable{kind}({x}, {n}, {compact});"]
        strict_branch = [f"if {n} then", f"  {null_err}", f"{w}.Write{kind}({x}, {compact});"]
        return if_block(null_ok, nullable_branch, 0, strict_branch)
    if f.kind in ("array_prim", "array_struct"):
        if f.kind == "array_prim":
            body = [f"{w}.{PRIMITIVE_ARRAYS[f.elem][1]}({x}, {compact});"]
        else:
            ctx.add("LI")
            body = [f"{w}.WriteArrayLength(System.Length({x}), {compact});",
                    f"for LI := 0 to System.Length({x}) - 1 do",
                    f"  {x}[LI].Encode({w}, AVersion);"]
        if not f.has_null_flag:
            return body
        null_branch = [f"if {n} then", f"  {w}.WriteNullArray({compact})", "else", "begin"] + \
            ["  " + b for b in body] + ["end;"]
        strict = [f"if {n} then", f"  {null_err}"] + body
        if null_ok == "True":
            return null_branch
        return if_block(null_ok, null_branch, 0, strict)
    if f.kind == "struct":
        return [f"{x}.Encode({w}, AVersion);"]
    raise GenError(f.kind)


def read_field(f, r, compact, valid, ctx):
    x = f.ident
    n = f.null_ident
    null_ok = cond(f.nullable, valid) if f.has_null_flag else "False"
    if f.kind == "scalar":
        return [f"{x} := {r}.{SCALARS[f.type][2]};"]
    if f.kind == "uuid":
        return [f"{x} := {r}.ReadUuid;"]
    if f.kind in ("string", "bytes"):
        kind = "String" if f.kind == "string" else "Bytes"
        if not f.has_null_flag:
            return [f"{x} := {r}.Read{kind}({compact});"]
        nullable = [f"{x} := {r}.ReadNullable{kind}({compact}, {n});"]
        strict = [f"{x} := {r}.Read{kind}({compact});", f"{n} := False;"]
        return if_block(null_ok, nullable, 0, strict)
    if f.kind == "array_prim":
        info = PRIMITIVE_ARRAYS[f.elem]
        if not f.has_null_flag:
            return [f"{x} := {r}.{info[2]}({compact});"]
        ctx.add("LI")
        ctx.add("LN")
        elem_read = f"{r}.ReadString({compact})" if f.elem == "string" else f"{r}.{info[4]}"
        nullable = [f"LN := {r}.ReadArrayLength({compact});",
                    f"{n} := LN < 0;",
                    f"if LN < 0 then",
                    f"  LN := 0;",
                    f"SetLength({x}, LN);",
                    f"for LI := 0 to LN - 1 do",
                    f"  {x}[LI] := {elem_read};"]
        strict = [f"{x} := {r}.{info[2]}({compact});", f"{n} := False;"]
        return if_block(null_ok, nullable, 0, strict)
    if f.kind == "array_struct":
        ctx.add("LI")
        ctx.add("LN")
        tail = [f"SetLength({x}, LN);",
                f"for LI := 0 to LN - 1 do",
                f"begin",
                f"  {x}[LI].Init;",
                f"  {x}[LI].Decode({r}, AVersion);",
                f"end;"]
        if not f.has_null_flag:
            return [f"LN := {r}.ReadNonNullArrayLength({compact});"] + tail
        nullable = [f"LN := {r}.ReadArrayLength({compact});",
                    f"{n} := LN < 0;",
                    f"if LN < 0 then",
                    f"  LN := 0;"]
        strict = [f"LN := {r}.ReadNonNullArrayLength({compact});", f"{n} := False;"]
        return if_block(null_ok, nullable, 0, strict) + tail
    if f.kind == "struct":
        return [f"{x}.Init;", f"{x}.Decode({r}, AVersion);"]
    raise GenError(f.kind)


# ---------------------------------------------------------------- code: structs

class Ctx:
    def __init__(self):
        self.vars = set()

    def add(self, v):
        self.vars.add(v)


VAR_TYPES = {
    "LI": "Integer", "LN": "Integer", "LFlex": "Boolean", "LSub": "TKafkaWriter",
    "LRaw": "Integer", "LTagCount": "Cardinal", "LTagIndex": "Cardinal", "LTag": "Cardinal",
    "LSize": "Cardinal", "LStart": "Integer",
}


def var_section(ctx):
    if not ctx.vars:
        return []
    order = ["LFlex", "LI", "LN", "LSub", "LRaw", "LTagCount", "LTagIndex", "LTag", "LSize", "LStart"]
    out = ["var"]
    for v in order:
        if v in ctx.vars:
            out.append(f"  {v}: {VAR_TYPES[v]};")
    return out


def main_fields(s):
    """Fields written in the main sequence in some version."""
    return [f for f in s.fields]


def main_range(f):
    """The versions in which f is in the main sequence (not tagged)."""
    if f.tag is None:
        return f.versions
    return Versions.of(f.versions.lo, f.tagged.lo - 1)


def gen_encode(s, msg):
    valid = msg.valid
    ctx = Ctx()
    body = version_check(msg) if s is msg.root else []
    flex = cond(msg.flexible, valid)
    if flex not in ("True", "False"):
        ctx.add("LFlex")
        body.append(f"LFlex := {flex};")
        compact = "LFlex"
    else:
        compact = flex
    for f in s.fields:
        f.struct_owner = s.type_name[1:]
        present = cond(main_range(f), valid)
        write = write_field(f, "AWriter", compact, valid, ctx)
        # Absent: the field doesn't exist in this version (neither in the main
        # sequence nor tagged). Java refuses a non-default value there unless
        # the field is ignorable.
        absent = []
        if not f.ignorable and f.differs_from_default() is not None:
            absent = [f"if {f.differs_from_default()} then",
                      f"  raise EKafkaUnsupportedVersion.CreateFmt('{s.type_name[1:]}.{f.name} "
                      f"can''t be written in version %d', [AVersion]);"]
        if f.tag is None:
            body += if_block(present, write, 0, absent)
        else:
            body += if_block(negate(cond(f.versions, valid)), absent, 0)
            body += if_block(present, write, 0)
    tagged = sorted([f for f in s.fields if f.tag is not None], key=lambda f: f.tag)
    tag_lines = []
    ctx.add("LRaw")
    ctx.add("LTagCount")
    tag_lines.append("LTagCount := Cardinal(System.Length(UnknownTaggedFields));")
    for f in tagged:
        tc = and_cond(cond(f.tagged, valid), f.differs_from_default())
        tag_lines += if_block(tc, ["Inc(LTagCount);"], 0)
    tag_lines.append("AWriter.WriteUnsignedVarint(LTagCount);")
    tag_lines.append("LRaw := 0;")
    for f in tagged:
        ctx.add("LSub")
        tc = and_cond(cond(f.tagged, valid), f.differs_from_default())
        inner = [f"AWriter.WriteRawTaggedFieldsBelow(UnknownTaggedFields, LRaw, {f.tag});",
                 "LSub := TKafkaWriter.Create;",
                 "try"]
        inner += ["  " + l for l in write_field(f, "LSub", "True", valid, ctx)]
        inner += [f"  AWriter.WriteTaggedField({f.tag}, LSub.ToBytes);",
                  "finally",
                  "  LSub.Free;",
                  "end;"]
        tag_lines += if_block(tc, inner, 0)
    tag_lines.append("AWriter.WriteRawTaggedFieldsBelow(UnknownTaggedFields, LRaw, High(Int64));")
    if compact != "False":
        body += if_block(compact, tag_lines, 0)
    else:
        ctx.vars.discard("LRaw")
        ctx.vars.discard("LTagCount")
        ctx.vars.discard("LSub")
    out = [f"procedure {s.type_name}.Encode(AWriter: TKafkaWriter; AVersion: Int16);"]
    out += var_section(ctx)
    out.append("begin")
    out += ["  " + l if l else l for l in body]
    out.append("end;")
    return out


def gen_decode(s, msg):
    valid = msg.valid
    ctx = Ctx()
    body = version_check(msg) if s is msg.root else []
    flex = cond(msg.flexible, valid)
    if flex not in ("True", "False"):
        ctx.add("LFlex")
        body.append(f"LFlex := {flex};")
        compact = "LFlex"
    else:
        compact = flex
    body.append("Init;")
    for f in s.fields:
        present = cond(main_range(f), valid)
        body += if_block(present, read_field(f, "AReader", compact, valid, ctx), 0)
    tagged = sorted([f for f in s.fields if f.tag is not None], key=lambda f: f.tag)
    if compact != "False":
        for v in ("LTagCount", "LTagIndex", "LTag", "LSize"):
            ctx.add(v)
        loop = ["LTag := AReader.ReadUnsignedVarint;",
                "LSize := AReader.ReadUnsignedVarint;"]
        chain = []
        for f in tagged:
            ctx.add("LStart")
            tc = and_cond(f"LTag = {f.tag}", cond(f.tagged, valid))
            chain.append((tc, ["LStart := AReader.Position;"] +
                          read_field(f, "AReader", "True", valid, ctx) +
                          ["if AReader.Position - LStart <> Integer(LSize) then",
                           f"  KafkaProtocolError('{s.type_name[1:]}.{f.name}: tagged field "
                           f"of %d bytes read as %d', [Int64(LSize), AReader.Position - LStart]);"]))
        unknown = ["if Int64(LSize) > AReader.Remaining then",
                   "  KafkaProtocolError('Tagged field %d of %d bytes with only %d bytes left',",
                   "    [Int64(LTag), Int64(LSize), AReader.Remaining]);",
                   "KafkaAddRawTaggedField(UnknownTaggedFields, LTag, AReader.ReadRaw(Integer(LSize)));"]
        first = True
        for tc, lines in chain:
            kw = "if" if first else "else if"
            loop.append(f"{kw} {tc} then")
            loop.append("begin")
            loop += ["  " + l for l in lines]
            loop.append("end")
            first = False
        if chain:
            loop.append("else")
            loop.append("begin")
            loop += ["  " + l for l in unknown]
            loop.append("end;")
        else:
            loop += unknown
        tag_lines = ["LTagCount := AReader.ReadUnsignedVarint;",
                     "LTagIndex := 0;",
                     "while LTagIndex < LTagCount do",
                     "begin"]
        tag_lines += ["  " + l for l in loop]
        tag_lines += ["  Inc(LTagIndex);", "end;"]
        body += if_block(compact, tag_lines, 0)
    out = [f"procedure {s.type_name}.Decode(AReader: TKafkaReader; AVersion: Int16);"]
    out += var_section(ctx)
    out.append("begin")
    out += ["  " + l if l else l for l in body]
    out.append("end;")
    return out


def version_check(msg):
    pre = msg_const_prefix(msg)
    return [f"if (AVersion < {pre}_MIN_VERSION) or (AVersion > {pre}_MAX_VERSION) then",
            f"  raise EKafkaUnsupportedVersion.CreateFmt('{msg.name} version %d is not supported "
            f"(%d-%d)',",
            f"    [AVersion, {pre}_MIN_VERSION, {pre}_MAX_VERSION]);"]


def gen_init(s):
    out = [f"procedure {s.type_name}.Init;", "begin", "  Self := Default(" + s.type_name + ");"]
    for f in s.fields:
        if f.kind == "scalar":
            d = f.default_literal()
            if d not in ("0", "False"):
                out.append(f"  {f.ident} := {d};")
        elif f.kind == "string":
            d = f.default_literal()
            if d != "''":
                out.append(f"  {f.ident} := {d};")
            if f.has_null_flag and f.default_is_null():
                out.append(f"  {f.null_ident} := True;")
        elif f.kind in ("bytes", "array_prim", "array_struct"):
            if f.has_null_flag and f.default_is_null():
                out.append(f"  {f.null_ident} := True;")
        elif f.kind == "struct":
            out.append(f"  {f.ident}.Init;")
    out.append("end;")
    return out


def gen_is_default(s):
    terms = [f"({f.differs_from_default()})" for f in s.fields]
    terms.append("(System.Length(UnknownTaggedFields) > 0)")
    out = [f"function {s.type_name}.IsDefault: Boolean;", "begin", "  Result := not ("]
    out += [f"    {t}" + (" or" if i < len(terms) - 1 else ");") for i, t in enumerate(terms)]
    out.append("end;")
    return out


def msg_const_prefix(msg):
    return "KAFKA_" + re.sub(r"(?<!^)(?=[A-Z])", "_", msg.name).upper()


def gen_struct_decl(s, msg):
    out = []
    title = "The message itself." if s is msg.root else f"A {s.name} of {msg.name}."
    out.append(f"  {{ {title} }}")
    out.append(f"  {s.type_name} = record")
    for f in s.fields:
        out += field_comment(f, 2)
        out.append(f"    {f.ident}: {f.pascal_type()};")
        if f.has_null_flag:
            out.append(f"    /// True for a null {f.name}.")
            out.append(f"    {f.null_ident}: Boolean;")
    out.append("    /// Tagged fields this version of the library doesn't know.")
    out.append("    UnknownTaggedFields: TKafkaRawTaggedFields;")
    out.append("    /// Every field to its default (the definition's \"default\").")
    out.append("    procedure Init;")
    out.append("    procedure Encode(AWriter: TKafkaWriter; AVersion: Int16);")
    out.append("    /// Every field holds its default, unknown tagged fields included.")
    out.append("    function IsDefault: Boolean;")
    out.append("    /// Init, then the fields of AVersion.")
    out.append("    procedure Decode(AReader: TKafkaReader; AVersion: Int16);")
    if s is msg.root:
        out.append("    function ToBytes(AVersion: Int16): TBytes;")
        out.append("    /// Decodes all of AData; EKafkaProtocol when bytes are left over.")
        out.append("    procedure FromBytes(const AData: TBytes; AVersion: Int16);")
    out.append("  end;")
    if s.used_as_array:
        out.append(f"  {s.array_type} = array of {s.type_name};")
    out.append("")
    return out


def gen_root_helpers(msg):
    t = msg.root.type_name
    return [
        f"function {t}.ToBytes(AVersion: Int16): TBytes;",
        "var",
        "  LWriter: TKafkaWriter;",
        "begin",
        "  LWriter := TKafkaWriter.Create;",
        "  try",
        "    Encode(LWriter, AVersion);",
        "    Result := LWriter.ToBytes;",
        "  finally",
        "    LWriter.Free;",
        "  end;",
        "end;",
        "",
        f"procedure {t}.FromBytes(const AData: TBytes; AVersion: Int16);",
        "var",
        "  LReader: TKafkaReader;",
        "begin",
        "  LReader := TKafkaReader.Create(AData);",
        "  try",
        "    Decode(LReader, AVersion);",
        "    if not LReader.AtEnd then",
        f"      KafkaProtocolError('{msg.name} v%d: %d bytes left after the message',",
        "        [AVersion, LReader.Remaining]);",
        "  finally",
        "    LReader.Free;",
        "  end;",
        "end;",
        "",
    ]


def gen_api_unit(api, req, rsp):
    unit = f"Kafka.Messages.{api}"
    lines = [BOM + f"unit {unit};", "", "{$I kafka.inc}", ""]
    lines += [
        f"{{ {api} (API key {req.api_key}): the request and response codecs.",
        "",
        f"  GENERATED by tools/gen_messages.py from {req.name}.json and {rsp.name}.json",
        f"  of Apache Kafka {KAFKA_TAG} (tools/kafka-messages/{KAFKA_TAG}). Do not edit by",
        "  hand: change the generator or the definitions and run it again. What the",
        "  records hold and the rules the codecs follow are in the generator's",
        "  docstring.",
        "",
        f"  Versions: {req.valid.lo} to {req.valid.hi}; flexible from "
        f"{req.flexible.lo if not req.flexible.empty() else 'none'}. }}",
        "",
        "{$OVERFLOWCHECKS OFF}",
        "{$RANGECHECKS OFF}",
        "",
        "interface",
        "",
        "uses",
        "  SysUtils,",
        "  Kafka.Types,",
        "  Kafka.Protocol.Primitives;",
        "",
        "const",
    ]
    lines += [f"  /// API key {req.api_key}: KAFKA_API_KEY_{api_const(api)} in Kafka.Messages.Apis."]
    for m in (req, rsp):
        p = msg_const_prefix(m)
        lines += [f"  {p}_MIN_VERSION = {m.valid.lo};",
                  f"  {p}_MAX_VERSION = {m.valid.hi};"]
    lines += [f"  /// The first flexible version (compact forms and tagged fields).",
              f"  KAFKA_{api_const(api)}_FIRST_FLEXIBLE_VERSION = "
              f"{req.flexible.lo if not req.flexible.empty() else -1};",
              "", "type"]
    impl = ["implementation", ""]
    names = [s.type_name for m in (req, rsp) for s in m.structs]
    dups = sorted({n for n in names if names.count(n) > 1})
    if dups:
        raise GenError(f"{api}: request and response both define {', '.join(dups)}")
    for m in (req, rsp):
        for s in m.structs:
            lines += gen_struct_decl(s, m)
            impl.append(f"{{ {s.type_name} }}")
            impl.append("")
            impl += gen_init(s) + [""]
            impl += gen_is_default(s) + [""]
            impl += gen_encode(s, m) + [""]
            impl += gen_decode(s, m) + [""]
            if s is m.root:
                impl += gen_root_helpers(m)
    lines += impl
    lines.append("end.")
    return unit, "\n".join(lines) + "\n"


def api_const(api):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", api).upper()


def gen_apis_unit(msgs):
    lines = [BOM + "unit Kafka.Messages.Apis;", "", "{$I kafka.inc}", "",
             "{ The APIs this library has codecs for, and the versions of each.", "",
             f"  GENERATED by tools/gen_messages.py from Apache Kafka {KAFKA_TAG}'s message",
             "  definitions. Do not edit by hand.", "",
             "  Kafka.Connection negotiates against this table: for each API, the highest",
             "  version that both this table and the broker's ApiVersions answer allow.",
             "  The flexible version decides the header versions",
             "  (Kafka.Protocol.Envelope). }", "",
             "interface", "", "uses", "  SysUtils,", "  Kafka.Types;", "",
             "type",
             "  TKafkaApiInfo = record",
             "    ApiKey: Int16;",
             "    Name: string;",
             "    MinVersion: Int16;",
             "    MaxVersion: Int16;",
             "    /// -1 when no version is flexible.",
             "    FirstFlexibleVersion: Int16;",
             "  end;", "",
             "const"]
    for api, req, _ in msgs:
        lines.append(f"  KAFKA_API_KEY_{api_const(api)} = {req.api_key};")
    lines.append("")
    lines.append(f"  KAFKA_SUPPORTED_APIS: array[0..{len(msgs) - 1}] of TKafkaApiInfo = (")
    rows = []
    for api, req, _ in sorted(msgs, key=lambda m: m[1].api_key):
        ff = req.flexible.lo if not req.flexible.empty() else -1
        rows.append(f"    (ApiKey: {req.api_key}; Name: '{api}'; MinVersion: {req.valid.lo}; "
                    f"MaxVersion: {req.valid.hi}; FirstFlexibleVersion: {ff})")
    lines.append(",\n".join(rows))
    lines.append("  );")
    lines += ["",
              "/// The entry for AApiKey; False when the library has no codec for it.",
              "function KafkaFindApi(AApiKey: Int16; out AInfo: TKafkaApiInfo): Boolean;",
              "/// The API's name, or 'ApiKey(n)' for one the library doesn't know.",
              "function KafkaApiName(AApiKey: Int16): string;",
              "/// Whether AVersion of the API is flexible; EKafkaUnsupportedVersion for an",
              "/// API the library doesn't know.",
              "function KafkaApiIsFlexible(AApiKey, AVersion: Int16): Boolean;",
              "",
              "implementation",
              "",
              "function KafkaFindApi(AApiKey: Int16; out AInfo: TKafkaApiInfo): Boolean;",
              "var",
              "  I: Integer;",
              "begin",
              "  for I := Low(KAFKA_SUPPORTED_APIS) to High(KAFKA_SUPPORTED_APIS) do",
              "    if KAFKA_SUPPORTED_APIS[I].ApiKey = AApiKey then",
              "    begin",
              "      AInfo := KAFKA_SUPPORTED_APIS[I];",
              "      Exit(True);",
              "    end;",
              "  AInfo := Default(TKafkaApiInfo);",
              "  Result := False;",
              "end;",
              "",
              "function KafkaApiName(AApiKey: Int16): string;",
              "var",
              "  LInfo: TKafkaApiInfo;",
              "begin",
              "  if KafkaFindApi(AApiKey, LInfo) then",
              "    Result := LInfo.Name",
              "  else",
              "    Result := Format('ApiKey(%d)', [AApiKey]);",
              "end;",
              "",
              "function KafkaApiIsFlexible(AApiKey, AVersion: Int16): Boolean;",
              "var",
              "  LInfo: TKafkaApiInfo;",
              "begin",
              "  if not KafkaFindApi(AApiKey, LInfo) then",
              "    raise EKafkaUnsupportedVersion.CreateFmt('API key %d has no codec in this library',",
              "      [AApiKey]);",
              "  Result := (LInfo.FirstFlexibleVersion >= 0) and (AVersion >= LInfo.FirstFlexibleVersion);",
              "end;",
              "",
              "end."]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- errors

ENTRY_RE = re.compile(r"^    ([A-Z0-9_]+)\((-?\d+),", re.M)
LITERAL_RE = re.compile(r'"((?:[^"\\]|\\.)*)"')


def parse_errors():
    text = (DEFS / "Errors.java").read_text(encoding="utf-8")
    start = text.index("public enum Errors {")
    body = text[start:]
    end = body.index(";\n\n")
    body = body[:end + 1]
    entries = []
    matches = list(ENTRY_RE.finditer(body))
    for i, m in enumerate(matches):
        chunk = body[m.end(): matches[i + 1].start() if i + 1 < len(matches) else len(body)]
        cut = len(chunk)
        for marker in ("::new", "->"):
            k = chunk.find(marker)
            if k >= 0:
                cut = min(cut, k)
        msg = "".join(LITERAL_RE.findall(chunk[:cut]))
        msg = msg.replace('\\"', '"').replace("\\\\", "\\")
        entries.append((m.group(1), int(m.group(2)), msg))
    if len(entries) < 100:
        raise GenError(f"Errors.java: only {len(entries)} entries found; did the format change?")
    return entries


def parse_error_classes(entries):
    """code -> list of class names (the exception, then its superclasses)."""
    path = DEFS / "error-classes.tsv"
    if not path.exists():
        raise GenError(f"{path} is missing: run tools/fetch_error_classes.py")
    result = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        code, name, chain = line.split("\t")
        result[int(code)] = [] if chain == "-" else chain.split(">")
    codes = {code for _, code, _ in entries}
    if set(result) != codes:
        raise GenError("error-classes.tsv doesn't list the codes of Errors.java: "
                       "run tools/fetch_error_classes.py again")
    return result


def case_lines(items):
    """'    KAFKA_ERROR_A,' lines of a case label list, the last ending in ':'."""
    return [f"    KAFKA_ERROR_{name}{':' if i == len(items) - 1 else ','}"
            for i, (name, _) in enumerate(items)]


def gen_errors_unit(entries):
    classes = parse_error_classes(entries)
    retriable = [(n, c) for n, c, _ in entries if "RetriableException" in classes[c]]
    invalid_md = [(n, c) for n, c, _ in entries if "InvalidMetadataException" in classes[c]]
    lines = [BOM + "unit Kafka.Errors;", "", "{$I kafka.inc}", "",
             "{ Kafka's error codes: a constant per code, and its name and description.", "",
             f"  GENERATED by tools/gen_messages.py from Errors.java of Apache Kafka {KAFKA_TAG}",
             f"  (tools/kafka-messages/{KAFKA_TAG}). Do not edit by hand.", "",
             "  The descriptions are Java's, in English, so that a message logged here reads",
             "  like one logged by any other client. The two predicates follow the class",
             "  of each code's Java exception, from the same tag's sources",
             f"  (tools/kafka-messages/{KAFKA_TAG}/error-classes.tsv): RetriableException and",
             "  InvalidMetadataException (which is retriable too). }", "",
             "interface", "", "uses", "  SysUtils;", "", "const"]
    for name, code, _ in entries:
        lines.append(f"  KAFKA_ERROR_{name} = {code};")
    lines += ["",
              "/// UNKNOWN_TOPIC_OR_PARTITION for 3; 'UNKNOWN_ERROR_CODE(n)' for a code this",
              "/// table doesn't have (a newer broker's).",
              "function KafkaErrorName(ACode: Int16): string;",
              "/// Java's description of the code; '' for NONE and for an unknown code.",
              "function KafkaErrorDescription(ACode: Int16): string;",
              "/// 'NAME (code): description', for exception messages.",
              "function KafkaErrorText(ACode: Int16): string;",
              "/// The code's Java exception is a RetriableException: the same request may",
              "/// succeed if sent again. False for NONE and for an unknown code.",
              "function KafkaErrorIsRetriable(ACode: Int16): Boolean;",
              "/// The code's Java exception is an InvalidMetadataException: the client's",
              "/// metadata is stale (a leader moved, a topic was created or deleted), and a",
              "/// refresh comes before the retry.",
              "function KafkaErrorIsInvalidMetadata(ACode: Int16): Boolean;",
              "",
              "implementation",
              "",
              "function KafkaErrorName(ACode: Int16): string;",
              "begin",
              "  case ACode of"]
    for name, code, _ in entries:
        lines.append(f"    {code}: Result := '{name}';")
    lines += ["  else",
              "    Result := Format('UNKNOWN_ERROR_CODE(%d)', [ACode]);",
              "  end;",
              "end;",
              "",
              "function KafkaErrorDescription(ACode: Int16): string;",
              "begin",
              "  case ACode of"]
    for name, code, msg in entries:
        if msg:
            lines.append(f"    {code}: Result := {wrap_literal(msg)};")
    lines += ["  else",
              "    Result := '';",
              "  end;",
              "end;",
              "",
              "function KafkaErrorText(ACode: Int16): string;",
              "var",
              "  LDescription: string;",
              "begin",
              "  LDescription := KafkaErrorDescription(ACode);",
              "  if LDescription = '' then",
              "    Result := Format('%s (%d)', [KafkaErrorName(ACode), ACode])",
              "  else",
              "    Result := Format('%s (%d): %s', [KafkaErrorName(ACode), ACode, LDescription]);",
              "end;",
              "",
              "function KafkaErrorIsRetriable(ACode: Int16): Boolean;",
              "begin",
              "  case ACode of"]
    lines += case_lines(retriable)
    lines += ["      Result := True;",
              "  else",
              "    Result := False;",
              "  end;",
              "end;",
              "",
              "function KafkaErrorIsInvalidMetadata(ACode: Int16): Boolean;",
              "begin",
              "  case ACode of"]
    lines += case_lines(invalid_md)
    lines += ["      Result := True;",
              "  else",
              "    Result := False;",
              "  end;",
              "end;",
              "",
              "end."]
    return "\n".join(lines) + "\n"


def wrap_literal(s, width=80):
    if not all(32 <= ord(c) < 127 for c in s):
        raise GenError(f"non-ASCII error description: {s!r}")
    parts = []
    while len(s) > width:
        k = s.rfind(" ", 0, width)
        if k <= 0:
            k = width
        parts.append(s[:k + 1])
        s = s[k + 1:]
    parts.append(s)
    return (" +\n" + " " * 8).join(pascal_string_literal(p) for p in parts)


# ---------------------------------------------------------------- main

def generate():
    outputs = {}
    msgs = []
    for api in APIS:
        req = Message(load_def(f"{api}Request"), api, {})
        rsp = Message(load_def(f"{api}Response"), api, {})
        if req.api_key != rsp.api_key or req.valid.lo != rsp.valid.lo or req.valid.hi != rsp.valid.hi:
            raise GenError(f"{api}: request and response disagree on key or versions")
        if (req.flexible.lo, req.flexible.hi) != (rsp.flexible.lo, rsp.flexible.hi):
            raise GenError(f"{api}: request and response disagree on flexible versions")
        unit, text = gen_api_unit(api, req, rsp)
        outputs[SRC / f"{unit}.pas"] = text
        msgs.append((api, req, rsp))
    outputs[SRC / "Kafka.Messages.Apis.pas"] = gen_apis_unit(msgs)
    outputs[SRC / "Kafka.Errors.pas"] = gen_errors_unit(parse_errors())
    return outputs


def main(argv):
    check = "--check" in argv
    try:
        outputs = generate()
    except GenError as e:
        print(f"gen_messages: {e}", file=sys.stderr)
        return 2
    stale = []
    for path, text in outputs.items():
        data = text.encode("utf-8")
        if path.exists() and path.read_bytes() == data:
            continue
        if check:
            stale.append(path)
        else:
            path.write_bytes(data)
            print(f"wrote {path.relative_to(ROOT)}")
    if stale:
        for p in stale:
            print(f"out of date: {p.relative_to(ROOT)} (run python tools/gen_messages.py)")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
