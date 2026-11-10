"""Bounded fail-closed public write guard; no platform calls or session reads."""

import html
import ipaddress
import re
import unicodedata
from urllib.parse import unquote

_SECRET_KEY = re.compile(
    r"password|passwd|pwd|secret|token|api[_-]?key|authorization|cookie|credential|"
    r"private[_-]?key|密码|口令|令牌|密钥",
    re.I,
)


def _collect(value):
    pieces, ancestors = [], set()
    nodes, chars = 0, 0

    def visit(item, depth=0):
        nonlocal nodes, chars
        nodes += 1
        if depth > 16 or nodes > 4096:
            raise ValueError("OUTPUT_STRUCTURE_LIMIT")
        if item is None:
            return
        if isinstance(item, (list, dict)):
            identity = id(item)
            if identity in ancestors:
                raise ValueError("OUTPUT_CYCLE")
            ancestors.add(identity)
            if isinstance(item, dict):
                for key, child in item.items():
                    if _SECRET_KEY.search(str(key)) and child is not None and str(child).strip():
                        raise ValueError("SECRET_FIELD")
                    visit(child, depth + 1)
            else:
                for child in item:
                    visit(child, depth + 1)
            ancestors.remove(identity)
        else:
            text = str(item)
            chars += len(text)
            if chars > 32768:
                raise ValueError("OUTPUT_LIMIT")
            pieces.append(text)

    visit(value)
    return "\n".join(pieces)


def _decode_percent_run(value):
    try:
        return unquote(value, errors="strict")
    except UnicodeDecodeError:
        return value


def blocked_reason(value):
    try:
        text = _collect(value)
    except (ValueError, RecursionError) as exc:
        return str(exc) if isinstance(exc, ValueError) else "OUTPUT_STRUCTURE_LIMIT"
    text = unicodedata.normalize("NFKC", text)
    for _ in range(4):
        before = text
        text = html.unescape(text)
        text = re.sub(
            r"(?:%[0-9a-f]{2})+",
            lambda match: _decode_percent_run(match.group(0)),
            text,
            flags=re.I,
        )
        text = unicodedata.normalize("NFKC", text)
        text = re.sub(r"§[0-9a-fk-orx]", "", text, flags=re.I)
        text = re.sub(r"[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]", "", text)
        if text == before:
            break
    if re.search(r"(?:&#(?:x[0-9a-f]+|\d+);?|%[0-9a-f]{2})", text, re.I):
        return "OUTPUT_ENCODING_LIMIT"
    text = re.sub(r"\[\s*(?:dot|点)\s*\]|\(\s*dot\s*\)|[。．]", ".", text, flags=re.I)
    dense = re.sub(r"\s+", "", text)
    dense = re.sub(r"(?<=\d)点(?=\d)", ".", dense)
    candidates = re.findall(r"(?<![\d.])(?:\d{1,3}\s*\.\s*){3}\d{1,3}(?![\d.])", text)
    candidates += re.findall(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])", dense)
    for token in candidates:
        token = re.sub(r"\s+", "", token)
        try:
            ipaddress.ip_address(token)
            return "IP_ADDRESS"
        except ValueError:
            pass
    for token in re.findall(r"[0-9a-fA-F:.%\[\]-]{3,}", text):
        try:
            if ":" in token and ipaddress.ip_address(token.strip("[]").split("%")[0]).version == 6:
                return "IP_ADDRESS"
        except ValueError:
            pass
    if re.search(
        r"(?:password|passwd|pwd|secret|token|api[_ -]?key|authorization|cookie|密码|口令|令牌|密钥)"
        r"[\s\"\x27]*(?:是|为|[:：=])[\s\"\x27]*[^\s\"\x27，。；;]{2,}",
        text,
        re.I,
    ):
        return "SECRET_ASSIGNMENT"
    if re.search(
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----|bearer\s+\S{8,}|\bsk-[A-Za-z0-9_-]{12,}|"
        r"\beyJ[\w-]+\.[\w-]+\.[\w-]+|(?:mysql|redis|postgres(?:ql)?):\/\/[^\s]+@|"
        r"\/(?:login|register|changepassword|l|reg)\s+\S+",
        text,
        re.I,
    ):
        return "CREDENTIAL"
    return None
