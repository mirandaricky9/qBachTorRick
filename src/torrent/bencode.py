# Encoding of objects into a stream of bytes
# Decoding a stream of bytes into an object
# Bencoded dictionary is the method used for torrent files

import typing as t


class BencodeError(ValueError):
    """Raised when a value can't be encoded or the data isn't valid bencode."""

def encode(value: t.Any) -> bytes:
    """Encode a python value as bencode bytes."""
    out: list[bytes] = []
    _encode(value, out)

    return b"".join(out)

def _encode(value: t.Any, out: list[bytes]) -> None:
    """Recursively encode a python value (if needed) to bencoded bytes.
    

    Keyword arguments:
    value -- the python value that is to be bencoded
    out   -- a list of pieces that get appended, later joined by encode()
    """

    # checks if a boolean value has been passed, which is not allowed
    if isinstance(value, bool):
        raise BencodeError("Booleans not supported, use 0 or 1.")

    # append an integer with the start and end markers
    if isinstance(value, int):
        out.append(b"i%de" % value)

    elif isinstance(value, (bytes, bytearray)):
        out.append(b"%d:" % len(value))
        out.append(bytes(value))
    
    elif isinstance(value, str):
        _encode(value.encode("utf-8"), out)

    elif isinstance(value, (list, tuple)):
        out.append(b"l")
        for item in value:
            _encode(item, out)
        out.append(b"e")

    elif isinstance(value, dict):
        _encode_dict(value, out)

    else:
        raise BencodeError(f"Cannot bencode type: {type(value).__name__}")


def _encode_dict(value: dict, out: list[bytes]) -> None:
    items: dict[bytes, t.Any] = {}
    for key, item in value.items():
        if isinstance(key, str):
            key = key.encode("utf-8")
        elif not isinstance(key, (bytes, bytearray)):
            raise BencodeError(f"Dictionary keys must be str or bytes, got {type(key).__name__}")
        key = bytes(key)

        if key in items:
            raise BencodeError(f"Duplicate dictionary key {key!r}")

        items[key] = item

    out.append(b"d")
    for key in sorted(items):
        _encode(key, out)
        _encode(items[key], out)
    out.append(b"e")

# All decoding functions take a string of bytes

def decode(data: bytes) -> t.Any:
    """Decode a complete bencoded byte string."""

    if not isinstance(data, (bytes, bytearray)):
        raise BencodeError("decode() expects bytes")
    data = bytes(data)

    try:
        value, end = _decode_at(data, 0)
    except RecursionError:
        raise BencodeError("Data is nested too deeply") from None

    if end != len(data):
        raise BencodeError(f"Trailing data at byte {end}")
    return value

def _decode_at(data: bytes, i: int) -> tuple[t.Any, int]:
    """
    Bencode supports four different types of values:
    - integers, 
    - lists,
    - dictionaries
    - and strings
    """
    if i >= len(data):
        raise BencodeError("Unexpected end of data")

    lead = data[i : i + 1]

    if lead == b"i":
        return _decode_int(data, i)
    if lead == b"l":
        return _decode_list(data, i)
    if lead == b"d":
        return _decode_dict(data, i)
    if lead.isdigit():
        return _decode_string(data, i)

    raise BencodeError(f"Unexpected byte {lead!r} at byte {i}")


def _decode_int(data: bytes, i : int) -> tuple[int, int]:
    end = data.find(b"e", i + 1)

    if end == -1:
        raise BencodeError(f"Unterminated integer byte at {i}")

    digits = data[i + 1 : end]

    if not _is_acceptable_int(digits):
        raise BencodeError(f"Invalid integer {digits!r} at byte {i}")
    
    return int(digits), end + 1

def _is_acceptable_int(digits: bytes) -> bool:
    body = digits[1:] if digits.startswith(b"-") else digits

    if not body.isdigit():
        return False
    
    if body.startswith(b"0"):
        return digits == b"0"

    return True


def _decode_string(data: bytes, i: int) -> tuple[bytes, int]:
    colon = data.find(b":", i)

    if colon == -1:
        raise BencodeError(f"Missing ':' in string length at byte {i}")

    length_digits = data[i:colon]

    if not length_digits.isdigit() or (len(length_digits) > 1 and length_digits.startswith(b"0")):
        raise BencodeError(f"Invalid string length {length_digits!r} at byte {i}")

    start = colon + 1
    end = start + int(length_digits)

    if end > len(data):
        raise BencodeError(f"String at byte {i} runs past end of data")

    return data[start:end], end

def _decode_list(data: bytes, i: int) -> tuple[list, int]:
    i += 1
    items = []

    # while the end marker is not read
    while data[i : i + 1] != b"e":
        item, i = _decode_at(data, i)
        items.append(item)

    return items, i + 1


def _decode_dict(data: bytes, i: int) -> tuple[dict, int]:
    i += 1
    result: dict[bytes, t.Any] = {}
    previous_key: bytes | None = None

    while data[i : i + 1] != b"e":
        if not data[i : i + 1].isdigit():
            raise BencodeError(f"Dictionary key must be a byte string at byte {i}")
        key, i = _decode_string(data, i)

        if previous_key is not None and key <= previous_key:
            raise BencodeError(f"Dictionary keys unsorted or duplicated at {key!r}")

        value, i = _decode_at(data, i)
        result[key] = value
        previous_key = key

    return result, i + 1

