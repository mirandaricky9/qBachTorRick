"""
Utils to handle peer network messages.

Message Structure
-----------------
All messages have the following format:
```json
{
    "version": 1,
    "type": "MESSAGE_TYPE",
    "other_fields": "..."
}
```

Supported message types are: `HANDSHAKE`, `KEEP_ALIVE`, `PEX`, `BITFIELD`,
`HAVE`, `REQUEST`, `PIECE`, and `CANCEL`.
"""

import base64
import binascii
import json
import typing as t

from src import utils
from src.models import peer

VALID_PEER_MESSAGE_TYPES = (
    "HANDSHAKE",
    "KEEP_ALIVE",
    "PEX",
    "BITFIELD",
    "HAVE",
    "REQUEST",
    "PIECE",
    "CANCEL",
)


def get_bitfield_msg(pieces: int):
    return {"version": 1, "type": "BITFIELD", "pieces": pieces}


def get_have_msg(index: int):
    return {"version": 1, "type": "HAVE", "index": index}


def get_handshake_msg(swarm_id: int, peer_id: int, port: int) -> dict[str, t.Any]:
    return {
        "version": 1,
        "type": "HANDSHAKE",
        "swarm_id": utils.id_to_hex(swarm_id),
        "peer_id": utils.id_to_hex(peer_id),
        "port": port,
    }


def get_keep_alive_msg():
    return {
        "version": 1,
        "type": "KEEP_ALIVE",
    }


def get_pex_msg(added: list[peer.PeerInfo], dropped: list[peer.PeerInfo]):
    return {
        "version": 1,
        "type": "PEX",
        "added": [(info.ip, info.port) for info in added],
        "dropped": [(info.ip, info.port) for info in dropped],
    }


def get_request_msg(index: int, begin: int, length: int):
    return {
        "version": 1,
        "type": "REQUEST",
        "index": index,
        "begin": begin,
        "length": length,
    }


def get_piece_msg(index: int, begin: int, data: bytes):
    return {
        "version": 1,
        "type": "PIECE",
        "index": index,
        "begin": begin,
        "block": base64.b64encode(data).decode("ascii"),
    }


def get_cancel_msg(index: int, begin: int, length: int):
    return {
        "version": 1,
        "type": "CANCEL",
        "index": index,
        "begin": begin,
        "length": length,
    }


def parse_peer_handshake(message: dict[str, t.Any]):
    """Parse a peer HANDSHAKE message to obtain IDs and port number.

    Parameters
    ----------
    message : dict[str, t.Any]
        A handshake message (with the `type` field as `HANDSHAKE`).

    Returns
    -------
    tuple[int, int, int]
        Swarm ID, peer ID, and peer port number, respectively.

    Raises
    ------
    ValueError
        Invalid message values.
    TypeError
        Invalid message types.
    """

    if message["type"] != "HANDSHAKE":
        raise ValueError("Expected peer handshake")
    swarm_id = utils.hex_to_id(message["swarm_id"])
    peer_id = utils.hex_to_id(message["peer_id"])
    port = int(message["port"])
    return swarm_id, peer_id, port


def parse_pex(message: dict[str, t.Any], max_entries: int):
    """Return added and dropped endpoint sets."""

    added = _parse_pex_endpoints(message["added"], max_entries)
    dropped = _parse_pex_endpoints(message["dropped"], max_entries)
    if added & dropped:
        raise ValueError("PEX cannot add and drop the same endpoint")
    return added, dropped


def _parse_pex_endpoints(
    entries: list[tuple[str, int]],
    max_entries: int,
) -> set[peer.PeerInfo]:
    if len(entries) > max_entries:
        entries = entries[:max_entries]

    endpoints = set()
    for ip, port in entries:
        info = peer.PeerInfo(ip, port)
        if info in endpoints:
            raise ValueError("Duplicate PEX endpoint")
        endpoints.add(info)
    return endpoints

def parse_bitfield(message: dict[str, t.Any], piece_count: int):
    """Parse `BITFIELD` message for the bitmap and verify if it matches the piece count.

    Raises
    ------
    ValueError
        The bitmap is longer than the expected piece count.
    """

    pieces = _non_negative_int(message, "pieces")
    if pieces.bit_length() > piece_count:
        raise ValueError("BITFIELD contains an out-of-range piece")
    return pieces


def parse_have(message: dict[str, t.Any], piece_count: int):
    """Parse `HAVE` message for the advertised index and verify if it is valid.

    Raises
    ------
    ValueError
        The index is outside the piece count.
    """

    index = _non_negative_int(message, "index")
    if index >= piece_count:
        raise ValueError("HAVE index is outside the torrent")
    return index


def parse_block_request(message: dict[str, t.Any]):

    if message["type"] not in ("REQUEST", "CANCEL"):
        raise ValueError("Expected a REQUEST or CANCEL message.")

    index = _non_negative_int(message, "index")
    begin = _non_negative_int(message, "begin")
    length = _non_negative_int(message, "length")

    if length == 0:
        raise ValueError("Block length must be positive")

    return index, begin, length


def parse_piece(message: dict[str, t.Any]):

    if message["type"] != "PIECE":
        raise ValueError("Expected PIECE type.")

    index = _non_negative_int(message, "index")
    begin = _non_negative_int(message, "begin")
    block = message.get("block")
    if not isinstance(block, str):
        raise ValueError("PIECE block must be a base64 string")

    try:
        data = base64.b64decode(block, validate=True)
    except binascii.Error as error:
        raise ValueError(f"Invalid base64 in PIECE: {error}") from None

    return index, begin, data


def _non_negative_int(message: dict[str, t.Any], key: str):
    value = message.get(key)

    if type(value) is not int or value < 0:
        raise ValueError(f"'{key}' must be a non-negative int")

    return value


def encode(message: dict):
    """Serialize a peer message as a UTF-8 JSON.

    Parameters
    ----------
    message : dict
        The message.

    Raises
    ------
    TypeError
        Message can't be serialized to JSON.
    """

    return json.dumps(message, separators=(",", ":"), allow_nan=False).encode("utf-8")


def decode(data: bytes) -> dict[str, t.Any]:
    """Decode and validate a peer message."""

    message = json.loads(data)

    if not isinstance(message, dict):
        raise TypeError("Peer frame must be an object")
    if type(message.get("version")) is not int or message["version"] != 1:
        raise ValueError("Unsupported peer protocol version")
    if message.get("type") not in VALID_PEER_MESSAGE_TYPES:
        raise ValueError("Unknown peer message type")

    return message
