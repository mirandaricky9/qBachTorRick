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

Supported message types are: `HANDSHAKE`, `KEEP_ALIVE`, `BITFIELD`, `HAVE`, `REQUEST`, `PIECE`, `CANCEL`, and `PEX`.
"""

import json
import typing as t

from src import utils

VALID_PEER_MESSAGE_TYPES = ("HANDSHAKE", "KEEP_ALIVE")


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
