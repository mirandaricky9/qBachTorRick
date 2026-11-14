"""
Utils to handle DHT network messages.

Message Structure
-----------------
All messages have the following format:
```json
{
    "version": 1,
    "type": "request, response, or error",
    "txid": "32 lowercase hex characters",
    "sender_id": "ID of the node in hex",
    "other_fields": "..."
}
```

- For compacting reasons, `sender_id` is hex instead of binary 256-bit string. Therefore, `sender_id` should have exactly 64 hex characters.
- The `response` message will repeat the corresponding `request`'s `txid`.
"""

import json

from src import utils
from src.models import node

MAX_DATAGRAM = 1200
VALID_MESSAGE_TYPES = ("request", "response", "error")
METHODS = {"PING", "FIND_NODE", "ANNOUNCE", "GET_PEERS"}


def info_to_payload(info: node.NodeInfo) -> dict:
    """Convert `NodeInfo` to a `dict`.

    Returns
    -------
    dict
        A dictionary with `node_id` (hex form), `ip`, and `port` keys.
    """

    return {
        "node_id": utils.id_to_hex(info.node_id),
        "ip": info.ip,
        "port": info.port,
    }


def encode(message: dict) -> bytes:
    """Serialize a message as a compact UTF-8 JSON datagram.

    Parameters
    ----------
    message : dict
        The message.

    Returns
    -------
    bytes
        No larger than `MAX_DATAGRAM` bytes.

    Raises
    ------
    TypeError
        Message can't be serialized to JSON.
    ValueError
        Encoded message exceed `MAX_DATAGRAM` bytes.
    """

    data = json.dumps(message, separators=(",", ":"), allow_nan=False).encode("utf-8")
    if len(data) > MAX_DATAGRAM:
        raise ValueError("Oversized datagram")
    return data


def decode(data: bytes) -> dict:
    """Decode a JSON datagram and validate its common message fields.

    Parameters
    ----------
    data : bytes
        Received JSON datagram, no larger than `MAX_DATAGRAM` bytes.

    Returns
    -------
    dict
        A valid common message.

    Raises
    ------
    ValueError
        Invalid message values.
    TypeError
        Invalid message types.
    """

    if len(data) > MAX_DATAGRAM:
        raise ValueError("Oversized datagram")

    message = json.loads(data)

    if not isinstance(message, dict):
        raise TypeError("Message is not an object")
    if type(message.get("version")) is not int or message["version"] != 1:
        raise ValueError("Unsupported protocol version")
    if message.get("type") not in VALID_MESSAGE_TYPES:
        raise ValueError("Invalid message type")

    txid = message.get("txid")
    if not isinstance(txid, str):
        raise TypeError("Invalid transaction ID type")
    sender_id = message.get("sender_id")
    if not isinstance(sender_id, str):
        raise TypeError("Invalid sender ID type")
    # Validate sender_id further.
    utils.hex_to_id(sender_id)

    return message


def get_message_template(kind: str, txid: str, sender_id: int):
    """Return a message with common fields.

    Parameters
    ----------
    kind : str
        Message type: `request`, `response`, or `error`.
    txid : str
        Transaction ID, 32 lowercase hexadecimal characters.
    sender_id : int
        Sender's unsigned 256-bit node identifier.

    Returns
    -------
    dict
        A message with `version`, `type`, `txid`, and `sender_id` fields.

    Raises
    ------
    ValueError, TypeError
        If any of the fields are invalid.
    """

    if kind not in VALID_MESSAGE_TYPES:
        raise ValueError("Invalid message type.")
    if not isinstance(txid, str):
        raise TypeError("Invalid transaction ID type")

    return {
        "version": 1,
        "type": kind,
        "txid": txid,
        "sender_id": utils.id_to_hex(sender_id),
    }
