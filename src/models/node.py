import dataclasses
import ipaddress
import re

ID_LIMIT = 2**256
ID_PATTERN = re.compile(r"[0-9a-fA-F]{64}")


def validate_id(value: int) -> None:
    if type(value) is not int or not 0 <= value < ID_LIMIT:
        raise ValueError("ID must be 256-bit int")


def hex_to_id(value: str) -> int:
    """Return the ID (node, swarm, etc.) from a valid hex string.

    Parameters
    ----------
    value : str
        Should be a hex string containing exactly 64 characters.

    Returns
    -------
    int
        Unsigned 256-bit identifier.

    Raises
    ------
    ValueError
        The value is not a string of exactly 64 hexadecimal characters.
    """

    if not isinstance(value, str) or ID_PATTERN.fullmatch(value) is None:
        raise ValueError("Identifier must contain exactly 64 hexadecimal characters")
    return int(value, 16)


def id_to_hex(id: int) -> str:
    """Return the hex string from a valid ID.

    Parameters
    ----------
    id : int
        Identifier in the range `[0, 2**256)`.

    Returns
    -------
    str
        Identifier padded to 64 hexadecimal characters.

    Raises
    ------
    ValueError
        If `id` is not a valid ID.
    """

    validate_id(id)
    return f"{id:064x}"


@dataclasses.dataclass(frozen=True)
class NodeInfo:
    node_id: int
    ip: str
    port: int

    def __post_init__(self):
        validate_id(self.node_id)
        # Validate if it's a valid IP string.
        ipaddress.IPv4Address(self.ip)

        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("Port must be in [1, 65535]")

    def to_dict(self):
        """Convert `NodeInfo` to a `dict` with `node_id` in hex form.
    
        Returns
        -------
        dict
            A dictionary with `node_id` (hex form), `ip`, and `port` keys.
        """
    
        return {
            "node_id": id_to_hex(self.node_id), 
            "ip": self.ip, 
            "port": self.port,
        }
