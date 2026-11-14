import dataclasses
import ipaddress

from src import utils


@dataclasses.dataclass(frozen=True)
class NodeInfo:
    node_id: int
    ip: str
    port: int

    def __post_init__(self):
        utils.validate_id(self.node_id)
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
            "node_id": utils.id_to_hex(self.node_id),
            "ip": self.ip,
            "port": self.port,
        }

    def __repr__(self) -> str:
        return f"NodeInfo(node_id={utils.id_to_hex(self.node_id)}, ip={self.ip}, port={self.port})"
