import dataclasses
import ipaddress

ID_LIMIT = 2**256


def validate_id(value: int) -> None:
    if type(value) is not int or not 0 <= value < ID_LIMIT:
        raise ValueError("ID must be 256-bit int")


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
            raise ValueError("port must be in [1, 65535]")
