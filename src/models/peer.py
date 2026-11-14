import dataclasses
import ipaddress


@dataclasses.dataclass(frozen=True)
class PeerInfo:
    ip: str
    port: int

    def __post_init__(self):
        ipaddress.IPv4Address(self.ip)
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("Port must be in [1, 65535]")
