import dataclasses
import ipaddress


@dataclasses.dataclass(frozen=True)
class DHTConfig:
    advertise_ip: str
    """Reachable IP address"""

    port: int = 6881
    bind_ip: str = "0.0.0.0"

    bootstrap: tuple | None = None
    """Tuple of (IP, port) where it should contact for initial DHT node entry."""

    k: int = 8
    """See KBuckets"""

    alpha: int = 3
    """Number of concurrent FIND_NODES request."""

    replicas: int = 2
    """Number of nodes a swarm need to register."""

    rpc_timeout: float = 2
    """Timeout in seconds for RPC responses."""

    failure_limit: int = 3
    """See RoutingTable"""

    idle_threshold: float = 30
    """See RoutingTable"""

    maintenance_interval: float = 10
    """How often background maintenance checks contacts."""

    registration_ttl: float = 1800
    """How long a swarm entry remains valid without another announcement."""

    reannounce_interval: float = 600
    """How often this node re-announce its swarm membership. Must be smaller than `registration_ttl`."""

    refresh_interval: float = 900
    """Bucket inactivity threshold before scheduling a random lookup. See `RoutingTable.get_refresh_targets()`."""

    def __post_init__(self):
        if not isinstance(self.advertise_ip, str) or not isinstance(self.bind_ip, str):
            raise TypeError("Addresses must be literal IPv4 strings")
        ipaddress.IPv4Address(self.bind_ip)
        advertised = ipaddress.IPv4Address(self.advertise_ip)
        if (
            advertised.is_unspecified
            or advertised.is_multicast
            or int(advertised) == 0xFFFFFFFF
        ):
            raise ValueError("'advertise_ip' must be a reachable unicast IPv4 address")
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("Invalid UDP port")
        if self.reannounce_interval >= self.registration_ttl:
            raise ValueError(
                "Reannounce interval must be shorter than registration TTL"
            )
