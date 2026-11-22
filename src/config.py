import dataclasses


@dataclasses.dataclass(frozen=True)
class DHTConfig:
    port: int = 6881
    """UDP port for DHT operations."""

    bootstrap: tuple | None = None
    """Tuple of (IP, port) where it should contact for initial DHT node entry."""

    k: int = 8
    """See KBuckets"""

    alpha: int = 3
    """Max number of concurrent `FIND_NODE` and `GET_PEERS` requests per lookup."""

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

    # registration_ttl: float = 1800
    registration_ttl: float = 20
    """How long a swarm entry remains valid without another announcement."""

    # reannounce_interval: float = 600
    reannounce_interval: float = 15
    """How often this node re-announce its swarm membership. Must be smaller than `registration_ttl`."""

    refresh_interval: float = 900
    """Bucket inactivity threshold before scheduling a random lookup. See `RoutingTable.get_refresh_targets()`."""

    def __post_init__(self):
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("Invalid UDP port")
        if self.reannounce_interval >= self.registration_ttl:
            raise ValueError(
                "Reannounce interval must be shorter than registration TTL"
            )


@dataclasses.dataclass(frozen=True)
class PeerConfig:
    port: int = 9000
    """TCP listening port for peer operations."""

    handshake_timeout: float = 10
    """Max number of seconds a peer handshake should last."""

    # Should be at most half of receive_timeout.
    heartbeat_interval: float = 30
    """How often to send keep-alive message."""

    # peer_timeout in libtorrent.
    # This is similar to idle_threshold in DHTConfig.
    receive_timeout: float = 90
    """How long a connection should wait for any activity before closing it."""

    maintenance_interval: float = 1
    """How often maintenance checks should be run. Affect how fast we detect the above issues."""

    pex_interval: float = 60
    """How often to check for peer exchange updates."""

    pex_max_entries: int = 50
    """Maximum endpoints in each added or dropped PEX list."""

    def __post_init__(self):
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("Invalid TCP port")
        if self.heartbeat_interval >= self.receive_timeout:
            raise ValueError(
                "Heartbeat interval shouldn't be larger than receive timeout."
            )
