import dataclasses
import time
import typing as t

from src.models import node


@dataclasses.dataclass
class SwarmRegistration:
    swarm_id: int
    peer: node.NodeInfo
    expires_at: float


class SwarmStore:
    """Store swarm peers.

    Three main operations: `put()`, `get()`, and `remove_expired()`.
    """

    def __init__(
        self,
        ttl: float = 1800,
        clock: t.Callable[[], float] = time.monotonic,
    ):
        if not 0 < ttl < float("inf"):
            raise ValueError("TTL must be positive and finite")

        self.__ttl = ttl
        self.__clock = clock
        # {swarm_id: {peer_id: SwarmRegistration(), ...}, ...}
        self.__registrations: dict[int, dict[int, SwarmRegistration]] = {}

    def put(self, swarm_id: int, peer: node.NodeInfo):
        """Register a peer as part of the swarm."""

        node.validate_id(swarm_id)
        peers = self.__registrations.setdefault(swarm_id, {})
        peers[peer.node_id] = SwarmRegistration(
            swarm_id,
            peer,
            self.__clock() + self.__ttl,
        )

    def get(self, swarm_id: int):
        """Get a list of fresh peers for the swarm."""

        node.validate_id(swarm_id)
        now = self.__clock()
        return [
            record.peer
            for record in self.__registrations.get(swarm_id, {}).values()
            if record.expires_at > now
        ]

    def remove_expired(self):
        """Remove expired peers from swarm membership."""
        
        now = self.__clock()
        # We're deleting stuff so iterator is not safe.
        for swarm_id, peers in list(self.__registrations.items()):
            for peer_id, record in list(peers.items()):
                if record.expires_at <= now:
                    del peers[peer_id]
            if not peers:
                del self.__registrations[swarm_id]
