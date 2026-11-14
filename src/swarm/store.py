import dataclasses
import time
import typing as t

from src.models import node, peer


@dataclasses.dataclass
class SwarmRegistration:
    swarm_id: int
    peer: peer.PeerInfo
    expires_at: float


class PeerRegistrationStore:
    """Store swarm announcements.

    When a peer announce a torrent/swarm, the system uses the torrent infohash/swarm ID to locate multiple DHT nodes to store this registration.
    Later on, when someone wants to find this torrent, they will arrive at these nodes and see the announcement, thereby locating the peer.

    A peer announcing the swarm ID essentially self-register it to the swarm.
    To keep this status, on top of exchanging peer information within the swarm, it must periodically reannounce to the DHT using the same swarm ID.
    If it fails to reannounce within the defined interval, the peer implies it left the swarm.

    This class store these registrations and keep track whether they are fresh.
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
        # {swarm_id: {(ip, port): SwarmRegistration(), ...}, ...}
        self.__registrations: dict[int, dict[tuple[str, int], SwarmRegistration]] = {}

    def put(self, swarm_id: int, peer: peer.PeerInfo):
        """Register a peer as part of the swarm."""

        node.validate_id(swarm_id)
        peers = self.__registrations.setdefault(swarm_id, {})
        peers[(peer.ip, peer.port)] = SwarmRegistration(
            swarm_id,
            peer,
            self.__clock() + self.__ttl,
        )

    def get(self, swarm_id: int):
        """Get a list of fresh peers for a swarm."""

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
