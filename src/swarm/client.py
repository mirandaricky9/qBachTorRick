from src import utils
from src.models import peer


# TODO: Add more operations, like piece exchange.
class SwarmClient:
    """Responsible for handling peer operations.

    For now, it only store the result of `get_peers`.
    """

    def __init__(self, peer_port: int):
        self.__peer_port = peer_port
        self.__peers: dict[int, list[peer.PeerInfo]] = {}

    @property
    def peer_port(self) -> int:
        return self.__peer_port

    def update_peers(self, swarm_id: int, peers: list[peer.PeerInfo]):
        """Update the list of peers for a swarm, deduplicate by endpoint."""

        utils.validate_id(swarm_id)
        merged = {}
        for info in peers:
            if (info.ip, info.port) not in merged:
                merged[(info.ip, info.port)] = info
        self.__peers[swarm_id] = list(merged.values())

    def get_peers(self, swarm_id: int) -> list[peer.PeerInfo]:
        """Get peers for a swarm.
        This reads from local cache (no networking involved).
        """

        utils.validate_id(swarm_id)
        return list(self.__peers.get(swarm_id, []))

    def clear_swarm(self, swarm_id: int):
        utils.validate_id(swarm_id)
        self.__peers.pop(swarm_id, None)

    def clear(self):
        self.__peers.clear()
