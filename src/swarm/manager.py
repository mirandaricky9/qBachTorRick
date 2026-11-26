"""Peer operations and connection ownership for one swarm."""

import asyncio
import logging
import typing as t

from src import config
from src.models import peer
from src.network import transport
from src.swarm import connection, peerutils
from src.torrent import availability

LOGGER = logging.getLogger(__name__)


class SwarmManager:
    """Manage peer connections for a specific swarm."""

    def __init__(
        self,
        swarm_id: int,
        peer_config: config.PeerConfig,
        clock: t.Callable[[], float],
        state: availability.TorrentAvailability | None = None,
    ):
        self.__swarm_id = swarm_id
        self.__config = peer_config
        self.__clock = clock
        # It's possible for state to be None since BitTorrent allows
        # magnet link which gives the infohash for DHT lookup.
        # BEP 9 is then used to obtain the metadata from peers.
        # ...But that's out of scope for now :)
        self.__state = state
        self.__connections: set[connection.PeerConnection] = set()
        # Only contain peers discovered via DHT.
        # Peers discovered via PEX is managed by PeerConnection.
        self.__peers: list[peer.PeerInfo] = []
        self.__is_closed = False

    @property
    def swarm_id(self) -> int:
        return self.__swarm_id

    def create_connection(
        self, channel: transport.TCPTransport, peer_id: int, endpoint: peer.PeerInfo
    ) -> connection.PeerConnection:
        """Take ownership of a successfully handshaken transport."""

        if self.__is_closed:
            raise RuntimeError("Swarm manager is closed")

        state = self.__state
        conn = connection.PeerConnection(
            channel,
            self.__swarm_id,
            peer_id,
            endpoint,
            self.__config,
            self.__clock,
            self.__connections.discard,
            self.get_connections,
            piece_count=state.piece_count if state is not None else None,
            initial_local_pieces=state.bitmap if state is not None else 0,
        )
        self.__connections.add(conn)
        conn.start()
        return conn

    def get_connections(self):
        """Return active connections in this swarm."""

        return [conn for conn in self.__connections if conn.is_connected]

    def update_peers(self, peers: list[peer.PeerInfo]):
        """Update peers discovered via DHT.
        Do not use this to update peers discovered via PEX."""

        self.__peers = list(set(peers))

    def get_cached_peers(self):
        """Get discovered peers for this swarm from local cache.
        This method doesn't perform networking calls.

        Returns
        -------
        list[PeerInfo]
            A list of discovered peers (via DHT and PEX) for a swarm.
        """

        merged = set(self.__peers)
        for conn in self.get_connections():
            for info in conn.get_pex_peers():
                merged.add(info)
        return list(merged)

    def clear(self):
        """Clear all discovered peers in the cache."""

        self.__peers.clear()
        for conn in self.__connections:
            conn.clear_pex_peers()

    async def mark_piece_verified(self, index: int):
        """Mark a piece within the bitmap as verified and announce this.
        This will send HAVE messages to all active peer connections (within the swarm).
        """

        if self.__is_closed:
            raise RuntimeError("Swarm manager is closed")
        if self.__state is None:
            raise ValueError("Torrent not loaded")

        message = peerutils.get_have_msg(index)
        peerutils.parse_have(message, self.__state.piece_count)

        # If the bitmap already have the piece, skip announcing.
        if self.__state.bitmap & (1 << index):
            return

        self.__state.bitmap |= 1 << index
        await asyncio.gather(
            *(self.__send_have(conn, message) for conn in self.get_connections())
        )

    async def close(self):
        """Close every owned connection and stop accepting new ones."""

        self.__is_closed = True
        closing = asyncio.gather(*(conn.close() for conn in list(self.__connections)))
        try:
            await asyncio.shield(closing)
        except asyncio.CancelledError:
            # Do not orphan connections when a leave operation is cancelled.
            await closing
            raise
        finally:
            self.__connections.clear()
            self.clear()

    async def __send_have(self, conn, message):
        try:
            await conn.send(message)
        except (OSError, EOFError, ValueError, TimeoutError) as error:
            LOGGER.debug("HAVE delivery failed: %s", error)
            await conn.close()
