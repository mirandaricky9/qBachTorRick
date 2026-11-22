import asyncio
import logging
import secrets
import time
import typing as t

from src import config, utils
from src.models import peer
from src.network import transport
from src.swarm import connection, peerutils

LOGGER = logging.getLogger(__name__)


class SwarmClient:
    """Responsible for handling peer operations.

    Store result of `get_peers()`, keeping peer connection alive.
    """

    def __init__(
        self,
        peer_config: config.PeerConfig,
        clock: t.Callable[[], float] = time.monotonic,
    ):
        # BitTorrent uses a peer ID separate from DHT node ID.
        # Many clients also keep one peer ID throughout its lifetime.
        self.__peer_id = secrets.randbits(256)
        self.__config = peer_config
        self.__clock = clock
        self.__peers: dict[int, list[peer.PeerInfo]] = {}
        self.__memberships: set[int] = set()
        self.__connections: set[connection.PeerConnection] = set()
        # Track task ownership separately from pending connections by swarm.
        self.__bg_tasks: set[asyncio.Task[None]] = set()
        self.__fg_tasks: set[asyncio.Task[t.Any]] = set()

        # Track TCP handshakes so we can cleanup if we terminate early.
        self.__pending_tcp_handshakes: dict[
            asyncio.Task[None], transport.TCPTransport
        ] = {}
        # Track pending peer handshake so we can cancel if we leave a torrent before
        # a PeerConnection is established.
        # dict[asyncio.Task, int] is necessary because there can be multiple
        # handshakes for the same torrent infohash.
        # This is different from tcp_handshakes cuz the TCP could fail
        # before we even get to the peer handshakes.
        self.__pending_peer_handshakes: dict[asyncio.Task[None], int] = {}

        self.__leaving: set[int] = set()
        self.__server: asyncio.Server | None = None

    @property
    def peer_port(self) -> int:
        return self.__config.port

    @property
    def peer_id(self) -> int:
        return self.__peer_id

    def __spawn_task(self, coroutine):
        """Spawn and return an async task to run the coroutine.

        This is meant to run background tasks (dispatch and don't really care about the return result).
        If you want to run foreground tasks (dispatch and wait for result), use `SwarmClient.__run_task()` instead."""

        task = asyncio.create_task(coroutine)
        self.__bg_tasks.add(task)
        task.add_done_callback(self.__task_done)
        return task

    def __task_done(self, task):
        """A callback when a background task is finished.
        Used by `__spawn_task()` only.
        """

        self.__bg_tasks.discard(task)
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                LOGGER.error(
                    "Background operation failed: %s",
                    error,
                    exc_info=(type(error), error, error.__traceback__),
                )

    async def __run_task(self, coroutine):
        """Run asynchronously and return the result of the coroutine.

        This is meant to run foreground tasks (dispatch and wait for result).
        If you want to run background tasks (dispatch and don't really care about return result), use `SwarmClient.__spawn_task()` instead.
        """

        task = asyncio.create_task(coroutine)
        self.__fg_tasks.add(task)
        try:
            return await task
        finally:
            self.__fg_tasks.discard(task)

    async def start(self, bind_ip: str):
        """Open the peer TCP socket and setup internal state.

        This must be called before using any other peer operations.
        """

        if self.__server is not None:
            raise RuntimeError("Peer listener already running")

        self.__server = await asyncio.start_server(
            self.__accept_tcp_handshake,
            host=bind_ip,
            port=self.__config.port,
            start_serving=False,
        )
        try:
            await self.__server.start_serving()
        except BaseException:
            await self.stop()
            raise

    def join_swarm(self, swarm_id: int):
        """Locally register itself as part of the given swarm.

        This is used when you're announcing to the DHT.
        For other ways to join a swarm, use `connect_peer()`.
        """

        self.__require_running()
        utils.validate_id(swarm_id)
        if swarm_id in self.__leaving:
            raise RuntimeError(f"Leaving swarm {swarm_id}")

        self.__memberships.add(swarm_id)

    async def connect_peer(
        self,
        swarm_id: int,
        endpoint: peer.PeerInfo,
        bind_ip: str,
    ) -> connection.PeerConnection:
        """Join locally and establish a peer connection for a torrent infohash.

        Connection failures leave local membership intact.

        Parameters
        ----------
        swarm_id : int
            The swarm ID to join.
        endpoint : PeerInfo
            The peer's endpoint. Could be obtained via `DHTNode.get_peers()`.
        bind_ip : str
            The IP address you bind to. This is to correctly identify IP address in some scenarios.
            See notes in `TCPTransport.connect()` for more details.

        Returns
        -------
        PeerConnection
            The resulting connection if the handshake succeeds.
        """

        self.join_swarm(swarm_id)
        return await self.__run_task(self.__connect(swarm_id, endpoint, bind_ip))

    def get_connections(self, swarm_id: int):
        """Return active peer connections for a swarm."""

        return [
            conn
            for conn in self.__connections
            if conn.swarm_id == swarm_id and conn.is_connected
        ]

    def update_peers(self, swarm_id: int, peers: list[peer.PeerInfo]):
        """Update peers discovered via DHT.
        Do not use this to update peers discovered via PEX.
        """

        utils.validate_id(swarm_id)
        merged = {}
        for info in peers:
            if (info.ip, info.port) not in merged:
                merged[(info.ip, info.port)] = info
        self.__peers[swarm_id] = list(merged.values())

    def get_cached_peers(self, swarm_id: int):
        """Get discovered peers for a swarm from local cache.
        This method doesn't perform networking calls.

        Parameters
        ----------
        swarm_id : int
            The swarm ID.

        Returns
        -------
        list[PeerInfo]
            A list of discovered peers (via DHT and PEX) for a swarm.
        """

        utils.validate_id(swarm_id)
        merged = dict.fromkeys(self.__peers.get(swarm_id, []))
        for conn in self.get_connections(swarm_id):
            for info in sorted(
                conn.get_pex_peers(), key=lambda info: (info.ip, info.port)
            ):
                merged[info] = None
        return list(merged)

    # Should probably be private. Not sure if it's gonna be useful or not...
    def clear_swarm(self, swarm_id: int):
        """Clear discovered peers for a swarm in the cache.

        To leave, use `leave()`.
        """

        self.__peers.pop(swarm_id, None)
        for conn in self.__connections:
            if conn.swarm_id == swarm_id:
                conn.clear_pex_peers()

    def clear(self):
        """Clear all discovered peers.

        To leave or close connections, use `leave()` or `stop()`.
        """

        self.__peers.clear()
        for conn in self.__connections:
            conn.clear_pex_peers()

    async def leave(self, swarm_id: int):
        """Leave a torrent.

        Notes
        -----
        This will close all connections related to the torrent.
        """

        utils.validate_id(swarm_id)
        await self.__run_task(self.__leave(swarm_id))

    async def stop(self):
        """Stop the client."""

        server = self.__server
        self.__server = None
        if server is not None:
            server.close()

        self.__memberships.clear()
        tasks = self.__bg_tasks | self.__fg_tasks
        for task in tasks:
            task.cancel()

        # pending_tcp_handshakes has a callback to remove finished tasks
        # (in accept_tcp_handshakes()) so we need to freeze it with list.
        transports_close = [
            channel.close() for channel in list(self.__pending_tcp_handshakes.values())
        ]
        # same reasoning as above
        conn_close = [conn.close() for conn in list(self.__connections)]
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*transports_close, *conn_close)

        if server is not None:
            await server.wait_closed()

        self.__bg_tasks.clear()
        self.__fg_tasks.clear()
        self.__pending_peer_handshakes.clear()
        self.__pending_tcp_handshakes.clear()
        self.__connections.clear()
        self.__leaving.clear()
        self.clear()

    def __require_running(self):
        if self.__server is None:
            raise RuntimeError("Peer listener is not running")

    def __accept_tcp_handshake(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        """Accept an incoming TCP handshake and process it."""

        channel = transport.TCPTransport(reader, writer)
        task = self.__spawn_task(self.__handle_tcp_handshake(channel))
        self.__pending_tcp_handshakes[task] = channel
        task.add_done_callback(
            lambda done: self.__pending_tcp_handshakes.pop(done, None)
        )

    async def __handle_tcp_handshake(self, channel: transport.TCPTransport):
        """Process the TCP handshake to convert into a peer handshake.

        Parameters
        ----------
        channel : TCPTransport
            A wrapper over the created connection.
        """

        this_task = asyncio.current_task()
        # Suppress the annoying type warning.
        assert this_task is not None

        is_conn_created = False
        try:
            async with asyncio.timeout(self.__config.handshake_timeout):
                message = peerutils.decode(await channel.receive())
                # Peer want to joins swarm_id.
                swarm_id, peer_id, peer_port = peerutils.parse_peer_handshake(message)
                peer_endpoint = peer.PeerInfo(channel.peer_addr[0], peer_port)

                if peer_id == self.__peer_id:
                    raise ValueError("Cannot connect to itself")
                if swarm_id not in self.__memberships:
                    raise ValueError("Peer requested an unknown swarm")

                # It's possible to disconnect before this get registered,
                # which would leave the cleanup logic skip this task in particular.
                # This isn't a big deal since this will send the message,
                # which fails, and eventually close the channel.
                # Even if the message send succeed somehow, it'll create
                # the connection and attempt to keep-alive, which fails.
                self.__pending_peer_handshakes[this_task] = swarm_id

                await channel.send(
                    peerutils.encode(
                        peerutils.get_handshake_msg(
                            swarm_id,
                            self.__peer_id,
                            self.__config.port,
                        )
                    )
                )
                self.__create_connection(channel, swarm_id, peer_id, peer_endpoint)
                is_conn_created = True
        except (OSError, EOFError, ValueError, TimeoutError) as error:
            LOGGER.debug("Incoming peer handshake rejected: %s", error)
        finally:
            try:
                if not is_conn_created:
                    await channel.close()
            finally:
                self.__pending_peer_handshakes.pop(this_task, None)

    async def __connect(self, swarm_id: int, endpoint: peer.PeerInfo, bind_ip: str):
        """Establish a connection to a peer for a specific torrent infohash.

        Parameters
        ----------
        swarm_id : int
            The swarm ID to participate in.
        endpoint : PeerInfo
            The peer's endpoint. Could be obtained via `DHTNode.get_peers()`.

        Returns
        -------
        PeerConnection
            The resulting connection if the handshake succeeds.

        Raises
        ------
        ValueError
            Invalid handshake.
        """

        this_task = asyncio.current_task()
        # Suppress the annoying type warning.
        assert this_task is not None
        self.__pending_peer_handshakes[this_task] = swarm_id

        channel = None
        is_conn_created = False
        try:
            async with asyncio.timeout(self.__config.handshake_timeout):
                channel = await transport.TCPTransport.connect(
                    (endpoint.ip, endpoint.port), bind_ip
                )
                await channel.send(
                    peerutils.encode(
                        peerutils.get_handshake_msg(
                            swarm_id,
                            self.__peer_id,
                            self.__config.port,
                        )
                    )
                )
                message = peerutils.decode(await channel.receive())
                remote_swarm, peer_id, peer_port = peerutils.parse_peer_handshake(
                    message
                )

                if remote_swarm != swarm_id:
                    raise ValueError("Peer replied for a different swarm")
                if remote_swarm not in self.__memberships:
                    raise ValueError("Peer requested an unknown swarm")
                if peer_id == self.__peer_id:
                    raise ValueError("Cannot connect to itself")

                peer_endpoint = peer.PeerInfo(channel.peer_addr[0], peer_port)
                conn = self.__create_connection(
                    channel, swarm_id, peer_id, peer_endpoint
                )
                is_conn_created = True
                return conn
        finally:
            try:
                if channel is not None and not is_conn_created:
                    await channel.close()
            finally:
                self.__pending_peer_handshakes.pop(this_task, None)

    def __create_connection(
        self,
        channel: transport.TCPTransport,
        swarm_id: int,
        peer_id: int,
        endpoint: peer.PeerInfo,
    ):
        """Create a peer connection between this and the peer.

        Parameters
        ----------
        channel : TCPTransport
            The underlying TCP connection between the two peers.
        swarm_id : int
            The swarm ID of the connection.
        peer_id : int
            The peer's ID.
        endpoint : PeerInfo
            The peer's endpoint.

        Returns
        -------
        PeerConnection
            The connection between this and the peer.
        """

        conn = connection.PeerConnection(
            channel,
            swarm_id,
            peer_id,
            endpoint,
            self.__config,
            self.__clock,
            self.__connections.discard,
            self.get_connections,
        )
        self.__connections.add(conn)
        conn.start()
        return conn

    async def __leave(self, swarm_id: int):
        """Leave a torrent and close all connections to it."""

        self.__leaving.add(swarm_id)
        self.__memberships.discard(swarm_id)

        # These two are cancelling PENDING handshake tasks.
        swarm_tasks = [
            task
            for task, pending_swarm in self.__pending_peer_handshakes.items()
            if pending_swarm == swarm_id
        ]
        swarm_transports_close = [
            self.__pending_tcp_handshakes[task].close()
            for task in swarm_tasks
            if task in self.__pending_tcp_handshakes
        ]
        for task in swarm_tasks:
            task.cancel()

        # This is cancelling ESTABLISHED connections.
        conn_close = [
            conn.close() for conn in self.__connections if conn.swarm_id == swarm_id
        ]
        try:
            await asyncio.gather(*swarm_tasks, return_exceptions=True)
            await asyncio.gather(*swarm_transports_close, *conn_close)
        finally:
            self.clear_swarm(swarm_id)
            self.__leaving.discard(swarm_id)
