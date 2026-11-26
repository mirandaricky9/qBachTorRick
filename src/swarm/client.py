import asyncio
import logging
import pathlib
import secrets
import time
import typing as t

from src import config, utils
from src.models import peer
from src.network import transport
from src.swarm import connection, manager, peerutils
from src.torrent import metainfo, store

LOGGER = logging.getLogger(__name__)


class SwarmClient:
    """Load torrents, handle handshakes, and coordinate per-swarm managers."""

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

        self.__managers: dict[int, manager.SwarmManager] = {}

        # Keep track of peers that we discovered via DHT but not managed by a SwarmManager.
        # This happens when we have yet to make peer connection to the swarm yet.
        self.__peers: dict[int, list[peer.PeerInfo]] = {}

        # Torrents we have added to the client.
        self.__torrents: dict[int, store.TorrentRecord] = {}
        # Keep track of which swarm we're participating in.
        # - We can leave a swarm but the torrent stay added.
        # - We can participate in a swarm before a peer connection happens.
        self.__memberships: set[int] = set()

        # Track which torrents we're loading locally.
        self.__adding_torrents: dict[int, asyncio.Task] = {}
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
        self.__is_starting = False

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

        if self.__server is not None or self.__is_starting:
            raise RuntimeError("Peer listener already running")

        self.__is_starting = True
        try:
            await self.__run_task(self.__start(bind_ip))
        except BaseException:
            await self.stop()
            raise
        finally:
            self.__is_starting = False

    def get_torrents(self) -> list[metainfo.Torrent]:
        """Return all added torrent metadata."""

        return [record.torrent for record in self.__torrents.values()]

    def get_manager(self, swarm_id: int):
        """Return an existing `SwarmManager` for a swarm if exists."""

        return self.__managers.get(swarm_id)

    def join_swarm(self, swarm_id: int):
        """Locally register itself as part of the given swarm.

        This is used when you're announcing to the DHT.
        For other ways to join a swarm, use `connect_peer()`.
        """

        self.__require_running()
        utils.validate_id(swarm_id)
        if swarm_id in self.__leaving:
            raise RuntimeError(f"Leaving swarm {swarm_id}")
        if swarm_id in self.__adding_torrents:
            raise RuntimeError(f"Loading torrent {swarm_id}")

        self.__memberships.add(swarm_id)

    async def add_torrent(self, torrent_path: str | pathlib.Path) -> int:
        """Add a torrent into the client.

        This will load the metadata within the `.torrent` file.
        It will then add the torrent into the client (via `register_torrent()`).

        It will also make a copy of the file so it'll be added automatically next time.

        Parameters
        ----------
        torrent_path : str | Path
            The path to the `.torrent` file.

        Returns
        -------
        int
            The swarm ID/torrent infohash.
        """

        self.__require_running()
        return await self.__run_task(self.__add_torrent(torrent_path))

    async def register_torrent(
        self,
        torrent: metainfo.Torrent,
        data_path: str | pathlib.Path | None = None,
    ) -> int:
        """Add an already loaded torrent.

        Parameters
        ----------
        torrent : Torrent
            The torrent object.
        data_path : str | Path, optional
            The path to the torrent's payload, default to `PeerConfig.data_dir/<torrent.name>`.

        Returns
        -------
        int
            The swarm ID/torrent infohash.
        """

        self.__require_running()
        return await self.__run_task(self.__register_torrent(torrent, data_path))

    def get_local_pieces(self, swarm_id: int):
        """Return the bitmap representing piece availability for a swarm."""

        return self.__torrents[swarm_id].availability.bitmap

    async def mark_piece_verified(self, swarm_id: int, index: int):
        """Mark a piece within the bitmap as verified and announce this.
        This will send HAVE messages to all active peer connections (within the swarm).
        """

        self.__require_running()
        if swarm_id in self.__leaving:
            raise RuntimeError(f"Leaving swarm {swarm_id}")

        state = self.__torrents[swarm_id].availability
        swarm = self.__managers.get(swarm_id)
        if swarm is None:
            peerutils.parse_have(peerutils.get_have_msg(index), state.piece_count)
            state.bitmap |= 1 << index
            return
        await self.__run_task(swarm.mark_piece_verified(index))

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
        self.__get_or_create_manager(swarm_id)
        return await self.__run_task(self.__connect(swarm_id, endpoint, bind_ip))

    def get_connections(self, swarm_id: int):
        """Return active peer connections for a swarm."""

        swarm = self.__managers.get(swarm_id)
        return swarm.get_connections() if swarm is not None else []

    def update_peers(self, swarm_id: int, peers: list[peer.PeerInfo]):
        """Update peers discovered via DHT.
        Do not use this to update peers discovered via PEX.
        """

        utils.validate_id(swarm_id)
        swarm = self.__managers.get(swarm_id)
        if swarm is not None:
            swarm.update_peers(peers)
            return

        self.__peers[swarm_id] = list(dict.fromkeys(peers))

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
        swarm = self.__managers.get(swarm_id)
        if swarm is not None:
            return swarm.get_cached_peers()
        return self.__peers.get(swarm_id, [])

    # Should probably be private. Not sure if it's gonna be useful or not...
    def clear_swarm(self, swarm_id: int):
        """Clear discovered peers for a swarm in the cache.

        To leave, use `leave()`.
        """

        self.__peers.pop(swarm_id, None)
        swarm = self.__managers.get(swarm_id)
        if swarm is not None:
            swarm.clear()

    def clear(self):
        """Clear all discovered peers.

        To leave or close connections, use `leave()` or `stop()`.
        """

        self.__peers.clear()
        for swarm in self.__managers.values():
            swarm.clear()

    async def leave(self, swarm_id: int):
        """Leave a torrent.

        Notes
        -----
        This closes the manager and its connections, retaining loaded metadata.
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
        manager_close = [swarm.close() for swarm in list(self.__managers.values())]
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*transports_close, *manager_close)

        if server is not None:
            await server.wait_closed()

        self.__bg_tasks.clear()
        self.__fg_tasks.clear()
        self.__pending_peer_handshakes.clear()
        self.__pending_tcp_handshakes.clear()
        self.__managers.clear()
        self.__torrents.clear()
        self.__adding_torrents.clear()
        self.__leaving.clear()
        self.clear()

    def __require_running(self):
        if self.__server is None or self.__is_starting:
            raise RuntimeError("Peer listener is not running")

    async def __start(self, bind_ip):
        """Start the client.

        This will load any existing torrents into the client and open the peer TCP socket.
        """

        await self.__load_existing_torrents()
        self.__server = await asyncio.start_server(
            self.__accept_tcp_handshake,
            host=bind_ip,
            port=self.__config.port,
            start_serving=False,
        )
        await self.__server.start_serving()

    async def __load_existing_torrents(self):
        paths = await asyncio.to_thread(
            store.get_torrent_paths,
            self.__config.torrents_dir,
            self.__config.data_dir,
        )
        for path in paths:
            torrent = await asyncio.to_thread(metainfo.Torrent.load, path)
            await self.__register_torrent(torrent, persist=False)

    async def __add_torrent(self, torrent_path: str | pathlib.Path):
        torrent = await asyncio.to_thread(metainfo.Torrent.load, torrent_path)
        return await self.__register_torrent(torrent)

    async def __register_torrent(
        self,
        torrent: metainfo.Torrent,
        data_path: str | pathlib.Path | None = None,
        persist: bool = True,
    ):
        swarm_id = torrent.swarm_id
        if swarm_id in self.__leaving:
            raise RuntimeError(f"Leaving swarm {swarm_id}")
        if swarm_id in self.__adding_torrents:
            # Let it finish loading the torrent. Make sure to protect it from cancellation.
            await asyncio.shield(self.__adding_torrents[swarm_id])
            if swarm_id in self.__leaving:
                raise RuntimeError(f"Leaving swarm {swarm_id}")

        if swarm_id in self.__torrents:
            self.__memberships.add(swarm_id)
            return swarm_id

        if (
            swarm_id in self.__managers
            or swarm_id in self.__pending_peer_handshakes.values()
        ):
            raise RuntimeError(
                "Leave the metadata-free swarm before loading its torrent"
            )

        this_task = asyncio.current_task()
        assert this_task is not None
        self.__adding_torrents[swarm_id] = this_task
        try:
            record = await asyncio.to_thread(
                store.prepare_torrent,
                torrent,
                data_path,
                persist,
                torrents_dir=self.__config.torrents_dir,
                data_dir=self.__config.data_dir,
            )
            self.__torrents[swarm_id] = record
            self.__memberships.add(swarm_id)
            return swarm_id
        finally:
            self.__adding_torrents.pop(swarm_id, None)

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
                if swarm_id in self.__adding_torrents:
                    raise ValueError("Torrent is being added")
                if swarm_id in self.__leaving:
                    raise ValueError("Leaving this swarm")

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

        return self.__get_or_create_manager(swarm_id).create_connection(
            channel, peer_id, endpoint
        )

    def __get_or_create_manager(self, swarm_id: int) -> manager.SwarmManager:
        swarm = self.__managers.get(swarm_id)
        if swarm is not None:
            return swarm

        record = self.__torrents.get(swarm_id)
        # Temporarily not allow metadata-free manager.
        if record is None:
            raise RuntimeError(
                "Torrent needs to be added before creating the corresponding manager."
            )

        swarm = manager.SwarmManager(
            swarm_id,
            self.__config,
            self.__clock,
            record.availability if record is not None else None,
        )
        # Transfer peers for the swarm to the manager.
        swarm.update_peers(self.__peers.pop(swarm_id, []))
        self.__managers[swarm_id] = swarm
        return swarm

    async def __leave(self, swarm_id: int):
        """Leave a torrent and close all connections to it."""

        self.__leaving.add(swarm_id)
        self.__memberships.discard(swarm_id)

        swarm_tasks = [
            task
            for task, pending_swarm in self.__pending_peer_handshakes.items()
            if pending_swarm == swarm_id
        ]

        # These two are cancelling PENDING handshake tasks.
        swarm_transports_close = [
            self.__pending_tcp_handshakes[task].close()
            for task in swarm_tasks
            if task in self.__pending_tcp_handshakes
        ]
        for task in swarm_tasks:
            task.cancel()

        adding_torrent_task = self.__adding_torrents.get(swarm_id)
        if adding_torrent_task is not None:
            adding_torrent_task.cancel()
            # So we can gather below.
            swarm_tasks.append(adding_torrent_task)

        swarm = self.__managers.get(swarm_id)
        try:
            await asyncio.gather(*swarm_tasks, return_exceptions=True)
        finally:
            # This is cancelling ESTABLISHED connections.
            try:
                manager_close = [swarm.close()] if swarm is not None else []
                await asyncio.gather(*swarm_transports_close, *manager_close)
            finally:
                self.__managers.pop(swarm_id, None)
                self.clear_swarm(swarm_id)
                self.__leaving.discard(swarm_id)
