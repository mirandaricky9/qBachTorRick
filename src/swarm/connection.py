import asyncio
import logging
import typing as t

from src import config
from src.models import peer
from src.network import transport
from src.swarm import peerutils, pex

LOGGER = logging.getLogger(__name__)


class PeerConnection:
    """An established peer connection.
    This is created after peer handshake is a success.

    It will own and manage the lifecycle of the underlying TCP connection.
    This includes sending keep-alive message and cleaning up upon closing.
    """

    def __init__(
        self,
        channel: transport.TCPTransport,
        swarm_id: int,
        peer_id: int,
        endpoint: peer.PeerInfo,
        config: config.PeerConfig,
        clock: t.Callable[[], float],
        on_close: t.Callable[[t.Self], None],
        # This one has to be the class string cuz pyright isn't happy
        # with t.Self ig :/ this is such a non-issue for python 3.14+
        get_connections: t.Callable[[], list["PeerConnection"]],
        piece_count: int | None = None,
        initial_local_pieces: int = 0,
    ):
        """Construct a peer connection given an existing TCP connection.

        This will own and manage the lifecycle of the given TCP connection.

        Parameters
        ----------
        channel : TCPTransport
            The TCP connection used to communicate with the peer.
        swarm_id : int
            The torrent infohash of this connection.
        peer_id : int
            The peer we're talking to.
        endpoint : PeerInfo
            The peer's endpoint.
        config : PeerConfig
            Settings.
        clock : t.Callable[[], float]
            A clock function that returns `float`.
        on_close : t.Callable[[PeerConnection], None]
            A callback to run when the connection is closed.
            Suitable for cleaning up bookkeeping.
        get_connections : t.Callable[[], list[PeerConnection]]
            A callback returning active peer connections in this connection's swarm.
            This is used for peer exchange.
        piece_count : int | None
            How many pieces for this torrent.
        initial_local_pieces : int
            The local bitmap when this connection is created.
            This is used for `BITFIELD` message.
        """

        self.__transport = channel
        self.__swarm_id = swarm_id
        self.__peer_id = peer_id
        self.__endpoint = endpoint
        self.__peer_addr = channel.peer_addr
        self.__config = config
        self.__clock = clock
        self.__on_close = on_close

        self.__piece_count = piece_count
        # This is gonna change later but for now keep it like this.
        self.__initial_local_pieces = initial_local_pieces
        self.__remote_pieces = 0 if piece_count is not None else None

        # Slightly misleading name, but otherwise the name would be long.
        # Did we attempt to send a BITFIELD.
        self.__is_bitfield_sent = False
        # Lock cuz is_bitfield_sent is critical region.
        self.__send_bitfield_lock = asyncio.Lock()

        # For timeout.
        self.__last_received = clock()
        # For heartbeating.
        self.__last_sent = self.__last_received
        self.__is_closed = False
        self.__maintenance_task: asyncio.Task | None = None
        self.__pex_manager = pex.PEXManager(self, config, get_connections)

        # We don't have any pieces, so we're not going to send bitfield.
        if self.__initial_local_pieces == 0:
            self.__is_bitfield_sent = True

    @property
    def swarm_id(self) -> int:
        return self.__swarm_id

    @property
    def peer_id(self) -> int:
        return self.__peer_id

    @property
    def peer_endpoint(self):
        return self.__endpoint

    @property
    def peer_addr(self):
        return self.__peer_addr

    @property
    def is_connected(self):
        return not self.__is_closed and not self.__transport.is_closing()

    def start(self):
        """Start the connection.

        This will run background maintenance to keep the connection alive.
        """

        if self.__maintenance_task is not None or self.__is_closed:
            raise RuntimeError("Peer connection already started or closed")
        self.__maintenance_task = asyncio.create_task(self.__run())

    def get_pex_peers(self) -> set[peer.PeerInfo]:
        """Return peers discovered through this connection's PEX manager."""

        return self.__pex_manager.get_pex_peers()

    def clear_pex_peers(self):
        """Clear PEX discoveries while keeping the connection running."""

        self.__pex_manager.clear()

    async def send(self, message: dict[str, t.Any]):
        """Send a peer message.

        Notes
        -----
        This method will check if the BITFIELD message has been attempted.
        If it didn't, the message will be sent after the BITFIELD is sent.
        """

        async with self.__send_bitfield_lock:
            if not self.__is_bitfield_sent:
                await self.__send_initial_bitfield()

        await self.__send_message(message)

    async def __send_initial_bitfield(self):
        """Attempt to send a `BITFIELD` message.
        Depending on the local state, it may or may not send one.

        `BITFIELD` is typically sent as the first message after a peer handshake.
        It is not sent if the peer has no available pieces (all 0s in the bitmap).

        """

        if (
            self.__piece_count is None
            or self.__is_bitfield_sent
            or self.__initial_local_pieces == 0
        ):
            return
        await self.__send_message(
            peerutils.get_bitfield_msg(self.__initial_local_pieces)
        )
        self.__is_bitfield_sent = True

    async def __send_message(self, message: dict[str, t.Any]):
        async with asyncio.timeout(self.__config.receive_timeout):
            await self.__transport.send(peerutils.encode(message))
            LOGGER.debug("Peer Send: %s", message)
        self.__last_sent = self.__clock()

    async def close(self, close_transport: bool = True):
        """Unwrap the abstraction and close the underlying TCP transport (by default).

        This will terminate background maintenance tasks (such as keep-alive, timeout monitoring, etc.)

        Parameters
        ----------
        close_transport : bool, optional
            Should this also close the underlying TCP connection. Default to True.
        """

        was_closed = self.__is_closed
        self.__is_closed = True
        await self.__pex_manager.stop()

        # We have not started all the fancy monitoring.
        # We just need to close the TCP connection.
        if self.__maintenance_task is None:
            if close_transport:
                await self.__transport_close()
            return

        # Prevent overlapping close() to occur.
        # Also, when __run() exit (due to exception), it'll also set is_closed to True.
        # So this prevent a close() called as that cleanup happens.
        if not was_closed:
            self.__maintenance_task.cancel()

        try:
            await asyncio.gather(self.__maintenance_task, return_exceptions=True)
        finally:
            if close_transport:
                await self.__transport_close()

    async def __transport_close(self):
        """Close the transport and make sure it invoke `__on_close` (passed via constructor)."""

        try:
            await self.__transport.close()
        finally:
            self.__on_close(self)

    async def __run(self):
        workers: list[asyncio.Task[t.Any]] = []
        try:
            async with self.__send_bitfield_lock:
                await self.__send_initial_bitfield()
            self.__pex_manager.start()
            workers = [
                asyncio.create_task(self.__receive_loop()),
                asyncio.create_task(self.__heartbeat_loop()),
                asyncio.create_task(self.__monitor_timeout()),
            ]
            await asyncio.gather(*workers)
        except (OSError, EOFError, ValueError, TypeError, TimeoutError) as error:
            LOGGER.debug("Peer connection closed: %s", error)
        finally:
            self.__is_closed = True

            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            await self.__pex_manager.stop()
            await self.__transport_close()

    async def __receive_loop(self):
        """Block until a message is received, then process it."""

        is_first_message = True
        while True:
            message = peerutils.decode(await self.__transport.receive())

            match message["type"]:
                case "KEEP_ALIVE":
                    pass
                case "PEX":
                    self.__pex_manager.on_pex_receive(message)
                case "BITFIELD":
                    self._handle_bitfield_msg(message, is_first_message)
                case "HAVE":
                    self._handle_have_msg(message)
                case _:
                    return

            is_first_message = False
            self.__last_received = self.__clock()

    async def __heartbeat_loop(self):
        """Perform heartbeating in the background."""

        while True:
            await asyncio.sleep(self.__config.maintenance_interval)
            now = self.__clock()
            if now - self.__last_sent >= self.__config.heartbeat_interval:
                await self.send(peerutils.get_keep_alive_msg())

    async def __monitor_timeout(self):
        """Monitor for connection timeout."""

        while True:
            await asyncio.sleep(self.__config.maintenance_interval)
            if self.__clock() - self.__last_received >= self.__config.receive_timeout:
                raise TimeoutError("Peer timeout")

    # TODO: Having to pass is_first_msg is ugly, is there a better way?
    def _handle_bitfield_msg(self, message: dict[str, t.Any], is_first_msg: bool):
        if self.__piece_count is None:
            raise ValueError("BITFIELD requires registered torrent metadata")
        if not is_first_msg:
            raise ValueError("BITFIELD must be the first peer message")
        self.__remote_pieces = peerutils.parse_bitfield(message, self.__piece_count)

    def _handle_have_msg(self, message: dict[str, t.Any]):
        if self.__piece_count is None:
            raise ValueError("HAVE requires registered torrent metadata")
        index = peerutils.parse_have(message, self.__piece_count)
        assert self.__remote_pieces is not None
        self.__remote_pieces |= 1 << index
