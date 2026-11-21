import asyncio
import logging
import typing as t

from src import config
from src.models import peer
from src.network import transport
from src.swarm import peerutils

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
        """

        self.__transport = channel
        self.__swarm_id = swarm_id
        self.__peer_id = peer_id
        self.__endpoint = endpoint
        self.__peer_addr = channel.peer_addr
        self.__config = config
        self.__clock = clock
        self.__on_close = on_close

        # For timeout.
        self.__last_received = clock()
        # For heartbeating.
        self.__last_sent = self.__last_received
        self.__is_closed = False
        self.__maintenance_task: asyncio.Task | None = None

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
        workers = [
            asyncio.create_task(self.__receive_loop()),
            asyncio.create_task(self.__heartbeat_loop()),
            asyncio.create_task(self.__monitor_timeout()),
        ]
        try:
            await asyncio.gather(*workers)
        except (OSError, EOFError, ValueError, TimeoutError) as error:
            LOGGER.debug("Peer connection closed: %s", error)
        finally:
            self.__is_closed = True

            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            await self.__transport_close()

    async def __send(self, message: dict[str, t.Any]):
        async with asyncio.timeout(self.__config.receive_timeout):
            await self.__transport.send(peerutils.encode(message))
            LOGGER.debug("Peer Send: %s", message)
        self.__last_sent = self.__clock()

    async def __receive_loop(self):
        """Block until a message is received, then process it."""

        while True:
            message = peerutils.decode(await self.__transport.receive())

            match message["type"]:
                case "KEEP_ALIVE":
                    pass
                case _:
                    return

            self.__last_received = self.__clock()

    async def __heartbeat_loop(self):
        """Perform heartbeating in the background."""

        while True:
            await asyncio.sleep(self.__config.maintenance_interval)
            now = self.__clock()
            if now - self.__last_sent >= self.__config.heartbeat_interval:
                await self.__send(peerutils.get_keep_alive_msg())

    async def __monitor_timeout(self):
        """Monitor for connection timeout."""

        while True:
            await asyncio.sleep(self.__config.maintenance_interval)
            if self.__clock() - self.__last_received >= self.__config.receive_timeout:
                raise TimeoutError("Peer timeout")
