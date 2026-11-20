import asyncio
import logging
import typing as t

LOGGER = logging.getLogger(__name__)


class UDPTransport(asyncio.DatagramProtocol):
    """The underlying UDP communication.
    Think of this as an equivalent to UDP "socket" but more convenient to use.
    """

    def __init__(self, on_receive: t.Callable[[bytes, tuple[str | t.Any, int]], t.Any]):
        """Construct a UDP "socket".

        Parameters
        ----------
        on_receive : t.Callable[[bytes, tuple[str | t.Any, int]], t.Any]
            A callback to handle incoming messages. Return results are ignored.
        """

        self.__on_receive = on_receive
        self.__transport: asyncio.DatagramTransport | None = None

        # Used to make sure self.__transport is fully closed.
        self.__closed = asyncio.get_running_loop().create_future()

    def connection_made(self, transport):
        self.__transport = transport

    def datagram_received(self, data, addr):
        self.__on_receive(data, addr)

    def error_received(self, exc):
        LOGGER.debug("UDP socket error: %s", exc)

    def connection_lost(self, exc):
        if not self.__closed.done():
            self.__closed.set_result(None)

    def send(self, data: bytes, address: tuple):
        """Send a message over UDP."""

        if self.__transport is None or self.__transport.is_closing():
            raise ConnectionError("UDP transport is closed")
        self.__transport.sendto(data, address)

    async def close(self):
        """Close the UDP transport."""

        if self.__transport is not None:
            # This initiate the closing, so we need to wait for it to be done.
            self.__transport.close()
            await self.__closed


class TCPTransport:
    """The underlying TCP stream.
    Think of this as an equivalent to TCP "socket" but more convenient to use.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Wrap an existing TCP stream.

        This is used when TCP connection is made from external source (like when accepting an incoming handshake).
        If you are the one initiating the connection, see `TCPTransport.connect()`.
        """

        self.__reader = reader
        self.__writer = writer
        self.__write_lock = asyncio.Lock()

        # The address of the other end of the connection.
        self.__address: tuple[str, int] = writer.get_extra_info("peername")

    @classmethod
    async def connect(cls, address: tuple[str, int]) -> t.Self:
        """Create a TCP connection to the given address.

        This is used when you are the one initiating the connection. If the connection is made externally,
        see the default constructor.
        """

        reader, writer = await asyncio.open_connection(*address)
        return cls(reader, writer)

    @property
    def peer_addr(self):
        return self.__address

    def is_closing(self) -> bool:
        return self.__writer.is_closing()

    async def receive(self) -> bytes:
        """Read a message.
        This method will suspend until there is an incoming message.
        """

        size = int.from_bytes(await self.__reader.readexactly(4), "big")
        return await self.__reader.readexactly(size)

    async def send(self, data: bytes):
        """Send a message over TCP."""

        async with self.__write_lock:
            if self.is_closing():
                raise ConnectionError("TCP is already closing")

            self.__writer.write(len(data).to_bytes(4, "big") + data)
            await self.__writer.drain()

    async def close(self):
        """Close the socket and wait for cleanup."""

        self.__writer.close()
        try:
            async with asyncio.timeout(1):
                await self.__writer.wait_closed()
        except TimeoutError:
            self.__writer.transport.abort()
        except OSError:
            pass
