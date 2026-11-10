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
