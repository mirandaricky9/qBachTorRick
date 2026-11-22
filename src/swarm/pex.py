import asyncio
import logging
import typing as t

from src import config
from src.models import peer
from src.swarm import peerutils

if t.TYPE_CHECKING:
    # PeerConnection imports PEXManager at runtime and owns its lifecycle.
    from src.swarm import connection

LOGGER = logging.getLogger(__name__)


class PEXManager:
    """Handle PEX (peer exchange).

    This class simply intercept peer connection and handle PEX messages only.
    It will not manage the connection's lifetime.
    """

    def __init__(
        self,
        conn: "connection.PeerConnection",
        peer_config: config.PeerConfig,
        get_connections: t.Callable[[int], list["connection.PeerConnection"]],
    ):
        """
        Construct a PEXManager.

        Parameters
        ----------
        conn : PeerConnection
            The connection between two peers.
        peer_config : PeerConfig
            Config for peer operations.
        get_connections : t.Callable[[int], list[PeerConnection]]
            A callback which, when invoked, return a list of active peer connections for a swarm.
        """

        self.__connection = conn
        self.__config = peer_config
        self.__get_connections = get_connections

        # Keep track of peers received via PEX.
        self.__pex_peers: set[peer.PeerInfo] = set()
        # Keep track of which peers were sent via PEX so we don't resend.
        self.__advertised_peers: set[peer.PeerInfo] = set()
        self.__pex_task: asyncio.Task[None] | None = None
        self.__is_stopped = False

    def start(self):
        """Start PEX for a connection."""

        if self.__pex_task is not None or self.__is_stopped:
            raise RuntimeError("PEX manager already started or stopped")
        self.__pex_task = asyncio.create_task(self.__pex_loop())
        self.__pex_task.add_done_callback(self.__task_done)

    def get_pex_peers(self) -> set[peer.PeerInfo]:
        """Return peers discovered via PEX."""

        if self.__is_stopped or not self.__connection.is_connected:
            return set()
        return set(self.__pex_peers)

    def on_pex_receive(self, message: dict[str, t.Any]):
        """Handle a PEX message.

        This will update its internal peer discoveries according to `added` and `dropped`.
        This list can be viewed via `get_pex_peers()`.
        """

        added, dropped = peerutils.parse_pex(message, self.__config.pex_max_entries)
        self.__pex_peers.update(added)
        self.__pex_peers.difference_update(dropped)

    def clear(self):
        """Clear currently stored peers."""

        self.__pex_peers.clear()

    async def stop(self):
        """Stop peer exchange.
        This will clear all internal states and stop sending and intercepting PEX.
        """

        self.__is_stopped = True
        self.__pex_peers.clear()
        self.__advertised_peers.clear()
        if self.__pex_task is not None:
            self.__pex_task.cancel()
            await asyncio.gather(self.__pex_task, return_exceptions=True)

    def __task_done(self, task):
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            LOGGER.error(
                "Peer exchange failed: %s",
                error,
                exc_info=(type(error), error, error.__traceback__),
            )

    async def __pex_loop(self):
        """Send PEX in the background at interval."""

        conn = self.__connection
        try:
            while not self.__is_stopped and conn.is_connected:
                await asyncio.sleep(self.__config.pex_interval)

                # Get all connections that don't include the one we're talking to.
                connections = self.__get_connections(conn.swarm_id)
                current_pex_peers: set[peer.PeerInfo] = set()
                for c in connections:
                    if (
                        c.peer_id != conn.peer_id
                        and c.peer_endpoint != conn.peer_endpoint
                    ):
                        current_pex_peers.add(c.peer_endpoint)

                added = sorted(
                    current_pex_peers - self.__advertised_peers,
                    key=lambda info: (info.ip, info.port),
                )[: self.__config.pex_max_entries]
                dropped = sorted(
                    self.__advertised_peers - current_pex_peers,
                    key=lambda info: (info.ip, info.port),
                )[: self.__config.pex_max_entries]

                if added or dropped:
                    await conn.send(peerutils.get_pex_msg(added, dropped))
                    self.__advertised_peers.update(added)
                    self.__advertised_peers.difference_update(dropped)
        except (OSError, EOFError, ValueError, TimeoutError) as error:
            LOGGER.debug("Peer exchange failed: %s", error)
        finally:
            self.__advertised_peers.clear()
