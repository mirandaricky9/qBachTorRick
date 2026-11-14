import asyncio
import logging
import time
import typing as t

from src import config, utils
from src.dht import dhtnode
from src.models import node, peer
from src.swarm import client

LOGGER = logging.getLogger(__name__)


class BitTorrentNode:
    """Represent a BitTorrent node, handling DHT and swarm operations."""

    def __init__(
        self,
        config: config.DHTConfig,
        peer_port: int,
        clock: t.Callable[[], float] = time.monotonic,
    ):
        """Construct a BitTorrent node to coordinate DHT and swarm.

        Parameters
        ----------
        config : config.DHTConfig
            Config for BitTorrent.
        peer_port : int
            The port for peer operations such as peer exchange, piece exchange, etc.
        clock : t.Callable[[], float]
            The clock to keep track of time.
        """

        self.__config = config
        self.__clock = clock
        self.__swarm = client.SwarmClient(peer_port)

        # DHT is required in this project, but it's technically not required
        # in BitTorrent (cuz trackers) so we represent that decoupling like this.
        self.__dht: dhtnode.DHTNode | None = None

        self.__is_running = False
        self.__is_stopping = False

        # Keep track of ongoing tasks so we can cancel them if needed.
        self.__tasks: set[asyncio.Task] = set()

        # Keep track of which swarms we're in and how long since we last announce.
        # This is here instead of SwarmClient because we use it to announce, which involve DHT.
        self.__swarm_memberships: dict[int, float] = {}
        # Keep track of ongoing announce_peer and get_peers.
        # We also store the task since if we leave a swarm,
        # we need to cancel these tasks.
        self.__ongoing_announcement: dict[
            int, asyncio.Task[dhtnode.SwarmAnnounceResult]
        ] = {}
        self.__ongoing_get_peers: dict[int, asyncio.Task[list[peer.PeerInfo]]] = {}

        self.__ongoing_leave: set[int] = set()

    @property
    def swarm(self) -> client.SwarmClient:
        return self.__swarm

    @property
    def info(self) -> node.NodeInfo:
        return self.__require_dht().info

    def get_routing_info(self) -> list[str]:
        """Return a routing snapshot for display."""
        return self.__require_dht().get_routing_info()

    async def start(self):
        """Initialize local state."""

        if self.__is_running or self.__is_stopping:
            raise RuntimeError("BitTorrent node is already running or stopping")

        self.__swarm.clear()
        self.__is_running = True
        self.__spawn_task(self.__maintenance())

    def __require_running(self):
        if not self.__is_running:
            raise RuntimeError("BitTorrent node is not running.")

    def __require_dht(self) -> dhtnode.DHTNode:
        self.__require_running()
        if self.__dht is None:
            raise RuntimeError(
                "DHT is not available. Did you forget to call 'join_dht()'?"
            )
        return self.__dht

    def __require_swarm_not_leaving(self, swarm_id: int):
        utils.validate_id(swarm_id)
        if swarm_id in self.__ongoing_leave:
            raise RuntimeError(f"Node is leaving swarm '{swarm_id}'")

    def __spawn_task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.__tasks.add(task)
        task.add_done_callback(self.__tasks.discard)
        return task

    async def join_dht(self, address: tuple[str, int] | None = None):
        """Create a DHT node.
        If `address` is passed, also join the DHT network under another DHT node.
        """

        self.__require_running()
        await self.__spawn_task(self.__join_dht(address))

    async def find_nodes(self, target_id: int):
        """Perform `find_node` on a node ID.

        Returns
        -------
        list[NodeInfo]
            A list of nodes closest to the given ID. At most `DHTConfig.k` nodes.
        """

        dht = self.__require_dht()
        return await self.__spawn_task(dht.find_nodes(target_id))

    async def get_peers(self, swarm_id: int) -> list[peer.PeerInfo]:
        """Perform `get_peers` on a swarm ID.

        Returns
        -------
        list[PeerInfo]
            A list of peers for the swarm. This is not a complete list of peers.
        """

        self.__require_dht()
        self.__require_swarm_not_leaving(swarm_id)
        return await self.__start_get_peers(swarm_id)

    async def announce(self, swarm_id: int) -> dhtnode.SwarmAnnounceResult:
        """Announce a torrent infohash to nodes (replicas) closest to its infohash.

        The number of nodes to announce to is in `DHTConfig.replicas`.

        Returns
        -------
        SwarmAnnounceResult
            Contain the expected number of replicas, the actual number, and list of failed nodes.
        """

        self.__require_dht()
        self.__require_swarm_not_leaving(swarm_id)
        return await self.__start_announcement(swarm_id)

    async def leave(self, swarm_id: int):
        """Quietly leave a swarm.

        This is done by not reannouncing its membership anymore.
        """

        self.__require_running()
        self.__require_swarm_not_leaving(swarm_id)
        await self.__spawn_task(self.__leave(swarm_id))

    async def stop(self):
        """Stop BitTorrent operations and perform cleanup."""

        self.__is_running = False
        self.__is_stopping = True
        try:
            for task in self.__tasks:
                task.cancel()
            await asyncio.gather(*self.__tasks, return_exceptions=True)
            self.__tasks.clear()
            self.__swarm_memberships.clear()
            self.__ongoing_announcement.clear()
            self.__ongoing_get_peers.clear()
            self.__ongoing_leave.clear()
            self.__swarm.clear()
            if self.__dht is not None:
                await self.__dht.stop()
                self.__dht = None
        finally:
            self.__is_stopping = False

    async def __join_dht(self, address: tuple[str, int] | None):
        if self.__dht is not None:
            if address is not None:
                await self.__dht.join(address)
            return

        dht = dhtnode.DHTNode(self.__config, self.__clock)
        try:
            await dht.start()
            if address is not None:
                await dht.join(address)
        except BaseException:
            # It's possible for dht.start() to get cancelled,
            # so we need to get this to cleanup.
            await dht.stop()
            raise

        self.__dht = dht

    def __start_get_peers(self, swarm_id: int):
        task = self.__ongoing_get_peers.get(swarm_id)
        if task is None:
            task = self.__spawn_task(self.__get_peers(swarm_id))
            self.__ongoing_get_peers[swarm_id] = task
            task.add_done_callback(
                lambda done: self.__ongoing_get_peers.pop(swarm_id, None)
            )
        return task

    async def __get_peers(self, swarm_id: int) -> list[peer.PeerInfo]:
        peers = await self.__require_dht().get_peers(swarm_id)
        self.__swarm.update_peers(swarm_id, peers)
        return self.__swarm.get_peers(swarm_id)

    def __start_announcement(self, swarm_id: int):
        task = self.__ongoing_announcement.get(swarm_id)
        if task is None:
            task = self.__spawn_task(self.__announce(swarm_id))
            self.__ongoing_announcement[swarm_id] = task
            task.add_done_callback(
                lambda done: self.__ongoing_announcement.pop(swarm_id, None)
            )
        return task

    async def __announce(self, swarm_id: int) -> dhtnode.SwarmAnnounceResult:
        try:
            result = await self.__require_dht().announce(
                swarm_id, self.__swarm.peer_port
            )
            if result.replicas_ack_count > 0:
                self.__swarm_memberships[swarm_id] = self.__clock()
            return result
        finally:
            # Initial failures do not create membership. Established memberships
            # survive failed refreshes, timed from completion of the last attempt.
            if swarm_id in self.__swarm_memberships:
                self.__swarm_memberships[swarm_id] = self.__clock()

    async def __leave(self, swarm_id: int):
        self.__ongoing_leave.add(swarm_id)
        self.__swarm_memberships.pop(swarm_id, None)
        tasks = []
        for operations in (self.__ongoing_announcement, self.__ongoing_get_peers):
            task = operations.get(swarm_id)
            if task is not None:
                task.cancel()
                tasks.append(task)
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            self.__swarm.clear_swarm(swarm_id)
            self.__ongoing_leave.discard(swarm_id)

    def __on_swarm_reannounce_done(self, task):
        if task.cancelled():
            return
        try:
            result = task.result()
            if result.replicas_ack_count < result.expected_replicas_count:
                LOGGER.info(
                    "Swarm refresh acknowledged by %d/%d requested replicas",
                    result.replicas_ack_count,
                    result.expected_replicas_count,
                )
        except (ValueError, OSError, RuntimeError):
            LOGGER.exception("Swarm refresh failed")

    async def __maintenance(self):
        while True:
            await asyncio.sleep(self.__config.maintenance_interval)
            now = self.__clock()

            # Swarm reannouncement.
            for swarm_id, last_attempt in self.__swarm_memberships.items():
                if (
                    now - last_attempt >= self.__config.reannounce_interval
                    and swarm_id not in self.__ongoing_announcement
                    and swarm_id not in self.__ongoing_leave
                ):
                    task = self.__start_announcement(swarm_id)
                    task.add_done_callback(self.__on_swarm_reannounce_done)
