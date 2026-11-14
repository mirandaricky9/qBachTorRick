import asyncio
import dataclasses
import logging
import secrets
import socket
import time
import typing as t

from src import config
from src.dht import lookup, routing_table
from src.models import node, peer
from src.network import netutils, rpc_client, rpc_server, transport
from src.swarm import store

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class SwarmAnnounceResult:
    expected_replicas_count: int
    replicas_ack_count: int
    failed_contacts: list[node.NodeInfo]


class DHTNode:
    def __init__(
        self,
        config: config.DHTConfig,
        clock: t.Callable[[], float] = time.monotonic,
    ):
        self.__config = config
        self.__clock = clock

        # This wall of None is unfortunate, but we do want init to
        # actually start in start() for fresh state.
        # The downside is a bunch of linter complaints, but the upside is
        # code down below will not complain about unknown method, which is way better.
        # Maybe there's a better way? Idk :/
        self.info: node.NodeInfo = None
        self.__rtb: routing_table.RoutingTable = None
        self.__store: store.PeerRegistrationStore = None
        self.__client: rpc_client.RPCClient = None
        self.__server: rpc_server.RPCServer = None
        self.__lookup: lookup.LookupService = None
        self.__protocol: transport.UDPTransport = None

        # Keep track of running tasks (background and foreground)
        # so we can cancel them if needed.
        self.__bg_tasks: set[asyncio.Task[None]] = set()
        self.__fg_tasks: set[asyncio.Task[t.Any]] = set()
        # Keep track of the nodes we're probing/PINGing.
        self.__probes: set[int] = set()

        # Keep track of the ID we're refreshing. A minor optimization.
        self.__refreshes: set[int] = set()
        # Keep track of background tasks that are responsible for handling requests.
        self.__request_handlers: set[asyncio.Task[t.Any]] = set()

        self.__running = False

    def __spawn_task(self, coroutine):
        """Spawn and return an async task to run the coroutine.

        This is meant to run background tasks (dispatch and don't really care about the return result).
        If you want to run foreground tasks (dispatch and wait for result), use `DHTNode.__run_task()` instead.
        """

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
        If you want to run background tasks (dispatch and don't really care about return result), use `DHTNode.__spawn_task()` instead.
        """

        task = asyncio.create_task(coroutine)
        self.__fg_tasks.add(task)
        try:
            return await task
        finally:
            self.__fg_tasks.discard(task)

    def get_routing_info(self) -> list[str]:
        """Return bucket descriptions (for debugging)."""
        
        self.__require_running()
        return [str(bucket) for bucket in self.__rtb.buckets]

    async def start(self) -> None:
        """Open the DHT UDP socket and setup internal state.

        This must be called before using any other DHT operations.
        """
        
        if self.__running:
            raise RuntimeError("Node is already running")

        node_id = secrets.randbits(256)
        self.info = node.NodeInfo(
            node_id,
            self.__config.advertise_ip,
            self.__config.port,
        )
        self.__rtb = routing_table.RoutingTable(
            node_id,
            self.__config.k,
            self.__clock,
            self.__config.failure_limit,
            self.__config.idle_threshold,
        )
        self.__store = store.PeerRegistrationStore(self.__config.registration_ttl, self.__clock)

        try:
            loop = asyncio.get_running_loop()
            _, self.__protocol = await loop.create_datagram_endpoint(
                lambda: transport.UDPTransport(self.__receive_req),
                local_addr=(self.__config.bind_ip, self.__config.port),
                family=socket.AF_INET,
            )
            self.__client = rpc_client.RPCClient(
                node_id,
                self.__config,
                self.__on_valid_node_communication,
                self.__on_message_failure,
            )
            self.__server = rpc_server.RPCServer(
                node_id,
                self.__rtb,
                self.__store,
                self.__config,
                self.__on_valid_node_communication,
            )
            self.__lookup = lookup.LookupService(
                node_id,
                self.__rtb,
                self.__client,
                self.__config,
            )
            self.__running = True
            self.__spawn_task(self.__maintenance())
        except BaseException:
            await self.stop()
            raise

    async def join(self, bootstrap_address: tuple[str | t.Any, int]) -> None:
        """Join the DHT network using a known DHT node's IP address and port.

        Parameters
        ----------
        bootstrap_address : tuple[str | t.Any, int]
            Tuple of a valid IPv4 address and port number.
        """

        self.__require_running()
        address = (str(bootstrap_address[0]), bootstrap_address[1])
        node.NodeInfo(0, *address)
        # Wrap the join into one task so we can cancel if needed.
        await self.__run_task(self.__join(address))

    async def find_nodes(self, target_id: int) -> list[node.NodeInfo]:
        self.__require_running()
        return await self.__run_task(
            self.__lookup.find_nodes(
                self.__protocol,
                target_id,
            )
        )

    async def announce(self, swarm_id: int, peer_port: int) -> SwarmAnnounceResult:
        self.__require_running()
        return await self.__run_task(self.__announce(swarm_id, peer_port))

    async def get_peers(self, swarm_id: int) -> list[peer.PeerInfo]:
        self.__require_running()
        return await self.__run_task(self.__get_peers(swarm_id))

    async def stop(self):
        self.__running = False
        tasks = list(self.__bg_tasks | self.__fg_tasks)
        for task in tasks:
            task.cancel()
        if self.__client is not None:
            self.__client.stop()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self.__protocol is not None:
            await self.__protocol.close()
            self.__protocol = None
        self.__bg_tasks.clear()
        self.__fg_tasks.clear()
        self.__probes.clear()
        self.__refreshes.clear()
        self.__request_handlers.clear()

    def __require_running(self):
        if not self.__running:
            raise RuntimeError("Node is not running. Start the node first.")

    def __receive_req(self, data: bytes, address: tuple):
        """A callback that intercept incoming messages.
        This should be passed into `UDPTransport`.

        If the message is a response or error, it is forwarded to `RPCClient.receive()`.
        Otherwise (a request), it spawns a `RPCServer.response()` to handle.
        """

        if not self.__running:
            # Node may be starting or stopping, so raising error is not appropriate.
            return

        try:
            message = netutils.decode(data)
            if message["type"] != "request":
                self.__client.receive(message, address)
            elif len(self.__request_handlers) < 64:
                task = self.__spawn_task(self.__handle_req(message, address))
                self.__request_handlers.add(task)
                task.add_done_callback(self.__request_handlers.discard)
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
            LOGGER.debug("Dropped invalid datagram from %s", address)

    async def __handle_req(self, message: dict[str, t.Any], address: tuple[str, int]):
        """Handle a request asynchronously."""

        self.__server.response(self.__protocol, message, address)

    def __on_valid_node_communication(self, info: node.NodeInfo):
        """A callback triggered when this node receives a valid communication from `info.node_id`.
        This is used to refresh the node's liveness.

        Notes
        -----
        This may schedule a probe as part of node liveness checking.
        The probe may target a different node than `info.node_id`. See `RoutingTable.add()` for more info.
        """

        probe = self.__rtb.add(info)
        if probe is not None:
            self.__schedule_probe(probe)

    def __on_message_failure(self, node_id: int):
        """A callback triggered when this node fails to deliver a message to `node_id`.

        Notes
        -----
        This may schedule a probe as part of node liveness checking.
        The probe may target a different node than `node_id`. See `RoutingTable.report_failure()` for more info.
        """

        probe = self.__rtb.report_failure(node_id)
        if probe is not None:
            self.__schedule_probe(probe)

    def __schedule_probe(self, info: node.NodeInfo):
        """Schedule a probe (PING) to a node."""

        if not self.__running or info.node_id in self.__probes:
            return

        if self.__client.is_busy(info.node_id):
            # There's an ongoing request for this node already,
            # so when there's a response, we can use that as liveness.
            # Therefore, don't probe.
            bucket = self.__rtb.bucket_for(info.node_id)
            if bucket.probe_id == info.node_id:
                bucket.probe_id = None
            return

        bucket = self.__rtb.bucket_for(info.node_id)
        # Enforce one probe per bucket.
        # If the bucket already wants to probe someone else, we cancel our attempt.
        if bucket.probe_id is not None and bucket.probe_id != info.node_id:
            return

        bucket.probe_id = info.node_id
        self.__probes.add(info.node_id)
        # print(f"{self.info.node_id} is probing {info.node_id}...")
        self.__spawn_task(self.__probe(info))

    async def __probe(self, info: node.NodeInfo):
        """Send a PING to the node asynchronously."""

        try:
            if not self.__client.is_busy(info.node_id):
                await self.__client.request(
                    self.__protocol,
                    (info.ip, info.port),
                    "PING",
                    {},
                    info.node_id,
                )
        except (TimeoutError, OSError, ConnectionError, rpc_client.RPCError):
            pass
        finally:
            self.__probes.discard(info.node_id)
            self.__rtb.finish_probe(info.node_id)

            # It could be that the PING happens in the middle of a backup promotion,
            # so we want to resume it.
            next_probe = self.__rtb.promote_backup(self.__rtb.bucket_for(info.node_id))
            if next_probe is not None:
                self.__schedule_probe(next_probe)

    async def __join(self, address):
        await self.__client.request(self.__protocol, address, "PING", {})
        # The responses from FIND_NODE is to populate the rtb
        # so we don't need the return result.
        await self.__lookup.find_nodes(self.__protocol, self.info.node_id)

    async def __get_peers(self, swarm_id: int) -> list[peer.PeerInfo]:
        node.validate_id(swarm_id)
        remote_peers = await self.__lookup.get_peers(self.__protocol, swarm_id)
        merged = {}
        for info in self.__store.get(swarm_id) + remote_peers:
            if (info.ip, info.port) not in merged:
                merged[(info.ip, info.port)] = info
        return list(merged.values())

    async def __announce(self, swarm_id: int, peer_port: int) -> SwarmAnnounceResult:
        node.validate_id(swarm_id)

        replica_candidates = await self.__lookup.find_nodes(self.__protocol, swarm_id)
        replica_candidates.append(self.info)
        chosen_replicas = sorted(
            replica_candidates,
            key=lambda info: info.node_id ^ swarm_id,
        )[: self.__config.replicas]

        announce_ack_count = 0
        if self.info in chosen_replicas:
            self.__store.put(swarm_id, peer.PeerInfo(self.info.ip, peer_port))
            announce_ack_count += 1
        remote_replicas = [
            info for info in chosen_replicas if info.node_id != self.info.node_id
        ]
        results = await asyncio.gather(
            *(
                self.__send_announce(info, swarm_id, peer_port)
                for info in remote_replicas
            )
        )

        failed = []
        for info, result in zip(remote_replicas, results):
            if result is None:
                failed.append(info)
        announce_ack_count += len(remote_replicas) - len(failed)

        return SwarmAnnounceResult(
            self.__config.replicas,
            announce_ack_count,
            failed,
        )

    async def __send_announce(self, info: node.NodeInfo, swarm_id: int, peer_port: int):
        try:
            _, result = await self.__client.request(
                self.__protocol,
                (info.ip, info.port),
                "ANNOUNCE",
                {"swarm_id": node.id_to_hex(swarm_id), "port": peer_port},
                info.node_id,
            )
            return result
        except (TimeoutError, OSError, ConnectionError, rpc_client.RPCError):
            return None

    async def __refresh(self, target_id: int):
        """Refresh asynchronously using `target_id`.

        See `RoutingTable` for more info.
        """

        self.__refreshes.add(target_id)
        try:
            await self.__lookup.find_nodes(self.__protocol, target_id)
        finally:
            self.__refreshes.discard(target_id)

    async def __maintenance(self):
        """Run maintenance asynchronously.

        Maintenance include:
            - Remove expired swarm peers.
            - Fix routing table (probe if needed).

        Notes
        -----
        This should be run in the background via `__spawn_task()`.
        """

        while self.__running:
            await asyncio.sleep(self.__config.maintenance_interval)
            try:
                self.__store.remove_expired()

                now = self.__clock()
                # Bucket maintenance.
                for bucket in self.__rtb.buckets:
                    if bucket.probe_id is not None:
                        continue

                    if not bucket.is_full() and bucket.backup_nodes:
                        probe = self.__rtb.promote_backup(bucket)
                        if probe is not None:
                            self.__schedule_probe(probe)
                            continue

                    if bucket.is_full() and bucket.backup_nodes:
                        oldest_node = next(iter(bucket.active_nodes.values()))
                        if (
                            now - oldest_node.last_seen
                            >= self.__config.maintenance_interval
                        ):
                            self.__schedule_probe(oldest_node.info)
                    else:
                        idle_node = next(
                            (
                                record
                                for record in bucket.active_nodes.values()
                                if now - record.last_seen
                                >= self.__config.idle_threshold
                            ),
                            None,
                        )
                        if idle_node is not None:
                            self.__schedule_probe(idle_node.info)

                # Bucket refreshing.
                for target in self.__rtb.get_refresh_targets(
                    self.__config.refresh_interval
                ):
                    if target not in self.__refreshes:
                        self.__spawn_task(self.__refresh(target))

            except Exception:
                LOGGER.exception("Maintenance scan failed")
