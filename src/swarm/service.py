import asyncio
import dataclasses
import time
import typing as t

from src.dht import lookup, routing_table
from src.models import node
from src.network import rpc_client, transport
from src.swarm import store


@dataclasses.dataclass(frozen=True)
class AnnounceResult:
    expected_replicas_count: int
    replicas_ack_count: int
    failed_contacts: list[node.NodeInfo]


class SwarmService:
    def __init__(
        self,
        local_info: node.NodeInfo,
        lookup: lookup.LookupService,
        client: rpc_client.RPCClient,
        store: store.SwarmStore,
        rtb: routing_table.RoutingTable,
        clock: t.Callable[[], float] = time.monotonic,
    ):
        self.__local_info = local_info
        self.__lookup = lookup
        self.__client = client
        self.__store = store
        self.__rtb = rtb
        self.__clock = clock
        self.swarm_last_announced = {}

    async def __replicas(
        self,
        transport: transport.UDPTransport,
        swarm_id: int,
        replicas_count: int,
    ):
        """Get nodes closest to `swarm_id` to announce swarm membership to.

        The returned list may also contain this node.
        """

        node.validate_id(swarm_id)

        contacts = await self.__lookup.find_nodes(transport, swarm_id)
        contacts = {info.node_id: info for info in contacts}
        contacts[self.__local_info.node_id] = self.__local_info
        return sorted(
            contacts.values(),
            key=lambda info: info.node_id ^ swarm_id,
        )[:replicas_count]

    async def __send_announce(
        self,
        transport: transport.UDPTransport,
        info: node.NodeInfo,
        swarm_id: int,
    ):
        try:
            _, result = await self.__client.request(
                transport,
                (info.ip, info.port),
                "ANNOUNCE",
                {"swarm_id": node.id_to_hex(swarm_id)},
                info.node_id,
            )
            return result
        except (TimeoutError, OSError, ConnectionError, rpc_client.RPCError):
            return None

    async def announce(
        self,
        transport: transport.UDPTransport,
        swarm_id: int,
        replicas_count: int,
    ):
        node.validate_id(swarm_id)

        # self.swarm_last_announced[swarm_id] = self.__clock()
        replicas = await self.__replicas(transport, swarm_id, replicas_count)
        announce_ack_count = 0
        if self.__local_info in replicas:
            self.__store.put(swarm_id, self.__local_info)
            announce_ack_count += 1

        remote_replicas = [
            info for info in replicas if info.node_id != self.__local_info.node_id
        ]
        results = await asyncio.gather(
            *(
                self.__send_announce(transport, info, swarm_id)
                for info in remote_replicas
            )
        )

        failed: list[node.NodeInfo] = []
        for info, result in zip(remote_replicas, results):
            if result is None:
                failed.append(info)
        announce_ack_count += len(remote_replicas) - len(failed)

        self.swarm_last_announced[swarm_id] = self.__clock()
        return AnnounceResult(
            replicas_count,
            announce_ack_count,
            failed,
        )

    async def get_peers(self, transport: transport.UDPTransport, swarm_id: int):
        node.validate_id(swarm_id)

        remote_peers = await self.__lookup.get_peers(transport, swarm_id)
        merged: dict[int, node.NodeInfo] = {}
        for peer in self.__store.get(swarm_id) + remote_peers:
            direct = self.__rtb.get(peer.node_id)
            if peer.node_id == self.__local_info.node_id:
                merged[peer.node_id] = self.__local_info
            elif direct is not None:
                merged[peer.node_id] = direct.info
            elif peer.node_id not in merged:
                merged[peer.node_id] = peer
        return list(merged.values())
