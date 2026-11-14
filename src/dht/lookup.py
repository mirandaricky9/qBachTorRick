import asyncio
import logging
import typing as t

from src import config, utils
from src.dht import routing_table
from src.models import node, peer
from src.network import rpc_client, transport

_CANDIDATE_LIST_LIMIT = 256
_MAX_LOOKUP_ATTEMPT = 256
LOGGER = logging.getLogger(__name__)


class LookupService:
    def __init__(
        self,
        local_id: int,
        routing: routing_table.RoutingTable,
        client: rpc_client.RPCClient,
        config: config.DHTConfig,
    ):
        self.__local_id = local_id
        self.__routing = routing
        self.__client = client
        self.__config = config

    async def find_nodes(
        self,
        transport: transport.UDPTransport,
        target_id: int,
    ):
        contacts, _ = await self.__search(transport, target_id, "FIND_NODE")
        return contacts

    async def get_peers(
        self,
        transport: transport.UDPTransport,
        swarm_id: int,
    ):
        _, peers = await self.__search(transport, swarm_id, "GET_PEERS")
        return peers

    async def __search(
        self,
        transport: transport.UDPTransport,
        target_id: int,
        method: t.Literal["FIND_NODE", "GET_PEERS"],
    ):
        utils.validate_id(target_id)
        queried_nodes: set[int] = set()
        # Keep track of nodes which response "negatively" to our request (error response, no response, etc.)
        failed_nodes: set[int] = set()
        # Keep track of who to contact for closer nodes next.
        candidates: dict[int, node.NodeInfo] = {}
        # Keep track of requests waiting for a response.
        ongoing_requests: dict[
            asyncio.Task[tuple[node.NodeInfo, dict[str, t.Any]]], int
        ] = {}
        # Keep track of which nodes responded.
        responsive: dict[int, node.NodeInfo] = {}
        # Mostly to write a warning when the search reach its limit.
        is_limited = False
        attempts = 0
        peers: dict[tuple[str, int], peer.PeerInfo] = {}
        parameter = "target_id" if method == "FIND_NODE" else "swarm_id"

        for info in self.__routing.closest(target_id, self.__config.k):
            candidates[info.node_id] = info

        try:
            while True:
                k_nearest_nodes = sorted(
                    (
                        candidate_id
                        for candidate_id in candidates
                        if candidate_id not in failed_nodes
                    ),
                    key=lambda node_id: node_id ^ target_id,
                )[: self.__config.k]

                # Send requests to those nodes.
                for node_id in k_nearest_nodes:
                    if (
                        len(ongoing_requests) >= self.__config.alpha
                        or attempts >= _MAX_LOOKUP_ATTEMPT
                    ):
                        break
                    if node_id in queried_nodes:
                        continue

                    info = candidates[node_id]
                    task = asyncio.create_task(
                        self.__client.request(
                            transport,
                            (info.ip, info.port),
                            method,
                            {parameter: utils.id_to_hex(target_id)},
                            node_id,
                        )
                    )
                    ongoing_requests[task] = node_id
                    queried_nodes.add(node_id)
                    attempts += 1

                # No need to wait for response if nothing is running.
                if not ongoing_requests:
                    is_limited = is_limited or (
                        attempts >= _MAX_LOOKUP_ATTEMPT
                        # attempt = MAX_LOOKUP_ATTEMPT happens in two cases.
                        # 1. We've already searched every eligible candidates.
                        # 2. There are still candidates, but we hit the cap.
                        # Only the second scenario yield is_limited to True.
                        and any(
                            node_id not in queried_nodes for node_id in k_nearest_nodes
                        )
                    )
                    break

                done, _ = await asyncio.wait(
                    ongoing_requests, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    node_id = ongoing_requests.pop(task)
                    try:
                        node_info, result = task.result()
                    except (
                        TimeoutError,
                        OSError,
                        ConnectionError,
                        rpc_client.RPCError,
                    ):
                        failed_nodes.add(node_id)
                        continue

                    responsive[node_id] = node_info

                    for entry in result.get("peers", []):
                        info = peer.PeerInfo(**entry)
                        # Later duplicate entry might not be correct.
                        if (info.ip, info.port) not in peers:
                            peers[(info.ip, info.port)] = info

                    for entry in result.get("nodes", []):
                        contact = node.NodeInfo(**entry)
                        if (
                            contact.node_id != self.__local_id
                            and contact.node_id not in queried_nodes
                            and contact.node_id not in candidates
                        ):
                            candidates[contact.node_id] = contact

                    # Truncate list of lookup candidates.
                    if len(candidates) > _CANDIDATE_LIST_LIMIT:
                        is_limited = True
                        # Make sure ongoing requests are preserved.
                        protected = set(ongoing_requests.values())
                        # Prioritize candidates closest to target.
                        # Also leave space for ongoing requests.
                        good_candidates = sorted(
                            candidates,
                            key=lambda node_id: node_id ^ target_id,
                        )[: _CANDIDATE_LIST_LIMIT - len(protected)]
                        keep = set(good_candidates) | protected

                        candidates = {
                            node_id: node_info
                            for node_id, node_info in candidates.items()
                            if node_id in keep
                        }
            if is_limited:
                LOGGER.warning(
                    "Lookup %s reached a candidate or request limit",
                    utils.id_to_hex(target_id),
                )
            contacts = sorted(
                responsive.values(),
                key=lambda info: info.node_id ^ target_id,
            )[: self.__config.k]
            return contacts, list(peers.values())
        finally:
            for task in ongoing_requests:
                task.cancel()
            if ongoing_requests:
                # Wait and consume all ongoing requests results/exceptions.
                await asyncio.gather(*ongoing_requests, return_exceptions=True)
