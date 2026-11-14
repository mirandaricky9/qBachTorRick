import dataclasses
import math
import random
import typing as t

from src import config
from src.dht import routing_table
from src.models import node, peer
from src.network import netutils, transport
from src.swarm import store


@dataclasses.dataclass
class RPCRequestContext:
    sender: node.NodeInfo
    params: dict[str, t.Any] | None


class RPCServer:
    def __init__(
        self,
        local_id: int,
        rtb: routing_table.RoutingTable,
        store: store.PeerRegistrationStore,
        config: config.DHTConfig,
        on_request: t.Callable[[node.NodeInfo], t.Any],
    ):
        """Construct an RPC server instance.

        Parameters
        ----------
        on_request : t.Callable[[NodeInfo], t.Any]
            A callback triggered when there's a request.
            This should be used to record its activity (like adding to routing table).
        """

        self.__local_id = local_id
        self.__rtb = rtb
        self.__store = store
        self.__config = config
        self.__on_request = on_request

        # NOTE: This is probably better to set at the DHTNode level.
        # Something like a decorator @RPCServer.on_get_peers.
        # In which case, it would not be necessary to pass rtb and store
        # to RPCServer, but it involve some decorator stuff that is complex.
        self.__METHOD_HANDLERS: dict[str, t.Callable[[RPCRequestContext], dict]] = {
            "PING": self._handle_ping,
            "FIND_NODE": self._handle_find_node,
            "ANNOUNCE": self._handle_announce,
            "GET_PEERS": self._handle_get_peers,
        }

    def response(
        self,
        transport: transport.UDPTransport,
        message: dict[str, t.Any],
        addr: tuple[str, int],
    ):
        """Handle an RPC request."""
        transport.send(self.__handle(message, addr), addr)

    def __handle(self, request: dict[str, t.Any], address: tuple[str, int]):
        """Handle RPC request."""

        response = netutils.get_message_template(
            "response",
            request["txid"],
            self.__local_id,
        )

        method = request.get("method")
        if not isinstance(method, str) or method not in self.__METHOD_HANDLERS:
            return self.__build_error(request, "method_unknown", "Unknown RPC method")
        try:
            sender = node.NodeInfo(node.hex_to_id(request["sender_id"]), *address)
            if sender.node_id == self.__local_id:
                return self.__build_error(
                    request,
                    "identity_conflict",
                    "Duplicate node identity",
                )
            self.__on_request(sender)

            params = request.get("params")

            ctx = RPCRequestContext(
                sender=sender,
                params=params,
            )
            callback = self.__METHOD_HANDLERS[method]
            result = callback(ctx)

            response["result"] = result
            return netutils.encode(response)

            # TODO: Shrink payload to fit max datagram size.
            # Below is an ad-hoc implementation.
            # while True:
            #     try:
            #         return netutils.encode(response)
            #     except ValueError:
            #         key = "peers" if "peers" in result else "nodes"
            #         if not result.get(key):
            #             raise
            #         result[key].pop()
        except (ValueError, TypeError, KeyError):
            return self.__build_error(
                request, "invalid_params", "Invalid request parameters"
            )

    def _handle_ping(self, _ctx: RPCRequestContext):
        return {}

    def _handle_find_node(self, ctx: RPCRequestContext):
        params = ctx.params
        if params is None:
            raise KeyError

        target = node.hex_to_id(params["target_id"])
        nodes = [info.to_dict() for info in self.__rtb.closest(target, self.__config.k)]

        # Madeup way to reduce the result list.
        # NOTE: Please change this if there's a better way.
        while True:
            try:
                dummy_msg = netutils.get_message_template(
                    "response", "0" * 32, self.__local_id
                )
                dummy_msg["result"] = {"nodes": nodes}
                netutils.encode(dummy_msg)
                break
            except ValueError:
                # Reduce by half is too steep.
                nodes = nodes[: math.floor(len(nodes) / 1.25)]

        return {"nodes": nodes}

    def _handle_announce(self, ctx: RPCRequestContext):
        sender = ctx.sender
        params = ctx.params
        if params is None:
            raise KeyError

        info = peer.PeerInfo(sender.ip, params["port"])
        self.__store.put(node.hex_to_id(params["swarm_id"]), info)
        return {"ttl": self.__config.registration_ttl}

    def _handle_get_peers(self, ctx: RPCRequestContext):
        params = ctx.params
        if params is None:
            raise KeyError

        target = node.hex_to_id(params["swarm_id"])
        peers = self.__store.get(target)
        nodes: list[dict] = []
        field_name = ""
        if not peers:
            field_name = "nodes"
            nodes = [
                info.to_dict() for info in self.__rtb.closest(target, self.__config.k)
            ]

        else:
            random.shuffle(peers)
            field_name = "peers"
            nodes = [dataclasses.asdict(info) for info in peers]

        # Madeup way to reduce the result list.
        # NOTE: Please change this if there's a better way.
        while True:
            try:
                dummy_msg = netutils.get_message_template(
                    "response", "0" * 32, self.__local_id
                )
                dummy_msg["result"] = {field_name: nodes}
                netutils.encode(dummy_msg)
                break
            except ValueError:
                # Reduce by half is too steep.
                nodes = nodes[: math.floor(len(nodes) / 1.25)]

        return {field_name: nodes}

    def __build_error(self, request: dict, code: str, message: str):
        response = netutils.get_message_template(
            "error", request["txid"], self.__local_id
        )
        response["error"] = {"code": code, "message": message}
        return netutils.encode(response)
