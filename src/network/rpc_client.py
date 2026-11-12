import asyncio
import dataclasses
import secrets
import typing as t

from src import config
from src.models import node
from src.network import netutils, transport


class RPCError(RuntimeError):
    pass


@dataclasses.dataclass
class PendingRequestInfo:
    """Store pending request information."""

    address: tuple[str, int]
    receiver_id: int | None
    method: str
    future: asyncio.Future[tuple[node.NodeInfo, dict[str, t.Any]]]


class RPCClient:
    def __init__(
        self,
        local_id: int,
        config: config.DHTConfig,
        on_valid_response: t.Callable[[node.NodeInfo], t.Any],
        on_delivery_error: t.Callable[[int], t.Any],
    ):
        self.__local_id = local_id
        self.__config = config
        self.__on_valid_response = on_valid_response
        self.__on_delivery_error = on_delivery_error

        # Keep track of which requests are waiting for response.
        self.__pending: dict[str, PendingRequestInfo] = {}

        # Keep track of which nodes are busy responding to us so we don't probe them anymore.
        # It's possible for node to appear in pending and not busy
        # when we first contact another node without node ID (like first join).
        self.__busy: dict[int, int] = {}
        # Max number of concurrent outbound requests.
        # Temporarily fixed, can be configured later.
        self.__slots = asyncio.Semaphore(20)

        # A flag whether the RPC client is stopped.
        self.__closed = False

    def is_busy(self, node_id: int):
        """Return whether `node_id` maybe busy responding to one of our ongoing request.

        Notes
        -----
        This is used for checking whether we sent a request to `node_id` and we think `node_id` is responding to us.
        If this is the case, we can postpone a PING, because its response is a liveness evidence.
        """

        return node_id in self.__busy

    async def request(
        self,
        transport: transport.UDPTransport,
        address: tuple[str | t.Any, int],
        method: str,
        params: dict[str, t.Any],
        receiver_id: int | None = None,
    ):
        """Send a request and wait asynchronously until timeout or when result arrives.

        Return the `NodeInfo` and the `result` object in the response field.
        """
        async with self.__slots:
            if self.__closed:
                raise ConnectionError("RPC client is stopped")

            txid = secrets.token_hex(16)
            while txid in self.__pending:
                txid = secrets.token_hex(16)

            future: asyncio.Future[tuple[node.NodeInfo, dict[str, t.Any]]] = (
                asyncio.get_running_loop().create_future()
            )
            try:
                message = netutils.get_message_template(
                    "request", txid, self.__local_id
                )
                # message.update(method=method, params=params)
                message["method"] = method
                message["params"] = params

                self.__pending[txid] = PendingRequestInfo(
                    address, receiver_id, method, future
                )
                if receiver_id is not None:
                    self.__busy[receiver_id] = self.__busy.get(receiver_id, 0) + 1

                transport.send(netutils.encode(message), address)
                return await asyncio.wait_for(future, self.__config.rpc_timeout)
            except (TimeoutError, OSError):
                if receiver_id is not None:
                    self.__on_delivery_error(receiver_id)
                raise
            finally:
                self.__pending.pop(txid, None)
                if not future.done():
                    future.cancel()
                if receiver_id is not None:
                    self.__busy[receiver_id] -= 1
                    if not self.__busy[receiver_id]:
                        del self.__busy[receiver_id]

    def receive(self, message: dict, address: tuple):
        """Receive and parse a decoded UDP response.
        This will finish the matching `RPCClient.request()`,
        so it is important to invoke this when intercepting UDP response.
        """

        pending_req = self.__pending.get(message["txid"])
        if (
            pending_req is None
            or pending_req.future.done()
            or pending_req.address != address
        ):
            return

        sender_id = node.hex_to_id(message["sender_id"])
        if pending_req.receiver_id is not None and sender_id != pending_req.receiver_id:
            return
        if sender_id == self.__local_id:
            return

        if message["type"] == "error":
            error = message.get("error")
            if not isinstance(error, dict) or not isinstance(error.get("code"), str):
                return
            if not isinstance(error.get("message"), str):
                return

            # An error message is a message-sent-successfully so we use __on_valid_response().
            self.__on_valid_response(node.NodeInfo(sender_id, *address))
            pending_req.future.set_exception(RPCError(error["message"]))
            return

        result = message.get("result")
        if not isinstance(result, dict):
            return

        for key in ("nodes", "peers"):
            if key not in result:
                continue
            entries = result[key]
            if not isinstance(entries, list):
                raise TypeError("Invalid contact list")

            contacts: list[dict[str, t.Any]] = []
            for entry in entries:
                if not isinstance(entry, dict):
                    raise TypeError("Contact must be an object")

                contact = {
                    "node_id": node.hex_to_id(entry.get("node_id")),
                    "ip": entry.get("ip"),
                    "port": entry.get("port"),
                }
                contacts.append(contact)

            result[key] = contacts

        info = node.NodeInfo(sender_id, *address)
        self.__on_valid_response(info)
        pending_req.future.set_result((info, result))

    def stop(self):
        self.__closed = True
        for pending_req in self.__pending.values():
            if not pending_req.future.done():
                pending_req.future.set_exception(ConnectionError("Node stopped"))
