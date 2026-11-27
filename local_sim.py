# Interactive console for local DHT and peer operations.

import asyncio
import logging
import pathlib
import random
import secrets
import shlex

from src import bittorrent, config, utils
from src.models import peer
from src.torrent import metainfo

NODE_COUNT = 5


def get_node(nodes: list[bittorrent.BitTorrentNode], index: str):
    node_index = int(index)
    if not 0 <= node_index < len(nodes):
        raise ValueError(f"Node index must be in [0, {len(nodes) - 1}]")
    return nodes[node_index]


async def handle_dht_join(nodes, args):
    node = get_node(nodes, args[0])
    address = None
    if len(args) == 2:
        target = get_node(nodes, args[1]).dht
        address = (target.ip, target.port)
    await node.join_dht(address)
    print(f"Node {args[0]} joined DHT: {node.dht}")


async def handle_dht_id(nodes, args):
    node = get_node(nodes, args[0])
    print(utils.id_to_hex(node.dht.node_id))


async def handle_dht_find_nodes(nodes, args):
    node = get_node(nodes, args[0])
    contacts = await node.find_nodes(utils.hex_to_id(args[1]))
    print(contacts)


async def handle_dht_get_peers(nodes, args):
    node = get_node(nodes, args[0])
    peers = await node.get_peers(utils.hex_to_id(args[1]))
    print(peers)


async def handle_dht_announce(nodes, args):
    node = get_node(nodes, args[0])
    result = await node.announce(utils.hex_to_id(args[1]))
    print(result)


async def handle_leave(nodes, args):
    node = get_node(nodes, args[0])
    await node.leave(utils.hex_to_id(args[1]))
    print(f"Node {args[0]} left swarm {args[1]}")


async def handle_stop(nodes, args):
    node = get_node(nodes, args[0])
    await node.stop()
    print(f"Node {args[0]} stopped")


async def handle_peer_id(nodes, args):
    node = get_node(nodes, args[0])
    print(utils.id_to_hex(node.swarm.peer_id))


async def handle_peer_connections(nodes, args):
    node = get_node(nodes, args[0])
    connections = node.swarm.get_connections(utils.hex_to_id(args[1]))
    peers = [
        {
            "peer_id": utils.id_to_hex(connection.peer_id),
            "ip": connection.peer_endpoint.ip,
            "port": connection.peer_endpoint.port,
        }
        for connection in connections
    ]
    print(peers)


async def handle_peer_connect(nodes, args):
    node = get_node(nodes, args[0])
    swarm_id = utils.hex_to_id(args[1])
    endpoint = peer.PeerInfo(args[2], int(args[3]))
    await node.connect_peer(swarm_id, endpoint)
    print(f"Node {args[0]} connected to {endpoint} for swarm {args[1]}")


async def handle_peer_cached_peers(nodes, args):
    node = get_node(nodes, args[0])
    print(node.swarm.get_cached_peers(utils.hex_to_id(args[1])))


async def handle_create_torrent(nodes, args):
    node = get_node(nodes, args[0])
    content_path = pathlib.Path(args[1]).expanduser().resolve()
    torrent = await asyncio.to_thread(metainfo.Torrent.from_path, content_path)
    swarm_id = await node.register_torrent(torrent, data_path=content_path)
    print(
        f"Node {args[0]} created torrent {utils.id_to_hex(swarm_id)} "
        f"with {len(torrent.info.pieces)} pieces"
    )


async def handle_add_torrent(nodes, args):
    node = get_node(nodes, args[0])
    swarm_id = await node.add_torrent(args[1])
    print(f"Node {args[0]} added torrent {utils.id_to_hex(swarm_id)}")


async def handle_list_torrent(nodes, args):
    node = get_node(nodes, args[0])
    torrents = node.swarm.get_torrents()
    if not torrents:
        print(f"Node {args[0]} has no torrents")
        return
    for torrent in torrents:
        print(f"{utils.id_to_hex(torrent.swarm_id)} {torrent.info.name}")


async def handle_quit(nodes, args):
    print("Stopping nodes and quitting...")


async def handle_help(nodes, args):
    for _, usage, _ in COMMANDS.values():
        print(usage)


async def handle_gen_id(nodes, args):
    print(secrets.token_hex(32))


COMMANDS = {
    "CREATE_TORRENT": (
        handle_create_torrent,
        "CREATE_TORRENT node_index torrent_content_path",
        (2,),
    ),
    "ADD_TORRENT": (handle_add_torrent, "ADD_TORRENT node_index torrent_path", (2,)),
    "LIST_TORRENT": (handle_list_torrent, "LIST_TORRENT node_index", (1,)),
    "DHT_JOIN": (handle_dht_join, "DHT_JOIN node_index [target_index]", (1, 2)),
    "DHT_ID": (handle_dht_id, "DHT_ID node_index", (1,)),
    "DHT_FIND_NODES": (
        handle_dht_find_nodes,
        "DHT_FIND_NODES node_index target_id",
        (2,),
    ),
    "DHT_GET_PEERS": (
        handle_dht_get_peers,
        "DHT_GET_PEERS node_index swarm_id",
        (2,),
    ),
    "DHT_ANNOUNCE": (handle_dht_announce, "DHT_ANNOUNCE node_index swarm_id", (2,)),
    "LEAVE": (handle_leave, "LEAVE node_index swarm_id", (2,)),
    "STOP": (handle_stop, "STOP node_index", (1,)),
    "PEER_ID": (handle_peer_id, "PEER_ID node_index", (1,)),
    "PEER_CONNECTIONS": (
        handle_peer_connections,
        "PEER_CONNECTIONS node_index swarm_id",
        (2,),
    ),
    "PEER_CONNECT": (
        handle_peer_connect,
        "PEER_CONNECT node_index swarm_id ip port",
        (4,),
    ),
    "PEER_CACHED_PEERS": (
        handle_peer_cached_peers,
        "PEER_CACHED_PEERS node_index swarm_id",
        (2,),
    ),
    "QUIT": (handle_quit, "QUIT", (0,)),
    "HELP": (handle_help, "HELP", (0,)),
    "GEN_ID": (handle_gen_id, "GEN_ID", (0,)),
}


async def command_loop(nodes: list[bittorrent.BitTorrentNode]):
    print(f"Started {len(nodes)} nodes.")
    print("Type HELP for available commands.")
    while True:
        try:
            # Keep network and maintenance tasks running while waiting for input.
            line = await asyncio.to_thread(input, "> ")
        except EOFError:
            return

        try:
            parts = shlex.split(line)
        except ValueError as error:
            print(f"Error: {error}")
            continue
        if not parts:
            continue

        command = parts[0].upper()
        if command not in COMMANDS:
            print(f"Unknown command: {parts[0]}. Type HELP for available commands.")
            continue

        handler, usage, counts = COMMANDS[command]
        args = parts[1:]
        if len(args) not in counts:
            print(f"Usage: {usage}")
            continue

        try:
            await handler(nodes, args)
        except (ValueError, RuntimeError, OSError, EOFError) as error:
            print(f"Error: {error}")
            continue

        if command == "QUIT":
            return


async def main():
    nodes: list[bittorrent.BitTorrentNode] = []
    try:
        for i in range(NODE_COUNT):
            node = bittorrent.BitTorrentNode(
                advertise_ip=f"127.0.0.{i + 1}",
                bind_ip=f"127.0.0.{i + 1}",
                config=config.DHTConfig(port=6000 + i),
                peer_config=config.PeerConfig(
                    port=8000 + i,
                    torrents_dir=f"./tests/torrent/node{i}",
                    data_dir=f"./tests/data/node{i}",
                ),
            )
            await node.start()
            nodes.append(node)

            if i == 0:
                await node.join_dht()
            else:
                selected_node = random.choice(nodes[:-1])
                await node.join_dht((selected_node.dht.ip, selected_node.dht.port))

        await command_loop(nodes)
    finally:
        await asyncio.gather(*(node.stop() for node in nodes))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s:%(name)s: %(message)s"
    )
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
