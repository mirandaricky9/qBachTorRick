import asyncio
import secrets

from src import utils
from src.bittorrent import BitTorrentNode
from src.config import DHTConfig


async def main():
    nodes: list[BitTorrentNode] = []
    for i in range(10):
        nodes.append(
            BitTorrentNode(
                config=DHTConfig(
                    advertise_ip=f"127.0.0.{i + 1}",
                    bind_ip=f"127.0.0.{i + 1}",
                    port=6000 + i,
                ),
                peer_port=8000 + i,
            )
        )
        await nodes[i].start()

    await nodes[0].join_dht()

    for i in range(1, 10):
        # Join nodes[0]
        await nodes[i].join_dht(("127.0.0.1", 6000))

    try:
        infohash = secrets.token_hex(32)
        await nodes[1].announce(utils.hex_to_id(infohash))
        await nodes[5].announce(utils.hex_to_id(infohash))
        peers = await nodes[9].get_peers(utils.hex_to_id(infohash))
        print(f"Peer for {infohash}: {peers}")
        await nodes[1].leave(utils.hex_to_id(infohash))
        print("Node 1 left swarm. Waiting 18s to update...")
        await asyncio.sleep(18)
        peers = await nodes[9].get_peers(utils.hex_to_id(infohash))
        print(f"Peer for {infohash}: {peers}")
        await asyncio.sleep(2)
        peers = await nodes[9].get_peers(utils.hex_to_id(infohash))
        print(f"Peer for {infohash}: {peers}")
    except KeyboardInterrupt:
        pass

    for i in range(10):
        await nodes[i].stop()


if __name__ == "__main__":
    asyncio.run(main())
