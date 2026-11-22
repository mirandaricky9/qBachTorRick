import asyncio
import json
import logging
import signal
import sys

from src.bittorrent import BitTorrentNode
from src.config import DHTConfig, PeerConfig

LOGGER = logging.getLogger(__name__)


async def main(config_path: str):
    with open(config_path, encoding="utf-8") as config_file:
        settings = json.load(config_file)

    bootstrap = None
    if settings.get("bootstrap") is not None:
        bootstrap_ip, bootstrap_port = settings["bootstrap"]
        bootstrap = (bootstrap_ip, bootstrap_port)

    node = BitTorrentNode(
        advertise_ip=settings["advertise_ip"],
        bind_ip=settings.get("bind_ip", "0.0.0.0"),
        config=DHTConfig(
            port=settings["port"],
            bootstrap=bootstrap,
        ),
        peer_config=PeerConfig(port=settings["peer_port"]),
    )

    # Properly handle SIGTERM.
    stopper = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stopper.set)

    try:
        await node.start()
        print(f"DHT ID: {node.dht.node_id}, peer ID: {node.swarm.peer_id}")

        # TODO: probably make this a CLI-like experience with commands.
        # For example, JOIN ip to join instead of relying on bootstrap.
        # Or GET_PEERS torrent_id to perform the lookup.
        await node.join_dht(bootstrap)
        LOGGER.info(
            "DHT running at %s:%d (peer port %d)",
            node.dht.ip,
            node.dht.port,
            settings["peer_port"],
        )
        if bootstrap is not None:
            LOGGER.info("Joined DHT through %s:%d", *bootstrap)
        await stopper.wait()
    finally:
        await node.stop()
        loop.remove_signal_handler(signal.SIGTERM)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} /path/to/config.json")
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")
    try:
        asyncio.run(main(sys.argv[1]))
    except KeyboardInterrupt:
        pass
