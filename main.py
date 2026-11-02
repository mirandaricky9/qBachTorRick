import src.dht.routing_table as rtb
from src.models import node


def main():
    nodes: list[node.NodeInfo] = []
    routings: list[rtb.RoutingTable] = []
    for i in range(10):
        nodes.append(
            node.NodeInfo(
                node_id=i,
                ip=f"127.0.0.{i}",
                port=8000 + i,
            )
        )
        routings.append(rtb.RoutingTable(local_id=i, k=2))

    routings[0].add(nodes[1])
    routings[0].add(nodes[2])
    routings[0].add(nodes[5])
    routings[0].add(nodes[9])

    print(routings[0].closest(target_id=3, count=2))
    fake_node = node.NodeInfo(
        node_id=10000,
        ip="0.0.0.0",
        port=15000,
    )
    routings[0].add(fake_node)
    print([bucket for bucket in routings[0].buckets if bucket.active_nodes])


if __name__ == "__main__":
    main()
