import collections
import dataclasses

from src.models import node


@dataclasses.dataclass
class NodeRecord:
    info: node.NodeInfo
    last_seen: float
    failure_count: int = 0


class KBucket:
    """Represent the bucket in the DHT routing table at each DHT node.

    The bucket internally contains a list of **active** and **backup** nodes, each with `k` max capacity.
    Active nodes are considered when performing DHT lookup. Backup nodes are when active nodes list is no longer full.
    Both active and backup lists are sorted least recently seen first.

    The bucket also keep track of `last_changed` and `last_refresh_attempt` to facilitate bucket refreshing.

    For more information about bucket splitting and refreshing, see `RoutingTable`.
    """

    def __init__(self, lower: int, upper: int, k: int, now: float = 0):
        """Construct a bucket with `k` capacity.
        It will contain node IDs within `[lower, upper)`.

        Parameters
        ----------
        lower : int
            The lower bound of the bucket.
        upper : int
            The upper bound of the bucket.
        k : int
            The max capacity of the bucket. As discussed above, the true max capacity is 2k.
        now : float
            The time this bucket is created. Default to 0.

        Raises
        ------
        ValueError
            Invalid bucket range or capacity.
        """

        if not 0 <= lower < upper <= node.ID_LIMIT or k < 1:
            raise ValueError("Invalid bucket range or capacity")

        self.__k = k
        self.__lower = lower
        self.__upper = upper

        # Sorted from least recently seen to most recently seen.
        self.active_nodes = collections.OrderedDict[int, NodeRecord]()
        self.backup_nodes = collections.OrderedDict[int, NodeRecord]()

        self.last_changed = now
        # NOTE: probably not needed? Just putting it here for correctness.
        self.last_refresh_attempt = now
        # Node ID to be probed (PING) as part of bucket refreshing and backup promotion.
        # Put it here cuz we're supposed to only probe for one node per bucket.
        self.probe_id: int | None = None

    def can_contain(self, node_id: int) -> bool:
        return self.__lower <= node_id < self.__upper

    def is_full(self) -> bool:
        return len(self.active_nodes) >= self.__k

    def get_range(self):
        """Return `[lower, upper)`."""
        return (self.__lower, self.__upper)

    def add(self, record: NodeRecord):
        """Add a node to the bucket's active peers.
        If successful, this also refresh the bucket's `last_changed`.

        If this node is already in the backup list, it'll be removed from the backup list.

        Parameters
        ----------
        record : NodeRecord
            The node record to add.

        Raises
        ------
        ValueError
            Either the ID is outside the bucket range, or the bucket is full.
            Happens on new insertion only.
        """

        node_id = record.info.node_id
        if not self.can_contain(node_id):
            raise ValueError("ID outside bucket range")
        if node_id not in self.active_nodes and self.is_full():
            raise ValueError("Bucket is full")

        self.active_nodes[node_id] = record
        self.active_nodes.move_to_end(node_id)
        self.backup_nodes.pop(node_id, None)
        self.last_changed = record.last_seen

    def add_as_backup(self, record: NodeRecord):
        node_id = record.info.node_id
        if not self.can_contain(node_id):
            raise ValueError("ID outside bucket range")

        self.backup_nodes[node_id] = record
        self.backup_nodes.move_to_end(node_id)
        if len(self.backup_nodes) > self.__k:
            self.backup_nodes.popitem(last=False)

    def remove(self, node_id: int):
        """Remove a node from the active and backup list."""

        self.active_nodes.pop(node_id, None)
        self.backup_nodes.pop(node_id, None)
        if self.probe_id == node_id:
            self.probe_id = None

    def split(self):
        """Return two buckets resulted from splitting itself.

        These buckets will inherit `last_changed` and `last_refresh_attempt`.
        For active and backup nodes, each will contain according to their range.
        Same goes for `probe_id`.
        """

        midpoint = (self.__lower + self.__upper) // 2
        if midpoint == self.__lower:
            raise ValueError("Can't split this bucket any further")

        children = (
            KBucket(self.__lower, midpoint, self.__k, self.last_changed),
            KBucket(midpoint, self.__upper, self.__k, self.last_changed),
        )
        for child_bucket in children:
            child_bucket.last_refresh_attempt = self.last_refresh_attempt

            for key, value in self.active_nodes.items():
                if child_bucket.can_contain(key):
                    child_bucket.active_nodes[key] = value

            for key, value in self.backup_nodes.items():
                if child_bucket.can_contain(key):
                    child_bucket.backup_nodes[key] = value

            if self.probe_id is not None and child_bucket.can_contain(self.probe_id):
                child_bucket.probe_id = self.probe_id

        return children

    def __repr__(self) -> str:
        return f"KBucket([{self.__lower}, {self.__upper}), active={self.active_nodes}, backup={self.backup_nodes})"

    def __str__(self) -> str:
        return f"KBucket([{self.__lower}, {self.__upper}), active={[node.info for node in self.active_nodes.values()]}, backup={len(self.backup_nodes)})"
