import random
import time
import typing as t

from src import utils
from src.dht import kbucket
from src.models import node


class RoutingTable:
    """The routing/finger table for each DHT node.

    Buckets
    -------
    Routing table contains a list of bucket of max capacity `k`. Initially, there is one bucket covering the entire ID space.
    When the bucket capacity is reached, the routing table will attempt to split the bucket in half.

    The split logic can be found in `RoutingTable.add()`. Logically, the routing table will split buckets closer to itself.

    Refresh
    -------
    As defined in BEP 5, if a bucket has no activity in 15 minutes, it is considered stale.
    "Activity" is when a DHT node is added or answer to a request/response or when a DHT node is promoted from backup to active.
    When a bucket is stale, it is refreshed by performing a node lookup for a random ID within the bucket range.

    This refreshing behavior is separate from DHT node idling, which checks whether the peer is alive.
    """

    def __init__(
        self,
        local_id: int,
        k: int,
        clock: t.Callable[[], float] = time.monotonic,
        failure_limit: int = 3,
        idle_threshold: float = 30,  # in BEP 5, this is 900s, but for testing we use 30s
    ):
        """Construct the routing table for a DHT node.

        Parameters
        ----------
        local_id : int
            Should be your DHT node ID.
        k : int
            The max number of DHT peers in a bucket.
        clock : t.Callable[[], float], optional
            A clock function that returns `float`. Default to `time.monotonic`.
        failure_limit : int
            How many times a DHT node failed consecutively to RPC before being evicted. Default to 3.
        idle_threshold : float
            Number of seconds a DHT node is allowed to not communicate with us.
            If a DHT node exceed this value, it will be PINGed. Default to 30 seconds.

        Raises
        ------
        ValueError
            When `k` is non-positive or when `local_id` is invalid.
        """

        utils.validate_id(local_id)
        if k < 1:
            raise ValueError("k must be positive")

        self.__local_id = local_id
        self.__k = k
        self.__clock = clock
        # Number of failed RPC before declared as failed.
        self.__failure_limit = failure_limit
        self.__idle_threshold = idle_threshold

        # Ordered list of buckets covering the entire ID space.
        # A bucket split when 1. Insert to bucket but it's full 2. Bucket contains local ID.
        self.buckets = [kbucket.KBucket(0, utils.ID_LIMIT, k, clock())]

    def bucket_for(self, node_id: int) -> kbucket.KBucket:
        """Return the bucket that can contain `node_id`."""

        utils.validate_id(node_id)
        return next(bucket for bucket in self.buckets if bucket.can_contain(node_id))

    def add(self, info: node.NodeInfo):
        """Add a node to the routing table.
        If it is added as a backup candidate and the bucket is not probing,
        return the oldest active peer for probing.

        Parameters
        ----------
        info : node.NodeInfo
            The node to add.

        Returns
        -------
        node.NodeInfo, optional
            If the node is added to the backup list and bucket is not probing,
            return the oldest active peer information.

        Notes
        -----
        Adding a node to the routing table will involve bucket splitting. The algorithm for this is:
            - Given a node ID, we locate which bucket should store it. We call this the targeted bucket.
            - While the targeted bucket is full and its range is close to us:
                - Split the targeted bucket into two.
                - Update targeted bucket to the correct one out of the two.
            - If the targeted bucket is not full, add the node to the active list. Otherwise, add to the backup list.

        The idea is to maximize the number of close peers while also maintaining some distant peers for coverage.
        """

        if info.node_id == self.__local_id:
            return

        record = kbucket.NodeRecord(info, self.__clock())
        bucket = self.bucket_for(info.node_id)
        if info.node_id in bucket.active_nodes:
            # Update the bucket refresh timer.
            bucket.add(record)
            return

        # Keep splitting while the bucket is close.
        while bucket.is_full() and bucket.can_contain(self.__local_id):
            index = self.buckets.index(bucket)
            left, right = bucket.split()
            self.buckets[index : index + 1] = [left, right]
            bucket = left if left.can_contain(info.node_id) else right
        if not bucket.is_full():
            bucket.add(record)
            return

        bucket.add_as_backup(record)
        # Bucket is full, we want to probe the oldest node
        # see if it should be replaced or not.
        # That would be a separate step.
        if bucket.probe_id is None:
            bucket.probe_id = next(iter(bucket.active_nodes))
            return bucket.active_nodes[bucket.probe_id].info

        return None

    def remove(self, node_id: int):
        """Remove a node ID from the routing table."""

        bucket = self.bucket_for(node_id)
        bucket.remove(node_id)
        return self.promote_backup(bucket)

    def promote_backup(self, bucket: kbucket.KBucket):
        """Attempt to promote backup nodes to active for a bucket.

        Returns
        -------
        NodeInfo, optional
            If the bucket is not probing anything and the to-be-promoted node is stale,
            return the node for probing.
        """

        # This function is here instead of KBucket cuz of self.__clock()
        # and passing self.__clock as a parameter sounds wrong, idk :/

        if bucket.is_full():
            return

        while bucket.backup_nodes and not bucket.is_full():
            node_id, node_record = next(reversed(bucket.backup_nodes.items()))
            if self.__clock() - node_record.last_seen > self.__idle_threshold:
                if bucket.probe_id is None:
                    bucket.probe_id = node_id
                    return node_record.info
                break
            else:
                bucket.add(node_record)
                bucket.last_changed = self.__clock()

        return None

    def finish_probe(self, node_id: int):
        """Unset the probe id for a bucket if it matches `node_id`."""

        bucket = self.bucket_for(node_id)
        if bucket.probe_id == node_id:
            bucket.probe_id = None

    def report_failure(self, node_id: int):
        """Mark a node as failed and increase its failure counter.
        If the node is a backup node, it is removed instead.

        If the counter for this active node reaches `failure_limit`, it will be removed.
        Backup promotion then happens internally.
        """

        bucket = self.bucket_for(node_id)
        record = bucket.active_nodes.get(node_id)
        if record is None:
            bucket.backup_nodes.pop(node_id, None)
            if bucket.probe_id == node_id:
                bucket.probe_id = None

            return self.promote_backup(bucket)

        record.failure_count += 1
        if record.failure_count >= self.__failure_limit:
            bucket.remove(node_id)
            return self.promote_backup(bucket)

        return None

    def get(self, node_id: int) -> kbucket.NodeRecord | None:
        return self.bucket_for(node_id).active_nodes.get(node_id)

    def closest(self, target_id: int, count: int) -> list[node.NodeInfo]:
        utils.validate_id(target_id)
        return sorted(
            self.all_nodes(),
            key=lambda info: info.node_id ^ target_id,
        )[:count]

    def all_nodes(self) -> list[node.NodeInfo]:
        result = []
        for bucket in self.buckets:
            result.extend(record.info for record in bucket.active_nodes.values())
        return result

    def get_refresh_targets(self, interval: float = 900):
        """Return a list of targets to refresh stale buckets.

        By default, a bucket with no changes after 15 minutes (900 seconds) is a stale bucket.
        To refresh a stale bucket, pick a random ID within the bucket range and perform `find_nodes()` on that ID.
        """

        now = self.__clock()
        targets: list[int] = []
        for bucket in self.buckets:
            if now - max(bucket.last_changed, bucket.last_refresh_attempt) >= interval:
                bucket.last_refresh_attempt = now
                targets.append(random.randrange(*bucket.get_range()))
        return targets
