"""Bounded rarest-first downloads for one swarm."""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import typing as t

from src import config
from src.swarm import peerutils
from src.torrent import diskio, store

if t.TYPE_CHECKING:
    # PeerConnection imports TransferCoordinator at runtime.
    from src.swarm import connection

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class PartialPiece:
    """Represent a partially downloaded piece."""

    data: bytearray
    """Buffer holding the bytes within the piece."""

    received: set[int] = dataclasses.field(default_factory=set)
    """Track the offset that has been received to identify which block is finished."""

    contributors: set[connection.PeerConnection] = dataclasses.field(
        default_factory=set
    )
    """Track which peers supply the blocks."""


class TransferCoordinator:
    """Handle sending and receiving pieces within a torrent.

    This will look at the local state and peer states to automatically schedule downloads.
    It will send the necessary `REQUEST` and `CANCEL` and will handle `PIECE`.
    """

    def __init__(
        self,
        record: store.TorrentRecord,
        config: config.PeerConfig,
        clock: t.Callable[[], float],
        get_connections: t.Callable[[], list[connection.PeerConnection]],
        mark_verified: t.Callable[[int], t.Awaitable[None]],
    ):
        """Construct the downloader.

        Parameters
        ----------
        record : TorrentRecord
            The loaded torrent.
        config : PeerConfig
            The settings for this downloader.
        clock : t.Callable[[], float]
            The clock to keep track of timeouts.
        get_connections : t.Callable[[], list[connection.PeerConnection]]
            A callback to obtain all active peer connections.
            This is used to obtain peer remote piece bitmap.
        mark_verified : t.Callable[[int], t.Awaitable[None]]
            An awaitable callback that will update the local bitmap.
        """
        
        self.io = diskio.TorrentIO(record)
        self.__state = record.availability
        self.__config = config
        self.__clock = clock

        self.__get_connections = get_connections
        self.__mark_verified = mark_verified

        # {piece_index: PartialPiece}
        self.__pieces: dict[int, PartialPiece] = {}
        # Track ongoing block requests.
        # {(piece_index, offset_from_piece, size): {peer_conn: clock()}}
        self.__requests: dict[
            tuple[int, int, int],
            dict[connection.PeerConnection, float],
        ] = {}
        self.__cancels: set[
            tuple[
                connection.PeerConnection,
                tuple[int, int, int],
            ]
        ] = set()
        self.__wake = asyncio.Event()
        self.__task: asyncio.Task | None = None
        self.__stopped = False

    @property
    def is_stopped(self):
        return self.__stopped

    def start(self):
        if self.__task is None and not self.__stopped:
            self.__task = asyncio.create_task(self.__run())

    def changed(self):
        """Trigger an internal update.

        For other classes, this is best used when either:
        - The torrent local availability is changed.
        - The peer piece availability is changed.
        """

        self.__wake.set()

    def has_piece(self, index: int):
        return bool(self.__state.bitmap & (1 << index))

    def is_request_expired(self, conn: connection.PeerConnection):
        """Whether any outstanding request to this connection has timed out."""

        now = self.__clock()
        for peers in self.__requests.values():
            sent_at = peers.get(conn)
            if sent_at is not None and now - sent_at >= self.__config.request_timeout:
                return True
        return False

    def disconnect_from(self, conn: connection.PeerConnection):
        """Clear all pending messages for a connection (like `REQUEST` and `CANCEL`)."""

        for requested_blocks, owners in list(self.__requests.items()):
            owners.pop(conn, None)
            if not owners:
                del self.__requests[requested_blocks]
        self.__cancels = {item for item in self.__cancels if item[0] is not conn}
        self.changed()

    def fail(self, error):
        """Mark the download as failed."""

        if not self.__stopped:
            LOGGER.error("Torrent transfers stopped: %s", error)
            self.__stopped = True
            self.changed()

    def stop(self):
        """Prevent new work before asynchronous shutdown starts."""

        self.__stopped = True
        self.changed()

    def on_receive_piece(
        self,
        conn: connection.PeerConnection,
        index: int,
        begin: int,
        data: bytes,
    ):
        """Handle an incoming PIECE message.

        Parameters
        ----------
        conn : PeerConnection
            The peer connection used.
        index : int
            The piece index.
        begin : int
            The byte offset within the piece.
        data : bytes
            The actual data.
        """

        self.io.validate_block(index, begin, len(data))
        if self.__stopped:
            return

        print("Received PIECE")
        requested_block = index, begin, len(data)

        owners = self.__requests.get(requested_block)

        # We're receiving a PIECE we didn't ask for.
        # Could happen when we receive one while sending CANCEL.
        # Another case (owners is None) is when the length is different.
        if owners is None or conn not in owners:
            # We check if this PIECE comes from someone we're expecting.
            # A reason we couldn't find is because the length is different.
            # In that case, there's something wrong and it's best to close the conn.
            for block, requested_peers in self.__requests.items():
                same_block = block[:2] == (index, begin)
                if same_block and conn in requested_peers:
                    raise ValueError("PIECE message does not match REQUEST")

            return

        # No longer outstanding.
        # Send CANCEL to other peers (if exist).
        del self.__requests[requested_block]
        for other in owners:
            if other is not conn:
                self.__cancels.add((other, requested_block))

        piece = self.__pieces[index]
        piece.data[begin : begin + len(data)] = data
        piece.received.add(begin)
        piece.contributors.add(conn)
        self.changed()

    async def close(self):
        self.stop()
        if self.__task is not None:
            self.__task.cancel()
            await asyncio.gather(self.__task, return_exceptions=True)
        self.__pieces.clear()
        self.__requests.clear()
        self.__cancels.clear()

    async def __run(self):
        try:
            await diskio.run_io(self.io.prepare)

            while not self.__stopped:
                self.__wake.clear()
                await self.__send_cancels()
                await self.__finish_pieces()

                if self.__stopped:
                    break

                await self.__schedule()
                try:
                    async with asyncio.timeout(self.__config.maintenance_interval):
                        await self.__wake.wait()
                except TimeoutError:
                    pass
        except (OSError, ValueError) as error:
            self.fail(error)
        finally:
            if self.__stopped:
                # Also stop uploads after a storage failure. Connection cleanup
                # waits for its filesystem workers before returning.
                await asyncio.gather(
                    *(conn.close() for conn in self.__get_connections())
                )

    def __next_request(self, conn: connection.PeerConnection):
        """Return the next block we should request the peer."""

        for index, piece in self.__pieces.items():
            # Peer don't have this piece.
            if not conn.remote_pieces & (1 << index):
                continue

            for block in self.__blocks(index):
                # We already got this block.
                if block[1] in piece.received:
                    continue

                owners = self.__requests.get(block, {})
                # Already requesting this block.
                if conn in owners:
                    continue

                # Skip if someone is working on it when not endgame.
                # Or if we exceed the configured cap during endgame.
                if owners and (
                    not self.__is_endgame()
                    or len(owners) >= self.__config.endgame_peers_per_block
                ):
                    continue
                return block
        return None

    async def __send(self, conn: connection.PeerConnection, message: dict[str, t.Any]):
        """Send a message.

        Returns
        -------
        bool
            Whether the message is sent successfully.
        """

        if not conn.is_connected:
            self.disconnect_from(conn)
            return False
        if self.__stopped:
            return False

        try:
            async with asyncio.timeout(self.__config.request_timeout):
                await conn.send(message)
            return True
        except (OSError, EOFError, ValueError, TimeoutError):
            await conn.close()
            self.disconnect_from(conn)
            return False

    async def __send_cancels(self):
        """Send CANCEL messages."""

        for conn, key in list(self.__cancels):
            self.__cancels.discard((conn, key))
            await self.__send(conn, peerutils.get_cancel_msg(*key))

    def __blocks(self, index: int):
        """Yield the byte offset and size within a piece to download."""

        size = self.io.piece_size(index)
        for begin in range(0, size, self.__config.block_size):
            yield index, begin, min(self.__config.block_size, size - begin)

    def __discard_piece(self, index: int):
        """Cancel downloading a piece.

        This can be used to remove a partially downloaded piece in order to prioritize another piece,
        or to remove a piece that has been saved to disk.
        """

        for requested_block in list(self.__requests):
            if requested_block[0] == index:
                for conn in self.__requests.pop(requested_block):
                    self.__cancels.add((conn, requested_block))
        del self.__pieces[index]

    def __setup_piece_buffers(self, connections: list[connection.PeerConnection]):
        """Setup data buffers for pieces.
        This creates `PartialPiece` for pieces (with priority for rare pieces).

        Parameters
        ----------
        connections : list[PeerConnection]
            A list of all active peer connections within the swarm.
            This is used to identify which pieces each peer have.
        """

        piece_count: dict[int, int] = {}
        for index in range(self.__state.piece_count):
            if self.__state.bitmap & (1 << index):
                continue

            count = sum(bool(conn.remote_pieces & (1 << index)) for conn in connections)
            if count:
                piece_count[index] = count

        for index in list(self.__pieces):
            if index not in piece_count:
                # No connected peer can finish this piece. Its partial buffer
                # must not prevent other available pieces from being admitted.
                self.__discard_piece(index)

        # Prioritize downloading rarest pieces first.
        rarest_first_pieces = sorted(
            piece_count,
            key=lambda index: (piece_count[index], index),
        )
        for index in rarest_first_pieces:
            if len(self.__pieces) >= self.__config.active_pieces:
                break
            if index not in self.__pieces:
                self.__pieces[index] = PartialPiece(
                    bytearray(self.io.piece_size(index))
                )

    def __is_endgame(self):
        """Whether we're at the endgame stage.

        Endgame is defined as only a few blocks remain to download.
        If we are, we send duplicate requests to avoid being held up by a slow peer.
        """

        remaining = 0
        for index in range(self.__state.piece_count):
            if self.__state.bitmap & (1 << index):
                continue
            piece = self.__pieces.get(index)
            for key in self.__blocks(index):
                if piece is not None and key[1] in piece.received:
                    continue
                remaining += 1
                if (
                    remaining > self.__config.endgame_blocks
                    or key not in self.__requests
                ):
                    return False
        return remaining > 0

    async def __schedule(self):
        """Send `REQUEST` messages for piece blocks.

        Among the peers that has a piece, this will ask each of them a different block within that piece.
        It will continue sending `REQUEST` until all blocks are requested,
        or if all peers hit the maximum number of concurrent requests.
        """

        connections = self.__get_connections()
        self.__setup_piece_buffers(connections)
        scheduled = True
        while scheduled and not self.__stopped:
            scheduled = False
            # Give each eligible peer one block per round before filling its
            # request window, so a small set of active pieces is shared fairly.
            for conn in connections:
                if self.__stopped:
                    return
                if not conn.is_connected:
                    continue

                request_count = sum(
                    conn in owners for owners in self.__requests.values()
                )
                if request_count >= self.__config.download_requests_per_peer:
                    continue
                block = self.__next_request(conn)
                if block is None:
                    continue

                # Register the block before sending.
                owners = self.__requests.get(block)
                if owners is None:
                    owners = {}
                    self.__requests[block] = owners
                owners[conn] = self.__clock()

                if await self.__send(conn, peerutils.get_request_msg(*block)):
                    scheduled = True

    async def __finish_pieces(self):
        """Finalize the downloading of pieces.
        This method should be called routinely.

        It will:
        - Clear finished piece buffers.
        - Wait for all blocks within a piece to arrive, then write it to disk.
        """

        for index, piece in list(self.__pieces.items()):
            if self.__stopped:
                return

            # If we're done with this piece, discard it.
            if self.__state.bitmap & (1 << index):
                self.__discard_piece(index)
                continue

            block_count = len(range(0, len(piece.data), self.__config.block_size))

            # We haven't received all blocks for the piece.
            if len(piece.received) != block_count:
                continue

            # We received all blocks, time to write to disk.
            is_hash_match = await diskio.run_io(self.io.commit_piece, index, piece.data)
            # This is after we receive the block, which remove it from self.requests
            # So we just need to delete this.
            del self.__pieces[index]

            if is_hash_match:
                if not self.__stopped:
                    await self.__mark_verified(index)
            else:
                LOGGER.warning("Piece %d failed SHA-256 verification", index)
                for conn in piece.contributors:
                    await conn.close()
                    self.disconnect_from(conn)
