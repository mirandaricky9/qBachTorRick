"""A module for reading and writing torrent contents."""

import asyncio
import hashlib
import pathlib

from src.torrent import availability, store


async def run_io(function, *args):
    """Wait for filesystem work to finish even if its caller is cancelled."""

    worker = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        # CANCEL may be followed by connection shutdown. Repeated cancellation
        # must not detach the thread from the task that owns its filesystem I/O.
        while not worker.done():
            try:
                await asyncio.shield(worker)
            except asyncio.CancelledError:
                continue
            except (OSError, ValueError):
                break
        worker.exception()
        raise


class TorrentIO:
    """Handle reading and writing torrent contents onto disk."""

    def __init__(self, record: store.TorrentRecord):
        self.__record = record
        self.__info = record.torrent.info
        self.__total_length = sum(entry.length for entry in self.__info.files)

    def piece_size(self, index: int):
        """Return a piece's size within the torrent.

        This method can identify the very last piece's size in the torrent.
        """

        if not 0 <= index < len(self.__info.pieces):
            raise ValueError("Piece index is outside the torrent")

        return min(
            self.__info.piece_length,
            self.__total_length - index * self.__info.piece_length,
        )

    def validate_block(self, index: int, begin: int, length: int):
        size = self.piece_size(index)
        if begin < 0:
            raise ValueError("Block offset must be a non-negative integer")
        if not 0 < length <= 16384:
            raise ValueError("Block length must be between 1 and 16384 bytes")
        if begin + length > size:
            raise ValueError("Block extends beyond the piece")

    def prepare(self):
        """Create zero-byte files declared in the torrent."""

        paths = availability.get_data_paths(self.__info, self.__record.data_path)
        for entry, path in zip(self.__info.files, paths):
            if entry.length == 0 and not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("ab"):
                    pass

    def read_block(self, index: int, begin: int, length: int):
        """Read bytes starting from an offset within a piece.
        This request may span through multiple files.

        Parameters
        ----------
        index : int
            Which piece do we start reading.
        begin : int
            The offset within the piece where we actually start reading.
        length: int
            How many bytes to read.
        """

        self.validate_block(index, begin, length)
        data = bytearray()
        offset = index * self.__info.piece_length + begin
        for path, file_offset, count in self.__segments(offset, length):
            with path.open("rb") as stream:
                stream.seek(file_offset)
                block = stream.read(count)
                if len(block) != count:
                    raise OSError("Verified torrent content is truncated")
                data.extend(block)
        return bytes(data)

    def commit_piece(self, index: int, data: bytearray):
        """Write a piece into disk.

        Returns
        -------
        bool
            Whether the hash of data matches the piece hash.
            If False, no data has been written.

        Raises
        ------
        ValueError
            If the size of data is different than the piece expected size.
        """

        if len(data) != self.piece_size(index):
            raise ValueError("Incomplete piece")
        if hashlib.sha256(data).digest() != self.__info.pieces[index]:
            return False

        offset = index * self.__info.piece_length
        consumed = 0
        for path, file_offset, count in self.__segments(offset, len(data)):
            path.parent.mkdir(parents=True, exist_ok=True)
            try:
                stream = path.open("r+b")
            except FileNotFoundError:
                stream = path.open("w+b")

            with stream:
                stream.seek(file_offset)
                stream.write(memoryview(data)[consumed : consumed + count])
            consumed += count
        return True

    def __segments(self, offset: int, length: int):
        """Given an offset and the requested number of bytes to read,
        return a list of tuples containing which files to read, where to start reading,
        and how many bytes to read from each file.

        This converts a byte range into ranges within individual files.
        For example, if you want to iterate over a torrent piece with offset 80,
        piece length 50, with two files in the torrent of 100 bytes each,
        then this returns [(file A, 80, 20), (file B, 0, 30)].

        Parameters
        ----------
        offset : int
            The byte offset from the first torrent content.
        length : int
            The total number of bytes in the requested range.
        """

        segments: list[tuple[pathlib.Path, int, int]] = []
        paths = availability.get_data_paths(self.__info, self.__record.data_path)
        for entry, path in zip(self.__info.files, paths):
            if offset >= entry.length:
                offset -= entry.length
                continue

            # Read the specified length or the remaining bytes within a file.
            count = min(length, entry.length - offset)
            segments.append((path, offset, count))

            length -= count
            if length == 0:
                break
            # We pass file boundary, so offset is 0.
            offset = 0
        return segments
