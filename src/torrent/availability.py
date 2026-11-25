"""Verify local pieces within a torrent."""

import dataclasses
import hashlib
import pathlib

from src.torrent import metainfo


@dataclasses.dataclass
class TorrentAvailability:
    piece_count: int
    bitmap: int


def bitmap_to_list(bitmap: int, piece_count: int) -> list[int]:
    """Convert a bitmap to a list for debugging purposes."""

    return [(bitmap >> index) & 1 for index in range(piece_count)]


def scan_pieces(info: metainfo.InfoDict, data_path: str | pathlib.Path) -> int:
    """Return a bitmap of available pieces compared to a torrent info.

    This will analyze the file(s) at the given path against the torrent info.
    If there are any missing or short files or incorrect hash, the affected pieces are considered missing.

    The return value should be used in `BITFIELD` peer message.

    Parameters
    ----------
    info : InfoDict
        The torrent info.
    data_path : str | Path
        Path to the file (or to a directory containing files) to be compared against the torrent info.

    Returns
    -------
    int
        A bitmap of available pieces.
    """

    total_length = sum(entry.length for entry in info.files)
    expected_count = (total_length + info.piece_length - 1) // info.piece_length
    if len(info.pieces) != expected_count or any(
        len(digest) != 32 for digest in info.pieces
    ):
        raise ValueError("Piece hashes do not match the declared torrent size")

    root = pathlib.Path(data_path).expanduser().resolve()
    paths: list[pathlib.Path] = []
    if info.single_file:
        paths.append(root)
    else:
        for file_entry in info.files:
            if not file_entry.path or any(
                part in ("", ".", "..")
                or pathlib.Path(part).is_absolute()
                or len(pathlib.Path(part).parts) != 1
                for part in file_entry.path
            ):
                raise ValueError("Unsafe torrent file path")

            path = root.joinpath(*file_entry.path).resolve()
            if not path.is_relative_to(root):
                raise ValueError("Torrent file path escapes the data directory")
            paths.append(path)

    bitmap = 0
    # What piece number we're on.
    piece_index = 0
    # What bytes are we on within a piece.
    # This is necessary because piece can span files boundaries.
    piece_bytes = 0
    # If this is true, we can safely mark a piece as available.
    # This is false when we end up reading less bytes than expected
    # or one of the file straight up missing.
    # In that cases, pieces that involve the missing files are all invalid.
    # Invalid pieces are marked as, same as missing.
    is_piece_valid = True
    digest = hashlib.sha256()
    for file_entry, path in zip(info.files, paths):
        try:
            stream = path.open("rb")
        except FileNotFoundError:
            stream = None

        try:
            remaining_bytes = file_entry.length
            while remaining_bytes > 0:
                chunk_size = min(
                    remaining_bytes,
                    info.piece_length - piece_bytes,
                    65536,  # Arbitrary cap to bound memory usage.
                )
                data = stream.read(chunk_size) if stream is not None else b""
                # File is either missing or shorter than expected somehow.
                # Either way, sth went wrong, declare as missing piece.
                if len(data) != chunk_size:
                    is_piece_valid = False

                digest.update(data)
                remaining_bytes -= chunk_size
                piece_bytes += chunk_size

                # We reach piece boundary.
                if piece_bytes == info.piece_length:
                    # Happy path: read the piece and they match infodict.
                    # We set the bit for that piece to be 1.
                    if is_piece_valid and digest.digest() == info.pieces[piece_index]:
                        bitmap |= 1 << piece_index

                    piece_index += 1
                    piece_bytes = 0
                    is_piece_valid = True
                    digest = hashlib.sha256()
        finally:
            if stream is not None:
                stream.close()

    # Final cleanup on any dangling bytes we've read but have not processed.
    if (
        piece_bytes > 0
        and is_piece_valid
        and digest.digest() == info.pieces[piece_index]
    ):
        bitmap |= 1 << piece_index
    return bitmap
