import dataclasses
import pathlib
import tempfile

from src.torrent import availability, metainfo


@dataclasses.dataclass
class TorrentRecord:
    """A record containing the torrent info, the path to the torrent's content, and its pieces availability."""

    torrent: metainfo.Torrent
    data_path: pathlib.Path
    availability: availability.TorrentAvailability


def get_torrent_paths(
    torrents_dir: str | pathlib.Path,
    data_dir: str | pathlib.Path,
) -> list[pathlib.Path]:
    """Return paths to existing torrent files."""

    torrents_dir = pathlib.Path(torrents_dir).expanduser().resolve()
    data_dir = pathlib.Path(data_dir).expanduser().resolve()
    torrents_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    return sorted(torrents_dir.glob("*.torrent"))


def prepare_torrent(
    torrent: metainfo.Torrent,
    data_path: str | pathlib.Path | None = None,
    persist: bool = True,
    *,
    torrents_dir: str | pathlib.Path,
    data_dir: str | pathlib.Path,
) -> TorrentRecord:
    """Return a `TorrentRecord` and optionally persist the torrent file on disk.

    Parameters
    ----------
    torrent : Torrent
        The torrent.
    data_path : str | Path, optional
        The path to the torrent's content. Defaults to `data_dir/<torrent.name>`.
    persist : bool, default True
        Whether to save a copy of the torrent in torrents_dir.
        This is used so once the user open a torrent, it'll be saved for next startup.
    torrents_dir : str | Path
        Where to save the torrent metadata.
    data_dir : str | Path
        Where to look for torrent content when `data_path` is omitted.
        This should be the default directory where torrent contents are stored.

    Returns
    -------
    TorrentRecord
    """

    if data_path is None:
        name = torrent.info.name
        if name in ("", ".", "..") or pathlib.Path(name).name != name:
            raise ValueError("Torrent name must be a single path component")

        root = pathlib.Path(data_dir).expanduser().resolve()
        data_path = (root / name).resolve()
        if not data_path.is_relative_to(root):
            raise ValueError("Torrent data path escapes the data directory")
    else:
        data_path = pathlib.Path(data_path).expanduser().resolve()

    bitmap = availability.scan_pieces(torrent.info, data_path)
    if persist:
        _save_torrent(torrent, torrents_dir)
    state = availability.TorrentAvailability(len(torrent.info.pieces), bitmap)
    return TorrentRecord(torrent, data_path, state)


def _save_torrent(torrent: metainfo.Torrent, torrents_dir: str | pathlib.Path):
    """Save a copy of the torrent in the configured metadata directory."""

    torrents_dir = pathlib.Path(torrents_dir).expanduser().resolve()
    torrents_dir.mkdir(parents=True, exist_ok=True)
    destination = torrents_dir / f"{torrent.swarm_id:064x}.torrent"

    # Make sure there's no partial torrent file.
    with tempfile.TemporaryDirectory(dir=torrents_dir) as directory:
        temporary = pathlib.Path(directory) / "metadata"
        torrent.save(temporary)
        temporary.replace(destination)
