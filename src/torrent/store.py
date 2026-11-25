import dataclasses
import pathlib
import tempfile

from src.torrent import availability, metainfo

PROJECT_DIR = pathlib.Path(__file__).resolve().parents[2]
TORRENTS_DIR = PROJECT_DIR / "torrents"
DATA_DIR = PROJECT_DIR / "data"


@dataclasses.dataclass
class TorrentRecord:
    """A record containing the torrent info, the path to the torrent's content, and its pieces availability."""

    torrent: metainfo.Torrent
    data_path: pathlib.Path
    availability: availability.TorrentAvailability


def get_torrent_paths() -> list[pathlib.Path]:
    """Return paths to existing torrent files."""

    TORRENTS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return sorted(TORRENTS_DIR.glob("*.torrent"))


def prepare_torrent(
    torrent: metainfo.Torrent,
    data_path: str | pathlib.Path | None = None,
    persist: bool = True,
) -> TorrentRecord:
    """Return a `TorrentRecord` and optionally persist the torrent file on disk.

    Parameters
    ----------
    torrent : Torrent
        The torrent.
    data_path : str | Path, optional
        The path to the torrent's content. By default, it'll look at the torrent's name for this.
    persist : bool, default True
        Whether to save a copy of the torrent at the project directory.
        This is used so once the user open a torrent, it'll be saved for next startup.

    Returns
    -------
    TorrentRecord
    """

    if data_path is None:
        name = torrent.info.name
        if name in ("", ".", "..") or pathlib.Path(name).name != name:
            raise ValueError("Torrent name must be a single path component")

        root = DATA_DIR.resolve()
        data_path = (root / name).resolve()
        if not data_path.is_relative_to(root):
            raise ValueError("Torrent data path escapes the data directory")
    else:
        data_path = pathlib.Path(data_path).expanduser().resolve()

    bitmap = availability.scan_pieces(torrent.info, data_path)
    if persist:
        _save_torrent(torrent)
    state = availability.TorrentAvailability(len(torrent.info.pieces), bitmap)
    return TorrentRecord(torrent, data_path, state)


def _save_torrent(torrent: metainfo.Torrent):
    """Save a copy of the torrent at the project directory."""

    TORRENTS_DIR.mkdir(parents=True, exist_ok=True)
    destination = TORRENTS_DIR / f"{torrent.swarm_id:064x}.torrent"

    # Make sure there's no partial torrent file.
    with tempfile.TemporaryDirectory(dir=TORRENTS_DIR) as directory:
        temporary = pathlib.Path(directory) / "metadata"
        torrent.save(temporary)
        temporary.replace(destination)
