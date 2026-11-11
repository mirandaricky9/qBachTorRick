# metainfo.py 
# Contains the infrastructure to support creating torrents
import dataclasses
import time

'''
FileEntry class:
    - represents the path to file/folder
    - Freezing it so that it cannot be modified after creation, necessary for hashing as a new hash
    means a new object which means a new torrent
'''
@dataclasses.dataclass(frozen=True)
class FileEntry:
    path: tuple[str, ...]
    length: int

'''
InfoDict class:
    - represents the info dictionary
    - This object will be hashed to return the infohash which can be used to verify file integrity
    - Same reason as above for freezing.

    Parameters: 
'''
@dataclasses.dataclass(frozen=True)
class InfoDict:
    name: str
    piece_length: int
    pieces: tuple[bytes, ...]
    files: tuple[FileEntry, ...]



'''
Torrent class:
    - represents the Torrent which contains the metadata that a typical torrent file would contain
    - 
    - 

'''
@dataclasses.dataclass
class Torrent:
    info: InfoDict
    announce_list: list[list[str]] = dataclasses.field(default_factory=list)
    nodes: list[tuple[str, int]]   = dataclasses.field(default_factory=list)
    creation_date: int | None = None
    comment: str | None = None
    created_by: str | None = None

    '''
    '''
    @classmethod
    def from_path(cls, path, *, piece_length=None, trackers=(), nodes=(), private=False, clock=time.time) -> "Torrent":

        # takes a path and length
        file_entry = FileEntry()

        # takes name, piece_length, and files from file_entry
        info_dict = InfoDict()
        torrent = Torrent()
        return torrent
