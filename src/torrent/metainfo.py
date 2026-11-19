# metainfo.py 
# Contains the infrastructure to support creating torrents
import dataclasses
import time
import hashlib
from pathlib import Path

from src.torrent import bencode

'''
FileEntry class:
    - represents the path to file/folder as a tuple, parsed by Path from pathlib
    - length represents the file's size in bytes
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
    single_file: bool
    private: bool = False

    def to_dict(self) -> dict:
        '''
        represents the to_dict function for the InfoDict class
        '''
        d = {"name": self.name, "piece length": self.piece_length, "pieces": b"".join(self.pieces),}

        if self.single_file:
            d["length"] = self.files[0].length
        else:
            d["files"] = [{"length": e.length, "path": list(e.path)} for e in self.files]

        if self.private:
            d["private"] = 1

        return d

    def info_hash(self) -> bytes:
        return hashlib.sha256(bencode.encode(self.to_dict())).digest()




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
    from_path
    parameters:
    cls - 
    path - the path to the file/folder given as a string 
        -
    '''
    @classmethod
    def from_path(cls, path, *, piece_length=None, trackers=(), nodes=(), private=False, clock=time.time) -> "Torrent":
        root = Path(path).expanduser()
        files = []

        # need to handle single file vs a folder with files
        # Check by look at root
        if root.is_file():
            single_file = True
            name = root.name
            files = [root]
            entries = (FileEntry((name,), root.stat().st_size), )
        elif root.is_dir():
            single_file = False
            name = root.name

            # rglob recursively finds a pattern in the current directory and subdirectories.
            # Since we are torrenting either a
            for p in root.rglob("*"):
                rel_parts = p.relative_to(root).parts
                if p.is_file() and not any(part.startswith(".") for part in rel_parts):
                    files.append(p)
            files.sort()

            entries = tuple(FileEntry(p.relative_to(root).parts, p.stat().st_size) for p in files)
        else:
            raise FileNotFoundError(root)

        

        # takes name, piece_length, and files from file_entry
        info_dict = InfoDict()
        torrent = Torrent()
        return torrent



# Test cases below: To be deleted
path1 = '~/torrent_tests/test1'
root = Path(path1).expanduser()

files = []
for p in root.rglob("*"):
    rel_parts = p.relative_to(root).parts
    print(rel_parts)
    if p.is_file() and not any(part.startswith(".") for part in rel_parts):
        files.append(p)
files.sort()

print(root)
print(files)