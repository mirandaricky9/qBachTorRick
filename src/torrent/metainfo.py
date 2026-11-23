# metainfo.py 
# Contains the infrastructure to support creating torrents
import dataclasses
import hashlib
import time
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

    @classmethod
    def from_dict(cls, d: dict):
        '''
        undoes to_dict()
        '''
        name = d[b"name"].decode("utf-8")
        raw = d[b"pieces"]

        # Using 32 bytes for SHA-256
        pieces = tuple(raw[i:i + 32] for i in range(0, len(raw), 32))

        if b"length" in d:
            single_file = True
            files = (FileEntry((name,), d[b"length"]),)
        else:
            single_file = False
            files = tuple(
                FileEntry(tuple(p.decode("utf-8") for p in f[b"path"]), f[b"length"]) for f in d[b"files"]
            )
        return cls(name, d[b"piece length"], pieces, files, single_file, d.get(b"private", 0) == 1)


    def info_hash(self) -> bytes:
        return hashlib.sha256(bencode.encode(self.to_dict())).digest()

    @property
    def total_length(self) -> int:
        return sum(e.length for e in self.files)

    @property
    def num_pieces(self) -> int:
        return len(self.pieces)

    def piece_size(self, index: int) -> int:
        '''
        All pieces are identical sizes except the last piece.
        
        returns the size of the last piece'''
        if index < 0 or index >= len(self.pieces):
            raise IndexError("Index is out of range of files")

        # if index is the last piece, compute and return last piece length,
        # else return piece_length
        if index == len(self.pieces) - 1:
            return self.total_length - self.piece_length * index
        else:
            return self.piece_length




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

        # helper function, hashes pieces across file boundaries
        def _hash_pieces(files, piece_length):
            pieces, buffer = [], bytearray()
            for file_path in files:
                with open(file_path, "rb") as f:
                    while chunk := f.read(piece_length - len(buffer)):
                        buffer += chunk
                        if len(buffer) == piece_length:
                            pieces.append(hashlib.sha256(buffer).digest())
                            buffer.clear()
            if buffer:
                pieces.append(hashlib.sha256(buffer).digest())
            return tuple(pieces)

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
            for p in root.rglob("*"):
                rel_parts = p.relative_to(root).parts
                if p.is_file() and not any(part.startswith(".") for part in rel_parts):
                    files.append(p)
            files.sort()

            entries = tuple(FileEntry(p.relative_to(root).parts, p.stat().st_size) for p in files)
            
        else:
            raise FileNotFoundError(root)

        # using a piece length of 16 KiB
        if piece_length is None:
            piece_length = 2**14

        if sum(e.length for e in entries) == 0:
            raise ValueError("Cannot create a torrent with no data (0 bytes)")

        # takes name, piece_length, and files from file_entry
        info_dict = InfoDict(name=name,piece_length=piece_length, pieces=_hash_pieces(files, piece_length), files=entries, single_file=single_file,private=private)

        torrent = cls(info=info_dict, announce_list=[[url] for url in trackers], nodes=list(nodes), creation_date=int(clock()))

        return torrent

    def to_dict(self) -> dict:

        d = {"info": self.info.to_dict(),}

        if self.nodes:
            d["nodes"] = self.nodes

        if self.announce_list:
            d["announce-list"] = self.announce_list

        if self.creation_date is not None:
            d["creation date"] = self.creation_date

        if self.comment is not None:
            d["comment"] = self.comment

        if self.created_by is not None:
            d["created by"] = self.created_by

        return d
    
    def encode(self) -> bytes:
        return bencode.encode(self.to_dict())

    @classmethod
    def from_bytes(cls, data: bytes) -> "Torrent":
        d = bencode.decode(data)
        return cls(
            info=InfoDict.from_dict(d[b"info"]),
            announce_list=[[u.decode("utf-8") for u in tier] for tier in d.get(b"announce-list", [])],
            nodes=[(ip.decode("utf-8"), port) for ip, port in d.get(b"nodes", [])],
            creation_date=d.get(b"creation date"),
            comment=d[b"comment"].decode("utf-8") if b"comment" in d else None,
            created_by=d[b"created by"].decode("utf-8") if b"created by" in d else None,
        )

    def save(self, path):
        Path(path).expanduser().write_bytes(self.encode())

    @classmethod
    def load(cls, path) -> "Torrent":
        return cls.from_bytes(Path(path).expanduser().read_bytes())

    @property
    def swarm_id(self) -> int:
        return int.from_bytes(self.info.info_hash(), "big")



if __name__ == "__main__":
    # Run this via `python -m src.torrent.metainfo` in main directory
    # Ensure the path variables are created on your machine
    # Test cases #1 
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

    # Test case #2 
    path2 = '~/torrent_tests/'
    t = Torrent.from_path(path2)
    t.save(path2 + "x.torrent")
    assert Torrent.load(path2 + "x.torrent") == t
    print("pieces:", len(t.info.pieces))
    print("files:", [e.path for e in t.info.files])
    print("swarm_id:", t.swarm_id.to_bytes(32, "big").hex())
    print("round trip OK")
