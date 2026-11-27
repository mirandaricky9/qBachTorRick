# Peer Communication Protocol

Describe the communication protocol used within peers within a torrent.

## TCP

Peers are communicated via TCP. Similar to DHT communication, `async` model is used for simplicity.

## Peer Connection

A peer connection is made per torrent per peer. For example, if A and B have two torrents, then to exchange these torrents, two peer connections are made (one for each torrent).

Peer connection is a wrapper over TCP connection. Peer connection has its activity interval (defined in `config.PeerConfig`) before it decides to close the underlying TCP connection. Of course, if there are issues with the TCP connection itself, the peer connection also closes.

Throughout this document, the term "connection" will refer to peer connection. TCP connection will be spelled out explicitly.

## Peer Message Structure

All messages have the following structures (see `src/swarm/peerutils.py` for accuracy):

```json
{
  "version": 1,
  "type": "MESSAGE_TYPE"
}
```

See [Peer Messages](#peer-messages) for supported message types. Additional fields are required depending on `type`.

## Piece Transfer

Detecting available pieces is done via `BITFIELD` messages. From here, a peer can use `REQUEST` messages to ask for part of the piece (called "blocks"), to which a `PIECE` message will carry that requested data. When a piece is completed and verified against the piece hash locally, the peer will send `HAVE` to all connected peers on the swarm, indicating it has the piece.

## Peer Messages

### HANDSHAKE

Establish a connection to a peer. If success, the peer will response with a `HANDSHAKE`.

Example request:

```json
{
  "version": 1,
  "type": "HANDSHAKE",
  "swarm_id": "<64-char torrent hex ID>",
  "peer_id": "<64-char peer hex ID>",
  "port": 9001
}
```

`peer_id` and `port` are of the sender.

A valid response will look similar to the request (with the peer's own `peer_id` and `port`).

### KEEP_ALIVE

Check activity with another peer on a connection.

Example message:

```json
{
  "version": 1,
  "type": "KEEP_ALIVE"
}
```

There are no response for this message. A peer decides a connection is alive when it *receives* a `KEEP_ALIVE` (or any other valid messages) within the interval defined in the config. *Just because a send succeeds doesn't mean the connection is alive*.

### PEX

Report changes to established peer connections in the same swarm.

A PEX message is sent only when there are changes since last PEX message. Therefore, if the peer topology (from a peer's perspective) is unchanged, it will not send a PEX message.

Example message:

```json
{
  "version": 1,
  "type": "PEX",
  "added": [["127.0.0.2", 9002]],
  "dropped": [["127.0.0.3", 9003]]
}
```

Both `added` and `dropped` are required, but they can be empty list.

### BITFIELD

Notify the receiver which pieces within the torrent it has.

This message is automatically sent when both these conditions apply:

1. A peer handshake has just completed.
2. You have some pieces available for the torrent.

A `BITFIELD` message is *never* sent after any other messages (aside from `HANDSHAKE`). If it is, there is something wrong and you should terminate the connection. If you don't have any pieces available, the `BITFIELD` message is skipped.

Example message:

```json
{
  "version": 1,
  "type": "BITFIELD",
  "pieces": "QA=="
}
```

`pieces` is a base64-encoded string of the bitmap, with the highest bit is piece 0. The decoded length will be `ceil(piece_count / 8)`. Base64 is used because the bitmap may be very large in Python. For large integers, Python will not allow converting to string.

### HAVE

Announce to other peers in the swarm you have received and verified a piece against the torrent metadata.

Example message:

```json
{
  "version": 1,
  "type": "HAVE",
  "index": 2
}
```

`index` indicates which piece you have received.

### REQUEST and PIECE

Request a block of data using its piece index, byte offset within that piece, and size. The expected response for a `REQUEST` is a `PIECE` message.

Example request:

```json
{
  "version": 1,
  "type": "REQUEST",
  "index": 0,
  "begin": 0,
  "length": 16384
}
```

Example response:

```json
{
  "version": 1,
  "type": "PIECE",
  "index": 0,
  "begin": 0,
  "block": "aGVsbG8="
}
```

`block` is a base64-encoded byte of the requested size.

### CANCEL

Cancel a `REQUEST` message.

Example message:

```json
{
  "version": 1,
  "type": "CANCEL",
  "index": 0,
  "begin": 0,
  "length": 16384
}
```
