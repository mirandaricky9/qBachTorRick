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

Example request:

```json
{
  "version": 1,
  "type": "KEEP_ALIVE"
}
```

There are no response for this request. A peer decides a connection is alive when it *receives* a `KEEP_ALIVE` (or any other valid messages) within the interval defined in the config. *Just because a send succeeds doesn't mean the connection is alive*.
