# Communication protocol

Describe the communication protocol used within DHT. This has nothing to do with peer communication (yet).

## RPC

DHT nodes are communicated using RPC over UDP. For concurrent communications, `async` model is used for simplicity.

## RPC Message Structure

All messages have the following structures (see `src/network/netutils.py` for accuracy):

```json
{
  "version": 1,
  "type": "request, response, or error",
  "txid": "32 lowercase hex characters",
  "sender_id": "ID of the node in hex",
  "method/result/error": "required for request/response/error respectively"
}
```

For requests, `method` is a required field, with `params` is soft-required (many methods require it, other methods may ignore it). For response, `result` is a required field. For error, `error` is a required field. Additional fields are required depending on `method`.

## RPC Responses

A valid response will contain the field `result` along with the same `txid` as the matching request. For the structure of RPC responses, see [RPC Requests](#rpc-requests)

## RPC Requests

See below for structure of a valid RPC request along with available methods.

### PING

Check reachability to another DHT node. `params` may be omitted.

Example request:

```json
{
  "version": 1,
  "type": "request",
  "txid": "<32-char transaction ID>",
  "sender_id": "<64-char node hex ID>",
  "method": "PING",
  "params": {}
}
```

Example response:

```json
{
  "version": 1,
  "type": "response",
  "txid": "same as request",
  "sender_id": "<64-char node hex ID",
  "result": {}
}
```

### FIND_NODE

Ask a DHT node for closer nodes to a target ID.

Example request:

```json
{
  "version": 1,
  "type": "request",
  "txid": "<32-char transaction ID>",
  "sender_id": "<64-char node hex ID>",
  "method": "FIND_NODE",
  "params": { "target_id": "<64-char target ID>" }
}
```

Example response:

```json
{
  "version": 1,
  "type": "response",
  "txid": "same as request",
  "sender_id": "<64-char node hex ID>",
  "result": {
    "nodes": [
      {
        "node_id": "<64-char node hex ID>",
        "ip": "127.0.0.1",
        "port": 6969
      }
    ]
  }
}
```

`nodes` can be an empty list (`[]`).

### ANNOUNCE

Register a peer endpoint under a swarm ID.

Example request:

```json
{
  "version": 1,
  "type": "request",
  "txid": "<32-char transaction ID>",
  "sender_id": "<64-char node hex ID>",
  "method": "ANNOUNCE",
  "params": {
    "swarm_id": "<64-char swarm ID>",
    "port": 16969
  }
}
```

Example response:

```json
{
  "version": 1,
  "type": "response",
  "txid": "same as request",
  "sender_id": "<64-char node hex ID>",
  "result": {
    "ttl": 1800
  }
}
```

### GET_PEERS

Ask a node to return its known peer list for a swarm, or a list of nodes closer to a swarm.

Example request:

```json
{
  "version": 1,
  "type": "request",
  "txid": "<32-char transaction ID>",
  "sender_id": "<64-char node hex ID>",
  "method": "GET_PEERS",
  "params": { "swarm_id": "<64-char target ID>" }
}
```

Example response if the node has the requested peers:

```json
{
  "version": 1,
  "type": "response",
  "txid": "same as request",
  "sender_id": "<64-char node hex ID>",
  "result": {
    "peers": [
      {
        "ip": "127.0.0.1",
        "port": 16881
      }
    ]
  }
}
```

If the node contains no peers, it'll return up to `k` closer DHT nodes, similar to [`FIND_NODE`](#find_node):

```json
{
  "version": 1,
  "type": "response",
  "txid": "same as request",
  "sender_id": "<64-char node hex ID>",
  "result": {
    "nodes": [
      {
        "node_id": "<64-char node hex ID>",
        "ip": "127.0.0.1",
        "port": 6969
      }
    ]
  }
}
```

`nodes` can be an empty list (`[]`).
