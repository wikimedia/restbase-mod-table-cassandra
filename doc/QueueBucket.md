# Queue buckets
## Client connection
Should make sense to use websockets or long polling. Fall-back to polling is
common in websocket libraries.

## Using Kafka

### for queuing
Goal: deliver each message at least once while avoiding duplicate processing.

Kafka splits a topic into many partitions.
- each partition maintains its own client offset
- reading from a partition is separate from committing the offset

Each partition should only be read by a single client, so the number of
partitions should be higher (2x?) than the maximum number of clients. On
connection, clients re-balance partitions consumed by each client.

On client connection, the server will make a dedicated connection on the
client's behalf. This allocates partitions which are read only by this client.
The client offset should only be committed when receiving an ack from the
client (`auto.commit.enable = false`). The internal rebalancing process is
transparent to websocket clients.

### for pub/sub
Goal: Deliver each message to all consumers.

- use single Kafka partition -> all clients use the same partition & see same
  messages
- auto offset commit so that new clients start at reasonable offset
- each client connection maintains its own offset, so speed independent

### Opent questions
- node bindings with rebalancing support?
    - https://github.com/pelger/Kafkaesque/ does not seem to do balancing
- rebalance periodically based on committed offsets in each partition?
