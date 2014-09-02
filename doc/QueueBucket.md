# Queue buckets
## Client connection
Should make sense to use websockets or long polling. Fall-back to polling is
common in websocket libraries.

## Using Kafka
- http://kafka.apache.org/documentation.html
- https://github.com/SOHU-Co/kafka-node

### for queuing
Goal: deliver each message at least once while avoiding duplicate processing.

Kafka splits a topic into many partitions.
- Each partition's *commit* offset is stored in zookeeper
- Reading from a partition is separate from committing the offset. Client
  keeps track of read offset
- Offset can be *committed* once acks are received by service clients
  (`auto.commit.enable = false`)
- Clients rebalance partitions on join; need more (~2x?) partitions than clients

#### Error / timeout handling
- Each message has an associated timeout
    - configured per queue
    - can be increased by client up to 12 hours (AWS) in poll request or per
      message ('heartbeat')
- If no ack is received within this timeout window:
    - message is placed in internal queue & handed out again *once*; messages
      are only committed up to the failed message
    - if retry succeeds, all is well
    - else: place message(s) into retry queue, ack primary queue
        - dequeue messages from retry queue as well, keep counter of retries
        - once max number of retries reached, place message in separate
          dead-letter queue for inspection (ideally with some debugging info)

### for pub/sub
Goal: Deliver each message to all consumers.

- use one consumer group id per pub/sub client
- each consumer group maintains its own commit offset, so speed independent
- error handling as in queuing

### Open questions
- rebalance periodically based on committed offsets in each partition?

## Similar services
- [Amazon SQS](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/Welcome.html)
- [Microsoft Azure
  Queues](http://azure.microsoft.com/en-us/documentation/articles/storage-nodejs-how-to-use-queues/)
- [Google Task
  Queues](https://developers.google.com/appengine/docs/python/taskqueue/rest/)
