# `dorm.contrib.outbox_cdc`

Publicadores acoplados a broker para `OutboxRelay`. Handlers
drop-in que reenvían eventos outbox a Kafka, NATS, Redis Streams
o un logger.

## API

::: dorm.contrib.outbox_cdc.LoggingPublisher
::: dorm.contrib.outbox_cdc.KafkaPublisher
::: dorm.contrib.outbox_cdc.NatsPublisher
::: dorm.contrib.outbox_cdc.RedisStreamPublisher
