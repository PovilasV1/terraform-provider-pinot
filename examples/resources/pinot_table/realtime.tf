# examples/resources/pinot_table/realtime.tf
resource "pinot_table" "events_realtime" {
  table_name  = "user_events"
  table_type  = "REALTIME"

  table_config = jsonencode({
    tableName = "user_events_REALTIME"
    tableType = "REALTIME"

    segmentsConfig = {
      timeColumnName       = "timestamp"
      timeType            = "MILLISECONDS"
      schemaName          = "user_events"
      replication         = "2"
      replicasPerPartition = "1"
      retentionTimeUnit   = "DAYS"
      retentionTimeValue  = "7"
    }

    tenants = {
      broker = "DefaultTenant"
      server = "DefaultTenant"
    }

    tableIndexConfig = {
      loadMode = "MMAP"
      invertedIndexColumns = ["userId", "eventType"]

      streamConfigs = {
        "streamType" = "kafka"
        "stream.kafka.consumer.type" = "lowLevel"
        "stream.kafka.topic.name" = "user-events"
        "stream.kafka.decoder.class.name" = "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder"
        "stream.kafka.consumer.factory.class.name" = "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory"
        "stream.kafka.broker.list" = "kafka-broker-1:9092,kafka-broker-2:9092"
        "stream.kafka.zk.broker.url" = "zookeeper:2181"

        "realtime.segment.flush.threshold.rows" = "100000"
        "realtime.segment.flush.threshold.time" = "1h"
        "stream.kafka.consumer.prop.auto.offset.reset" = "smallest"
      }
    }

    metadata = {}
  })
}
