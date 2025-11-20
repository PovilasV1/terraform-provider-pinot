# REALTIME table example
resource "pinot_table" "events_realtime" {
  table_name  = "user_events"
  table_type  = "REALTIME"

  table_config = jsonencode({
  "tableName": "dailySales",
  "tableType": "REALTIME",
  "segmentsConfig": {
    "segmentPushType": "APPEND",
    "segmentAssignmentStrategy": "BalanceNumSegmentAssignmentStrategy",
    "timeColumnName": "daysSinceEpoch",
    "retentionTimeUnit": "DAYS",
    "retentionTimeValue": "50000",
    "replication": "1"
  },
  "tenants": {
  },
  "tableIndexConfig": {
    "loadMode": "MMAP",
    "noDictionaryColumns": [
      "sales_count",
      "total_sales"
    ]
  },
  "ingestionConfig": {
    "streamIngestionConfig": {
      "streamConfigMaps": [
        {
          "streamType": "kafka",
          "stream.kafka.topic.name": "dailySales",
          "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder",
          "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
          "stream.kafka.consumer.prop.auto.offset.reset": "smallest",
          "stream.kafka.zk.broker.url": "localhost:2191/kafka",
          "stream.kafka.broker.list": "localhost:19092",
          "realtime.segment.flush.threshold.time": "3600000",
          "realtime.segment.flush.threshold.size": "50000"
        }
      ]
    },
    "transformConfigs": [
      {
        "columnName": "daysSinceEpoch",
        "transformFunction": "toEpochDays(\"timestamp\")"
      }
    ],
    "aggregationConfigs": [
      {
        "columnName": "total_sales",
        "aggregationFunction": "SUM(price)"
      },
      {
        "columnName": "sales_count",
        "aggregationFunction": "COUNT(*)"
      }
    ]
  },
  "metadata": {
    "customConfigs": {
    }
  }
})
}

# REALTIME table example with SASL authentication
resource "pinot_table" "events_realtime_sasl" {
  table_name   = "user_events_sasl"
  table_config = file("tables/events_realtime_sasl_REALTIME.json")
  table_type   = "REALTIME"

  kafka_username  = "username"
  kafka_password  = "password"
}

# OFFLINE table example
resource "pinot_table" "events_offline" {
  table_name  = "user_events"
  table_type  = "OFFLINE"

  table_config = jsonencode({
  "tableName": "user_events_OFFLINE",
  "tableType": "OFFLINE",
  "segmentsConfig": {
    "timeColumnName": "timestamp",
    "timeType": "MILLISECONDS",
    "replication": "3",
    "retentionTimeUnit": "DAYS",
    "retentionTimeValue": "365",
    "segmentPushType": "APPEND",
    "segmentAssignmentStrategy": "BalanceNumSegmentAssignmentStrategy"
  },
  "tenants": {
    "broker": "DefaultTenant",
    "server": "DefaultTenant"
  },
  "tableIndexConfig": {
    "loadMode": "MMAP",
    "invertedIndexColumns": ["userId", "eventType"],
    "bloomFilterColumns": ["userId"],
    "noDictionaryColumns": ["revenue"],
    "starTreeIndexConfigs": [
      {
        "dimensionsSplitOrder": ["userId", "eventType"],
        "functionColumnPairs": ["COUNT__*", "SUM__revenue"],
        "maxLeafRecords": 100
      }
    ]
  },
  "metadata": {
    "customConfigs": {
      "key1": "value1"
    }
  }
})
}
