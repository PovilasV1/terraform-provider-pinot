# examples/resources/pinot_table/offline.tf
resource "pinot_schema" "events" {

  schema = jsonencode({
    schemaName = "user_events"
    enableColumnBasedNullHandling = true

    dimensionFieldSpecs = [
      {
        name     = "userId"
        dataType = "STRING"
      },
      {
        name     = "eventType"
        dataType = "STRING"
      },
      {
        name             = "tags"
        dataType         = "STRING"
        singleValueField = false
      }
    ]

    metricFieldSpecs = [
      {
        name     = "count"
        dataType = "LONG"
        defaultNullValue = 0
      },
      {
        name     = "revenue"
        dataType = "DOUBLE"
      }
    ]

    dateTimeFieldSpecs = [
      {
        name        = "timestamp"
        dataType    = "LONG"
        format      = "1:MILLISECONDS:EPOCH"
        granularity = "1:HOURS"
      }
    ]
  })
}

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
