# Pinot schema example
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
