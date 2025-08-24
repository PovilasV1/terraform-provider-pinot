// internal/provider/table_resource_test.go
package provider

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

func TestAccPinotTable_basic(t *testing.T) {
	rName := acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckPinotTableDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccPinotTableConfig_basic(rName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPinotTableExists("pinot_table.test"),
					resource.TestCheckResourceAttr("pinot_table.test", "table_name", rName),
					resource.TestCheckResourceAttr("pinot_table.test", "table_type", "OFFLINE"),
					resource.TestCheckResourceAttrSet("pinot_table.test", "table_config"),
				),
			},
			{
				ResourceName:      "pinot_table.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func TestAccPinotTable_realtime(t *testing.T) {
	rName := acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckPinotTableDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccPinotTableConfig_realtime(rName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPinotTableExists("pinot_table.test"),
					resource.TestCheckResourceAttr("pinot_table.test", "table_type", "REALTIME"),
				),
			},
		},
	})
}

func testAccPinotTableConfig_basic(name string) string {
	return fmt.Sprintf(`
resource "pinot_schema" "test" {
  schema = jsonencode({
    schemaName = "%[1]s"
    dimensionFieldSpecs = [
      {
        name     = "userId"
        dataType = "STRING"
      }
    ]
    metricFieldSpecs = [
      {
        name     = "count"
        dataType = "LONG"
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

resource "pinot_table" "test" {
  table_name  = "%[1]s"
  table_type  = "OFFLINE"

  table_config = jsonencode({
    tableName = "%[1]s_OFFLINE"
    tableType = "OFFLINE"
    segmentsConfig = {
      timeColumnName = "timestamp"
      timeType       = "MILLISECONDS"
      schemaName     = "%[1]s"
      replication    = "1"
    }
    tenants = {
      broker = "DefaultTenant"
      server = "DefaultTenant"
    }
    tableIndexConfig = {
      loadMode = "MMAP"
    }
    metadata = {}
  })
}
`, name)
}

func testAccPinotTableConfig_realtime(name string) string {
	return fmt.Sprintf(`
resource "pinot_schema" "test" {
  schema = jsonencode({
    schemaName = "%[1]s"
    dimensionFieldSpecs = [
      {
        name     = "userId"
        dataType = "STRING"
      }
    ]
    metricFieldSpecs = [
      {
        name     = "count"
        dataType = "LONG"
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

resource "pinot_table" "test" {
  table_name  = "%[1]s"
  table_type  = "REALTIME"

  table_config = jsonencode({
    tableName = "%[1]s_REALTIME"
    tableType = "REALTIME"
    segmentsConfig = {
      timeColumnName       = "timestamp"
      timeType            = "MILLISECONDS"
      schemaName          = "%[1]s"
      replication         = "1"
      replicasPerPartition = "1"
    }
    tenants = {
      broker = "DefaultTenant"
      server = "DefaultTenant"
    }
    tableIndexConfig = {
      loadMode = "MMAP"
      streamConfigs = {
        "streamType"                        = "kafka"
        "stream.kafka.consumer.type"        = "simple"
        "stream.kafka.topic.name"           = "test-topic"
        "stream.kafka.decoder.class.name"   = "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder"
        "stream.kafka.broker.list"          = "localhost:9092"
        "realtime.segment.flush.threshold.rows" = "10000"
      }
    }
    metadata = {}
  })
}
`, name)
}

func testAccCheckPinotTableExists(resourceName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[resourceName]
		if !ok {
			return fmt.Errorf("Not found: %s", resourceName)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No Table ID is set")
		}

		// Verify table exists via API
		client := testAccProvider.Meta().(*client.PinotClient)
		_, err := client.GetTable(context.Background(), rs.Primary.ID)

		return err
	}
}

func testAccCheckPinotTableDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*client.PinotClient)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "pinot_table" {
			continue
		}

		_, err := client.GetTable(context.Background(), rs.Primary.ID)
		if err == nil {
			return fmt.Errorf("Table still exists: %s", rs.Primary.ID)
		}
	}

	return nil
}
