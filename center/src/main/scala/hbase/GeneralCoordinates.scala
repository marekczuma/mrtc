package hbase

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object GeneralCoordinates {
  var coordinates: Dataset[Row] = null

  def checkCoordinates(spark: SparkSession): Unit ={
    coordinates = spark
      .read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .option("hbase.config.resources", "/Users/user/apps/mrtc/center/hbase-site.xml")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    coordinates.show()
  }

  def catalog =
    s"""{
       |"table":{"namespace":"mrtc", "name":"coordinates"},
       |"rowkey":"key",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"key", "type":"string"},
       |"coords":{"cf":"info", "col":"coords", "type":"string"},
       |"step_1":{"cf":"info", "col":"step_1", "type":"string"},
       |"step_2":{"cf":"info", "col":"step_2", "type":"string"},
       |"step_3":{"cf":"info", "col":"step_3", "type":"string"},
       |"step_4":{"cf":"info", "col":"step_4", "type":"string"},
       |"step_5":{"cf":"info", "col":"step_5", "type":"string"},
       |"time_1":{"cf":"info", "col":"time_1", "type":"long"},
       |"time_2":{"cf":"info", "col":"time_2", "type":"long"},
       |"time_3":{"cf":"info", "col":"time_3", "type":"long"},
       |"time_4":{"cf":"info", "col":"time_4", "type":"long"},
       |"time_5":{"cf":"info", "col":"time_5", "type":"long"}
       |}
       |}""".stripMargin

}
