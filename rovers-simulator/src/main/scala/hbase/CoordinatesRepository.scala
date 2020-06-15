package hbase

import java.io.File
import java.util.Calendar
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
 * Connecting with "coordinates" table on HBase.
 * We need to pass hbase-site.xml
 */
object CoordinatesRepository {
  val path: String = "hbase-site.xml"
  val c: Configuration = HBaseConfiguration.create()
  val connection: Connection = createConnection()
  val table: Table = connection.getTable(TableName.valueOf("mrtc:coordinates"))

  /**
   *
   * @param rowkey - key in HBase
   * @param data - key-value. E.G. "coords"->"10,12"; "step-1"->"10,13"
   */
  def putRowToTable(rowkey: String, data: Map[String, String]): Unit ={
    val p: Put = new Put(Bytes.toBytes(rowkey))
    data.foreach{
      case(column, value) => p.addColumn(Bytes.toBytes("info"), Bytes.toBytes(column), Bytes.toBytes(value))
    }
    println(s"${Calendar.getInstance().getTime} BEFORE PUT TO HBASE")
    table.put(p)
  }

  def createConnection(): Connection={
    c.addResource(new File(path).toURI.toURL)
    ConnectionFactory.createConnection(c)
  }

}
