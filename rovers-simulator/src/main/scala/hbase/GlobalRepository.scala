package hbase

import java.io.File
import java.util.Calendar

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

object GlobalRepository {
  val path: String = "hbase-site.xml"
  val c = HBaseConfiguration.create()
  val connection = createConnection()
  val table = connection.getTable(TableName.valueOf("mrtc:coordinates"))

  def putRowToTable(rowkey: String, data: Map[String, String]): Unit ={
    val p = new Put(Bytes.toBytes(rowkey))
    data.foreach{
      case(column, value) => {
        p.addColumn(Bytes.toBytes("info"), Bytes.toBytes(column), Bytes.toBytes(value))
      }
    }
    println(s"${Calendar.getInstance().getTime} BEFORE PUT TO HBASE")
    table.put(p)
  }

  def createConnection(): Connection={
    c.addResource(new File(path).toURI.toURL)
    ConnectionFactory.createConnection(c)
  }

}
