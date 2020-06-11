package transformations

import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF1

import scala.collection.mutable.ListBuffer

class FinalDataUDF extends UDF1[Row,String]{

  val timesList = ListBuffer("timeout_1", "timeout_2", "timeout_3", "timeout_4", "timeout_5")

  override def call(row: Row): String = {
    val roverID: String = row.getAs[String]("stream_id")
    val times: String = timesList.map(t => row.getAs[Int](t).toString)
      .mkString(",")
    s"${roverID}-${times}"
  }

}
