package transformations

import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF1

import scala.collection.mutable.ListBuffer

class TmpMakeAnswerUDF extends UDF1[String,String]{

  val times = ListBuffer("time-1", "time-2", "time-3", "time-4", "time-5")

  override def call(roverID: String): String = {

    val times: String = (1 to 5).map(n=> (scala.util.Random.nextInt(3)*2).toString).toList.mkString(",")
    s"${roverID}-${times}"
  }

}
