package transformations

import org.apache.spark.sql.api.java.UDF1

class TmpMakeAnswerUDF extends UDF1[String,String]{

  override def call(roverID: String): String = {

    val times: String = (1 to 5).map(n=> (scala.util.Random.nextInt(3)*2).toString).toList.mkString(",")
    s"${roverID}-${times}"
  }

}
