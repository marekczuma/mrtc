package transformations

import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.{UDF2}

import scala.collection.mutable.ListBuffer

class SelectWrongTimesUDF extends UDF2[Row, Int, ListBuffer[Long]]{

  val steps = ListBuffer("step_1", "step_2", "step_3", "step_4", "step_5")

  override def call(row: Row, stepNumber: Int): ListBuffer[Long]= {
    val streamStepCoords: String = row.getAs[String](s"stream_step_${stepNumber}")
    val numbersOfSteps: ListBuffer[String] = steps.filter(s=>streamStepCoords.equals(row.getAs[String](s)))
      .map(s=> s.split("_")(1))
    numbersOfSteps.map(n=> row.getAs[Long](s"time_${n}"))
  }

}