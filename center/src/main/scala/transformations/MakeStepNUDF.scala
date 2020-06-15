package transformations

import org.apache.spark.sql.api.java.UDF2

class MakeStepNUDF extends UDF2[String, String, String]{

  override def call(startCoords: String, directions: String): String = {
    val tmpCoord: Array[Int] = startCoords.split(",").map(c=>c.toInt)
    for(d<- directions.split("")) yield {
      d.toUpperCase match {
        case "N" => tmpCoord(1) += 1
        case "S" => tmpCoord(1) -= 1
        case "E" => tmpCoord(0) += 1
        case "W" => tmpCoord(0) -= 1
      }
      s"${tmpCoord(0)},${tmpCoord(1)}"
    }
  }.mkString(";")
}
