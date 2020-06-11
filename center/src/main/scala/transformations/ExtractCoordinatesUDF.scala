package transformations

import org.apache.spark.sql.api.java.UDF1

class ExtractCoordinatesUDF extends UDF1[String, String]{

  override def call(coordinatesInString: String): String= {
    coordinatesInString.split("-")(1)
      .split(",")
      .toList
      .map(c=>c)
      .mkString(",")
  }

}