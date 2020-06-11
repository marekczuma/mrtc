package transformations

import org.apache.spark.sql.api.java.UDF1

class ExtractIDUDF extends UDF1[String, String]{

  override def call(roverIdInString: String): String = {
    roverIdInString.split("-")(0)
  }

}