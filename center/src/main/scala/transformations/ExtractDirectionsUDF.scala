package transformations

import org.apache.spark.sql.api.java.UDF1

class ExtractDirectionsUDF extends UDF1[String, String]{

  override def call(directionsInString: String): String = {
    directionsInString.split("-")(2)
  }

}
