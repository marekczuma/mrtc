package transformations

import org.scalatest.FlatSpec

class ExtractCoordinatesUDFSpec extends FlatSpec{

  "An UDF" should "extract coordinates = '0,0' from '10-0,0-NWNNN'" in {
    val extractCoordinatesUDF = new ExtractCoordinatesUDF()
    val coordinates = extractCoordinatesUDF.call("10-0,0-NWNNN")
    assert(coordinates.equals("0,0"))
  }

}
