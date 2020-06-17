package transformations

import org.scalatest.FlatSpec

class ExtractDirectionsUDFSpec extends FlatSpec{

  "An UDF" should "extract directions = 'NWNNN' from '10-0,0-NWNNN'" in {
    val extractDirectionsUDF = new ExtractDirectionsUDF()
    val directions = extractDirectionsUDF.call("10-0,0-NWNNN")
    assert(directions.equals("NWNNN"))
  }

}
