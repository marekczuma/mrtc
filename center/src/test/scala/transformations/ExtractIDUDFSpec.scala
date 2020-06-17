package transformations

import org.scalatest.FlatSpec

class ExtractIDUDFSpec extends FlatSpec{

  "An UDF" should "extract id = 10 from '10-0,0-NWNNN'" in {
    val extractIDUDF = new ExtractIDUDF()
    val id = extractIDUDF.call("10-0,0-NWNNN")
    assert(id.equals("10"))
  }

}
