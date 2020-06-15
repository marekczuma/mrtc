package transformations

import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF2

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class SpecifyGoodTimeUDF extends UDF2[Row, Int, Int]{

  override def call(row: Row, stepNumber: Int): Int= {
    try{
      val wrongTimes: List[Long] = row.getAs[ArrayBuffer[ListBuffer[Long]]]("wrong_times")
        .flatten
        .toList
        .sorted

      var isOK: Boolean = false
      var currentTimeout: Int = 0
      var currentTime: Long = System.currentTimeMillis()
      if(stepNumber!=1){
        currentTimeout = row.getAs[Int](s"timeout_${stepNumber-1}")
        currentTime = System.currentTimeMillis() + 120000*(stepNumber-1) + currentTimeout*60000
      }

      while(!isOK){
        isOK = true
        wrongTimes.foreach(wt=>{
          if((wt - 60000 to wt + 60000) contains(currentTime + 120000)){
            currentTimeout += 2
            currentTime += 120000
            isOK = false
          }
        })
      }
      if(stepNumber!=1){
        return currentTimeout - row.getAs[Int](s"timeout_${stepNumber-1}")
      }
      currentTimeout
    }catch {
      case e: NullPointerException=>{
        0
      }
    }
  }
}