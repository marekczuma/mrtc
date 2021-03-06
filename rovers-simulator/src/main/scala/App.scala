import hbase.CoordinatesRepository
import kafka.AnswersStreaming
import services.RoversManager

import scala.collection.mutable


/**
 * Main, start class.
 * 2 Threads. First - for running our streaming. Second - for simulating marsian rovers.
 */
object App {
  def main(args: Array[String]): Unit = {

    val t1 = new Thread(new Runnable {
      def run(): Unit = {
        AnswersStreaming.run()
      }
    })

    val t2 = new Thread(new Runnable {
      def run(): Unit = {
        RoversManager.runTheWorld()
      }
    })

    t1.start()
    t2.start()



  }
}
