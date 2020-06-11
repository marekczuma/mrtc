import hbase.GeneralCoordinates
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import pipeline.CollisionsStreaming
import org.apache.spark.sql.functions._


object App {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("mrtc-center")
      .master("local")
      .config("hbase.zookeeper.property.clientPort", "2181")
      .config("hbase.zookeeper.quorum", "192.168.100.5")
      .config("spark.hbase.host","192.168.100.5")
      .getOrCreate()
//    spark.sparkContext.setLogLevel("error")
    spark.conf.set("files", "/Users/user/apps/mrtc/center/hbase-site.xml")
    spark.conf.set("hbase.zookeeper.quorum", "192.168.100.5")
    spark.conf.set("hbase.master", "192.168.100.5:16010")
    spark.conf.set("hbase.zookeeper.property.clientPort", "2181")
    import spark.implicits._
    GeneralCoordinates.checkCoordinates(spark)

    val t1 = new Thread(new Runnable {
      def run(): Unit = {
        val collisionsStreaming: CollisionsStreaming = new CollisionsStreaming(spark)
        collisionsStreaming.run()
      }
    })

    val t2 = new Thread(new Runnable {
      def run(): Unit = {
        while (true){
          GeneralCoordinates.checkCoordinates(spark)
          Thread.sleep(60000)
          println("AFTER SLEEP")
        }
      }
    })
//
    t2.start()
    t1.start()

//    new CollisionsStreaming(spark).registerUDFs()
//
//    val general = Seq(("1","1,1","0,1","0,2",System.currentTimeMillis(),System.currentTimeMillis()+ (60000*6)), ("2","1,0","1,1","1,2",System.currentTimeMillis() - 360000,400L), ("3","0,1","0,2","1,1",100L,System.currentTimeMillis()+ 240000))
//      .toDF("id","coords", "step_1","step_2","time_1","time_2")
//      .withColumnRenamed("coords", "step_0")
//      .withColumn("time_0", lit(System.currentTimeMillis() + 120000))
//
//    val plan = Seq(("10","2,3","1,132","1,1")).toDF("stream_id","start_coords","stream_step_1","stream_step_2")
//
//
//    // FIRST STEP
//    val firstStepJoinDF = general.join(plan, plan.col("stream_step_1").equalTo(general.col("step_1"))
//      .or(plan.col("stream_step_1").equalTo(general.col("step_2")))
//      .or(plan.col("stream_step_1").equalTo(general.col("step_0"))), "inner")
//
//    val withWrongTimeDF = firstStepJoinDF.withColumn("wrong_time", callUDF("selectWrongTimesUDF", struct(firstStepJoinDF.columns.map(firstStepJoinDF(_)) : _*),lit(1)))
//    val withWrongTimesDF = withWrongTimeDF.groupBy("stream_id").agg(collect_list("wrong_time").alias("wrong_times")).withColumnRenamed("stream_id", "stream_id2")
//
//    val withWrongTimesAtPlan = plan.join(withWrongTimesDF, withWrongTimesDF.col("stream_id2").equalTo(plan.col("stream_id")), "left_outer")
//    val withFirstTimeDF = withWrongTimesAtPlan.withColumn("timeout_1", callUDF("specifyGoodTimeUDF", struct(withWrongTimesAtPlan.columns.map(withWrongTimesAtPlan(_)) : _*),lit(1)))
//        .select("stream_id2", "timeout_1")
//    val finalFirstDF = plan.join(withFirstTimeDF, plan.col("stream_id").equalTo(withFirstTimeDF.col("stream_id2")), "left_outer")
//      .drop("stream_id2")
//      .na
//      .fill(0, Seq("timeout_1"))
//
//    // SECOND STEP
//    val secondStepJoinDF = general.join(finalFirstDF, finalFirstDF.col("stream_step_2").equalTo(general.col("step_1"))
//      .or(finalFirstDF.col("stream_step_2").equalTo(general.col("step_2")))
//      .or(plan.col("stream_step_1").equalTo(general.col("step_0"))), "inner")
//    val secondWithWrongTimeDF = secondStepJoinDF.withColumn("wrong_time", callUDF("selectWrongTimesUDF", struct(secondStepJoinDF.columns.map(secondStepJoinDF(_)) : _*),lit(2)))
//    val secondWithWrongTimesDF = secondWithWrongTimeDF.groupBy("stream_id").agg(collect_list("wrong_time").alias("wrong_times")).withColumnRenamed("stream_id", "stream_id2")
//    val secondWithWrongTimesAtPlan = finalFirstDF.join(secondWithWrongTimesDF, secondWithWrongTimesDF.col("stream_id2").equalTo(plan.col("stream_id")), "left_outer")
//    val secondWithTimeDF = secondWithWrongTimesAtPlan.withColumn("timeout_2", callUDF("specifyGoodTimeUDF", struct(secondWithWrongTimesAtPlan.columns.map(secondWithWrongTimesAtPlan(_)) : _*),lit(2)))
//      .select("stream_id2", "timeout_2")
//    val secondFinalDF = finalFirstDF.join(secondWithTimeDF, finalFirstDF.col("stream_id").equalTo(secondWithTimeDF.col("stream_id2")), "left_outer")
//      .drop("stream_id2")
//      .na
//      .fill(0, Seq("timeout_2"))
//
//    secondFinalDF.show(false)










//    import spark.implicits._
//
//    val kafkaTestDF = spark.readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("subscribe", "test")
//      .load()
//
//    kafkaTestDF.selectExpr("CAST(key as STRING)", "CAST(value AS STRING)")
//      .as[(String, String)]
//
////    val lines = spark.readStream
////        .format("socket")
////        .option("host", "localhost")
////        .option("port", 9999)
////        .load()
//
//    val words = kafkaTestDF.selectExpr("CAST(value AS STRING)")
//      .as[String]
//
//    val generateUuid = udf(() => java.util.UUID.randomUUID.toString())
//
////    val wordsCount = words.groupBy("value").count()
//    val wordsCount = words.select("value").withColumn("uniqueId", generateUuid())
//
//    val query = wordsCount.writeStream
//        .outputMode("append")
//        .format("console")
//        .start()
//
//    query.awaitTermination()
    print("fhfhtfhg")
  }

}

