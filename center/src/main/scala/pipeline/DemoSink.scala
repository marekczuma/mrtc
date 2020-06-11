package pipeline

import java.util.Properties

import hbase.GeneralCoordinates
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.functions.{callUDF, collect_list, lit, struct}
import org.apache.spark.sql.streaming.OutputMode

case class DemoSink(
                     sqlContext: SQLContext,
                     parameters: Map[String, String],
                     partitionColumns: Seq[String],
                     outputMode: OutputMode) extends Sink {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    println(s"addBatch($batchId)")
    val withTimeouts: Dataset[Row] = decideTimeouts(data)
    val finalAnswer: Dataset[Row] = makeFinalAnswer(withTimeouts)
    val values: Dataset[Row] = finalAnswer.select("answer")
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    values.foreachPartition(partition=> {
      val producer = new KafkaProducer[String, String](props)
      partition.foreach(v=> producer.send(new ProducerRecord[String, String]("answers", v.getAs[String]("answer"))) )
      producer.close()
    })

    println("sdfsd")
  }

  def decideTimeouts(planCoordinatesDF: Dataset[Row]): Dataset[Row]={
    val general: Dataset[Row] = GeneralCoordinates.coordinates
      .withColumnRenamed("coords", "step_0")
      .withColumn("time_0", lit(System.currentTimeMillis() + 120000))
    // FIRST STEP
    val firstStepJoinDF = general.join(planCoordinatesDF, planCoordinatesDF.col("stream_step_1").equalTo(general.col("step_1"))
      .or(planCoordinatesDF.col("stream_step_1").equalTo(general.col("step_2")))
      .or(planCoordinatesDF.col("stream_step_1").equalTo(general.col("step_3")))
      .or(planCoordinatesDF.col("stream_step_1").equalTo(general.col("step_4")))
      .or(planCoordinatesDF.col("stream_step_1").equalTo(general.col("step_5")))
      .or(planCoordinatesDF.col("stream_step_1").equalTo(general.col("step_0"))), "inner")

    val withWrongTimeDF = firstStepJoinDF.withColumn("wrong_time", callUDF("selectWrongTimesUDF", struct(firstStepJoinDF.columns.map(firstStepJoinDF(_)) : _*),lit(1)))
    val withWrongTimesDF = withWrongTimeDF.groupBy("stream_id").agg(collect_list("wrong_time").alias("wrong_times")).withColumnRenamed("stream_id", "stream_id2")

    val withWrongTimesAtPlan = planCoordinatesDF.join(withWrongTimesDF, withWrongTimesDF.col("stream_id2").equalTo(planCoordinatesDF.col("stream_id")), "left_outer")
    val withFirstTimeDF = withWrongTimesAtPlan.withColumn("timeout_1", callUDF("specifyGoodTimeUDF", struct(withWrongTimesAtPlan.columns.map(withWrongTimesAtPlan(_)) : _*),lit(1)))
      .select("stream_id2", "timeout_1")
    val firstFinalDF = planCoordinatesDF.join(withFirstTimeDF, planCoordinatesDF.col("stream_id").equalTo(withFirstTimeDF.col("stream_id2")), "left_outer")
      .drop("stream_id2")
      .na
      .fill(0, Seq("timeout_1"))

    // SECOND STEP
    val secondStepJoinDF = general.join(firstFinalDF, firstFinalDF.col("stream_step_2").equalTo(general.col("step_1"))
      .or(firstFinalDF.col("stream_step_2").equalTo(general.col("step_2")))
      .or(firstFinalDF.col("stream_step_2").equalTo(general.col("step_3")))
      .or(firstFinalDF.col("stream_step_2").equalTo(general.col("step_4")))
      .or(firstFinalDF.col("stream_step_2").equalTo(general.col("step_5")))
      .or(firstFinalDF.col("stream_step_1").equalTo(general.col("step_0"))), "inner")
    val secondWithWrongTimeDF = secondStepJoinDF.withColumn("wrong_time", callUDF("selectWrongTimesUDF", struct(secondStepJoinDF.columns.map(secondStepJoinDF(_)) : _*),lit(2)))
    val secondWithWrongTimesDF = secondWithWrongTimeDF.groupBy("stream_id").agg(collect_list("wrong_time").alias("wrong_times")).withColumnRenamed("stream_id", "stream_id2")
    val secondWithWrongTimesAtPlan = firstFinalDF.join(secondWithWrongTimesDF, secondWithWrongTimesDF.col("stream_id2").equalTo(firstFinalDF.col("stream_id")), "left_outer")
    val secondWithTimeDF = secondWithWrongTimesAtPlan.withColumn("timeout_2", callUDF("specifyGoodTimeUDF", struct(secondWithWrongTimesAtPlan.columns.map(secondWithWrongTimesAtPlan(_)) : _*),lit(2)))
      .select("stream_id2", "timeout_2")
    val secondFinalDF = firstFinalDF.join(secondWithTimeDF, firstFinalDF.col("stream_id").equalTo(secondWithTimeDF.col("stream_id2")), "left_outer")
      .drop("stream_id2")
      .na
      .fill(0, Seq("timeout_2"))

    // THIRD STEP
    val thirdStepJoinDF = general.join(secondFinalDF, secondFinalDF.col("stream_step_3").equalTo(general.col("step_1"))
      .or(secondFinalDF.col("stream_step_3").equalTo(general.col("step_2")))
      .or(secondFinalDF.col("stream_step_3").equalTo(general.col("step_3")))
      .or(secondFinalDF.col("stream_step_3").equalTo(general.col("step_4")))
      .or(secondFinalDF.col("stream_step_3").equalTo(general.col("step_5")))
      .or(secondFinalDF.col("stream_step_3").equalTo(general.col("step_0"))), "inner")
    val thirdWithWrongTimeDF = thirdStepJoinDF.withColumn("wrong_time", callUDF("selectWrongTimesUDF", struct(thirdStepJoinDF.columns.map(thirdStepJoinDF(_)) : _*),lit(3)))
    val thirdWithWrongTimesDF = thirdWithWrongTimeDF.groupBy("stream_id").agg(collect_list("wrong_time").alias("wrong_times")).withColumnRenamed("stream_id", "stream_id2")
    val thirdWithWrongTimesAtPlan = secondFinalDF.join(thirdWithWrongTimesDF, thirdWithWrongTimesDF.col("stream_id2").equalTo(secondFinalDF.col("stream_id")), "left_outer")
    val thirdWithTimeDF = thirdWithWrongTimesAtPlan.withColumn("timeout_3", callUDF("specifyGoodTimeUDF", struct(thirdWithWrongTimesAtPlan.columns.map(thirdWithWrongTimesAtPlan(_)) : _*),lit(3)))
      .select("stream_id2", "timeout_3")
    val thirdFinalDF = secondFinalDF.join(thirdWithTimeDF, secondFinalDF.col("stream_id").equalTo(thirdWithTimeDF.col("stream_id2")), "left_outer")
      .drop("stream_id2")
      .na
      .fill(0, Seq("timeout_3"))

    // FOURTH STEP
    val fourthStepJoinDF = general.join(thirdFinalDF, thirdFinalDF.col("stream_step_4").equalTo(general.col("step_1"))
      .or(thirdFinalDF.col("stream_step_4").equalTo(general.col("step_2")))
      .or(thirdFinalDF.col("stream_step_4").equalTo(general.col("step_3")))
      .or(thirdFinalDF.col("stream_step_4").equalTo(general.col("step_4")))
      .or(thirdFinalDF.col("stream_step_4").equalTo(general.col("step_5")))
      .or(thirdFinalDF.col("stream_step_4").equalTo(general.col("step_0"))), "inner")
    val fourthWithWrongTimeDF = fourthStepJoinDF.withColumn("wrong_time", callUDF("selectWrongTimesUDF", struct(fourthStepJoinDF.columns.map(fourthStepJoinDF(_)) : _*),lit(4)))
    val fourthWithWrongTimesDF = fourthWithWrongTimeDF.groupBy("stream_id").agg(collect_list("wrong_time").alias("wrong_times")).withColumnRenamed("stream_id", "stream_id2")
    val fourthWithWrongTimesAtPlan = thirdFinalDF.join(fourthWithWrongTimesDF, fourthWithWrongTimesDF.col("stream_id2").equalTo(thirdFinalDF.col("stream_id")), "left_outer")
    val fourthWithTimeDF = fourthWithWrongTimesAtPlan.withColumn("timeout_4", callUDF("specifyGoodTimeUDF", struct(fourthWithWrongTimesAtPlan.columns.map(fourthWithWrongTimesAtPlan(_)) : _*),lit(4)))
      .select("stream_id2", "timeout_4")
    val fourthFinalDF = thirdFinalDF.join(fourthWithTimeDF, thirdFinalDF.col("stream_id").equalTo(fourthWithTimeDF.col("stream_id2")), "left_outer")
      .drop("stream_id2")
      .na
      .fill(0, Seq("timeout_4"))

    // FIFTH STEP
    val fifthStepJoinDF = general.join(fourthFinalDF, fourthFinalDF.col("stream_step_5").equalTo(general.col("step_1"))
      .or(fourthFinalDF.col("stream_step_5").equalTo(general.col("step_2")))
      .or(fourthFinalDF.col("stream_step_5").equalTo(general.col("step_3")))
      .or(fourthFinalDF.col("stream_step_5").equalTo(general.col("step_4")))
      .or(fourthFinalDF.col("stream_step_5").equalTo(general.col("step_5")))
      .or(fourthFinalDF.col("stream_step_5").equalTo(general.col("step_0"))), "inner")
    val fifthWithWrongTimeDF = fifthStepJoinDF.withColumn("wrong_time", callUDF("selectWrongTimesUDF", struct(fifthStepJoinDF.columns.map(fifthStepJoinDF(_)) : _*),lit(5)))
    val fifthWithWrongTimesDF = fifthWithWrongTimeDF.groupBy("stream_id").agg(collect_list("wrong_time").alias("wrong_times")).withColumnRenamed("stream_id", "stream_id2")
    val fifthWithWrongTimesAtPlan = fourthFinalDF.join(fifthWithWrongTimesDF, fifthWithWrongTimesDF.col("stream_id2").equalTo(fourthFinalDF.col("stream_id")), "left_outer")
    val fifthWithTimeDF = fifthWithWrongTimesAtPlan.withColumn("timeout_5", callUDF("specifyGoodTimeUDF", struct(fifthWithWrongTimesAtPlan.columns.map(fifthWithWrongTimesAtPlan(_)) : _*),lit(5)))
      .select("stream_id2", "timeout_5")
    fourthFinalDF.join(fifthWithTimeDF, fourthFinalDF.col("stream_id").equalTo(fifthWithTimeDF.col("stream_id2")), "left_outer")
      .drop("stream_id2")
      .na
      .fill(0, Seq("timeout_5"))
  }

  def makeFinalAnswer(preparedUDF: Dataset[Row]): Dataset[Row]={
    preparedUDF.withColumn("answer", callUDF("finalDataUDF", struct(preparedUDF.columns.map(preparedUDF(_)) : _*)))
  }
}