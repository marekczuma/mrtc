package pipeline

import java.util.Properties

import hbase.GeneralCoordinates
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.functions.{callUDF, collect_list, lit, struct}
import org.apache.spark.sql.streaming.OutputMode

/**
 * Custom sink. We add 2 operations to original pipeline.
 * 1. decideTimeouts() - main algorithm.
 * 2. makeFinalAnswer() - making final form of the answer.
 * @param sqlContext
 * @param parameters
 * @param partitionColumns
 * @param outputMode
 */
case class KafkaAnswersSink(
                     sqlContext: SQLContext,
                     parameters: Map[String, String],
                     partitionColumns: Seq[String],
                     outputMode: OutputMode) extends Sink {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val withTimeouts: Dataset[Row] = decideTimeouts(data)
    val finalAnswer: Dataset[Row] = makeFinalAnswer(withTimeouts)
    val values: Dataset[Row] = finalAnswer.select("answer")
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    values.foreachPartition(partition=> {
      val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
      partition.foreach(v=> producer.send(new ProducerRecord[String, String]("answers", v.getAs[String]("answer"))) )
      producer.close()
    })
  }

  def decideTimeouts(planCoordinatesDF: Dataset[Row]): Dataset[Row]={
    val general: Dataset[Row] = GeneralCoordinates.coordinates
      .withColumnRenamed("coords", "step_0")
      .withColumn("time_0", lit(System.currentTimeMillis() + 120000))
    // FIRST STEP
    val firstStepJoinDF: Dataset[Row] = general.join(planCoordinatesDF, planCoordinatesDF.col("stream_step_1").equalTo(general.col("step_1"))
      .or(planCoordinatesDF.col("stream_step_1").equalTo(general.col("step_2")))
      .or(planCoordinatesDF.col("stream_step_1").equalTo(general.col("step_3")))
      .or(planCoordinatesDF.col("stream_step_1").equalTo(general.col("step_4")))
      .or(planCoordinatesDF.col("stream_step_1").equalTo(general.col("step_5")))
      .or(planCoordinatesDF.col("stream_step_1").equalTo(general.col("step_0"))), "inner")

    val withWrongTimeDF: Dataset[Row] = firstStepJoinDF.withColumn("wrong_time", callUDF("selectWrongTimesUDF", struct(firstStepJoinDF.columns.map(firstStepJoinDF(_)) : _*),lit(1)))
    val withWrongTimesDF: Dataset[Row] = withWrongTimeDF.groupBy("stream_id").agg(collect_list("wrong_time").alias("wrong_times")).withColumnRenamed("stream_id", "stream_id2")

    val withWrongTimesAtPlanDF: Dataset[Row] = planCoordinatesDF.join(withWrongTimesDF, withWrongTimesDF.col("stream_id2").equalTo(planCoordinatesDF.col("stream_id")), "left_outer")
    val withFirstTimeDF: Dataset[Row] = withWrongTimesAtPlanDF.withColumn("timeout_1", callUDF("specifyGoodTimeUDF", struct(withWrongTimesAtPlanDF.columns.map(withWrongTimesAtPlanDF(_)) : _*),lit(1)))
      .select("stream_id2", "timeout_1")
    val firstFinalDF: Dataset[Row] = planCoordinatesDF.join(withFirstTimeDF, planCoordinatesDF.col("stream_id").equalTo(withFirstTimeDF.col("stream_id2")), "left_outer")
      .drop("stream_id2")
      .na
      .fill(0, Seq("timeout_1"))

    // SECOND STEP
    val secondStepJoinDF: Dataset[Row] = general.join(firstFinalDF, firstFinalDF.col("stream_step_2").equalTo(general.col("step_1"))
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
    val thirdStepJoinDF: Dataset[Row] = general.join(secondFinalDF, secondFinalDF.col("stream_step_3").equalTo(general.col("step_1"))
      .or(secondFinalDF.col("stream_step_3").equalTo(general.col("step_2")))
      .or(secondFinalDF.col("stream_step_3").equalTo(general.col("step_3")))
      .or(secondFinalDF.col("stream_step_3").equalTo(general.col("step_4")))
      .or(secondFinalDF.col("stream_step_3").equalTo(general.col("step_5")))
      .or(secondFinalDF.col("stream_step_3").equalTo(general.col("step_0"))), "inner")
    val thirdWithWrongTimeDF: Dataset[Row] = thirdStepJoinDF.withColumn("wrong_time", callUDF("selectWrongTimesUDF", struct(thirdStepJoinDF.columns.map(thirdStepJoinDF(_)) : _*),lit(3)))
    val thirdWithWrongTimesDF: Dataset[Row] = thirdWithWrongTimeDF.groupBy("stream_id").agg(collect_list("wrong_time").alias("wrong_times")).withColumnRenamed("stream_id", "stream_id2")
    val thirdWithWrongTimesAtPlanDF: Dataset[Row] = secondFinalDF.join(thirdWithWrongTimesDF, thirdWithWrongTimesDF.col("stream_id2").equalTo(secondFinalDF.col("stream_id")), "left_outer")
    val thirdWithTimeDF: Dataset[Row] = thirdWithWrongTimesAtPlanDF.withColumn("timeout_3", callUDF("specifyGoodTimeUDF", struct(thirdWithWrongTimesAtPlanDF.columns.map(thirdWithWrongTimesAtPlanDF(_)) : _*),lit(3)))
      .select("stream_id2", "timeout_3")
    val thirdFinalDF: Dataset[Row] = secondFinalDF.join(thirdWithTimeDF, secondFinalDF.col("stream_id").equalTo(thirdWithTimeDF.col("stream_id2")), "left_outer")
      .drop("stream_id2")
      .na
      .fill(0, Seq("timeout_3"))

    // FOURTH STEP
    val fourthStepJoinDF: Dataset[Row] = general.join(thirdFinalDF, thirdFinalDF.col("stream_step_4").equalTo(general.col("step_1"))
      .or(thirdFinalDF.col("stream_step_4").equalTo(general.col("step_2")))
      .or(thirdFinalDF.col("stream_step_4").equalTo(general.col("step_3")))
      .or(thirdFinalDF.col("stream_step_4").equalTo(general.col("step_4")))
      .or(thirdFinalDF.col("stream_step_4").equalTo(general.col("step_5")))
      .or(thirdFinalDF.col("stream_step_4").equalTo(general.col("step_0"))), "inner")
    val fourthWithWrongTimeDF: Dataset[Row] = fourthStepJoinDF.withColumn("wrong_time", callUDF("selectWrongTimesUDF", struct(fourthStepJoinDF.columns.map(fourthStepJoinDF(_)) : _*),lit(4)))
    val fourthWithWrongTimesDF: Dataset[Row] = fourthWithWrongTimeDF.groupBy("stream_id").agg(collect_list("wrong_time").alias("wrong_times")).withColumnRenamed("stream_id", "stream_id2")
    val fourthWithWrongTimesAtPlanDF: Dataset[Row] = thirdFinalDF.join(fourthWithWrongTimesDF, fourthWithWrongTimesDF.col("stream_id2").equalTo(thirdFinalDF.col("stream_id")), "left_outer")
    val fourthWithTimeDF: Dataset[Row] = fourthWithWrongTimesAtPlanDF.withColumn("timeout_4", callUDF("specifyGoodTimeUDF", struct(fourthWithWrongTimesAtPlanDF.columns.map(fourthWithWrongTimesAtPlanDF(_)) : _*),lit(4)))
      .select("stream_id2", "timeout_4")
    val fourthFinalDF: Dataset[Row] = thirdFinalDF.join(fourthWithTimeDF, thirdFinalDF.col("stream_id").equalTo(fourthWithTimeDF.col("stream_id2")), "left_outer")
      .drop("stream_id2")
      .na
      .fill(0, Seq("timeout_4"))

    // FIFTH STEP
    val fifthStepJoinDF: Dataset[Row] = general.join(fourthFinalDF, fourthFinalDF.col("stream_step_5").equalTo(general.col("step_1"))
      .or(fourthFinalDF.col("stream_step_5").equalTo(general.col("step_2")))
      .or(fourthFinalDF.col("stream_step_5").equalTo(general.col("step_3")))
      .or(fourthFinalDF.col("stream_step_5").equalTo(general.col("step_4")))
      .or(fourthFinalDF.col("stream_step_5").equalTo(general.col("step_5")))
      .or(fourthFinalDF.col("stream_step_5").equalTo(general.col("step_0"))), "inner")
    val fifthWithWrongTimeDF: Dataset[Row] = fifthStepJoinDF.withColumn("wrong_time", callUDF("selectWrongTimesUDF", struct(fifthStepJoinDF.columns.map(fifthStepJoinDF(_)) : _*),lit(5)))
    val fifthWithWrongTimesDF: Dataset[Row] = fifthWithWrongTimeDF.groupBy("stream_id").agg(collect_list("wrong_time").alias("wrong_times")).withColumnRenamed("stream_id", "stream_id2")
    val fifthWithWrongTimesAtPlanDF: Dataset[Row] = fourthFinalDF.join(fifthWithWrongTimesDF, fifthWithWrongTimesDF.col("stream_id2").equalTo(fourthFinalDF.col("stream_id")), "left_outer")
    val fifthWithTimeDF: Dataset[Row] = fifthWithWrongTimesAtPlanDF.withColumn("timeout_5", callUDF("specifyGoodTimeUDF", struct(fifthWithWrongTimesAtPlanDF.columns.map(fifthWithWrongTimesAtPlanDF(_)) : _*),lit(5)))
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