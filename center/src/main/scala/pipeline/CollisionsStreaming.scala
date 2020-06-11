package pipeline

import hbase.GeneralCoordinates
import org.apache.ivy.plugins.trigger.Trigger
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import transformations._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery

object CollisionsStreaming {
  private val ExtractIDUDF = "extractIDUDF"
  private val ExtractCoordinatesUDF = "extractCoordinatesUDF"
  private val ExtractDirectionsUDF = "extractDirectionsUDF"
  private val TmpMakeAnswerUDF = "tmpMakeAnswerUDF"
  private val ChangeDirectionsToCoordinatesUDF = "changeDirectionsToCoordinatesUDF"
  private val SelectWrongTimesUDF = "selectWrongTimesUDF"
  private val SpecifyGoodTimeUDF = "specifyGoodTimeUDF"
  private val FinalDataUDF = "finalDataUDF"
  private val IDColumnName = "stream_id"
  private val CoordinatesColumnName = "start_coords"
  private val CoordinatesInGeneralColumnName = "coords"
  private val DirectionsColumnName = "directions"
  private val CoordinatorsPlanColumnName = "coordinators_plan"
}

class CollisionsStreaming(val spark: SparkSession) {
  import spark.implicits._

  def run(): Unit ={
    registerUDFs()
    val questions: Dataset[Row] = readQuestions()
    val questionsWithExtractedParts: Dataset[Row] = extractionTransformations(questions)
    val planCoordinates: Dataset[Row] = changeToPlanCoordinates(questionsWithExtractedParts)
//    val withTimeouts: Dataset[Row] = decideTimeouts(planCoordinates)
//    val finalAnswer: Dataset[Row] = makeFinalAnswer(withTimeouts)
    val streamingQuery: StreamingQuery = writeAnswers(planCoordinates, false)
    streamingQuery.awaitTermination()
  }

  def readQuestions(): Dataset[Row]={
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "questions")
      .option("failOnDataLoss", "false")
      .load()
  }

  def writeAnswers(answerDF: Dataset[Row]): StreamingQuery={
    answerDF.select("answer")
      .withColumnRenamed("answer", "value")
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "answers")
      .option("checkpointLocation", "/Users/user/apps/mrtc/center/checkpoints")
      .start()
  }

  def writeAnswers(answerDF: Dataset[Row], forConsole: Boolean): StreamingQuery={
    if(forConsole) {
      answerDF
        .writeStream
        .outputMode("append")
        .format("console")
        .start()
    }else{
      answerDF.writeStream
        .format("pipeline.DemoSinkProvider")
        .option("checkpointLocation", "/Users/user/apps/mrtc/center/checkpointsDemo")
        .start
    }
  }

  def extractionTransformations(questionsDF: Dataset[Row]): Dataset[Row]={
    questionsDF.selectExpr("CAST(value AS STRING)")
      .as[String]
      .toDF()
      .withColumn(CollisionsStreaming.IDColumnName, callUDF(CollisionsStreaming.ExtractIDUDF, col("value")))
      .withColumn(CollisionsStreaming.CoordinatesColumnName, callUDF(CollisionsStreaming.ExtractCoordinatesUDF, col("value")))
      .withColumn(CollisionsStreaming.DirectionsColumnName, callUDF(CollisionsStreaming.ExtractDirectionsUDF, col("value")))
  }

  def changeToPlanCoordinates(extractedDF: Dataset[Row]): Dataset[Row]={
    extractedDF.withColumn("coordinators_plan", callUDF(CollisionsStreaming.ChangeDirectionsToCoordinatesUDF,
      col(CollisionsStreaming.CoordinatesColumnName),col("directions")))
      .withColumn("step_n", split(col(CollisionsStreaming.CoordinatorsPlanColumnName), "\\;"))
      .withColumn("stream_step_1", col("step_n").getItem(0))
      .withColumn("stream_step_2", col("step_n").getItem(1))
      .withColumn("stream_step_3", col("step_n").getItem(2))
      .withColumn("stream_step_4", col("step_n").getItem(3))
      .withColumn("stream_step_5", col("step_n").getItem(4))
      .drop("step_n")
  }

  def decideTimeouts(planCoordinatesDF: Dataset[Row]): Dataset[Row]={
    val general: Dataset[Row] = GeneralCoordinates.coordinates
      .withColumnRenamed(CollisionsStreaming.CoordinatesInGeneralColumnName, "step_0")
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
    preparedUDF.withColumn("answer", callUDF(CollisionsStreaming.FinalDataUDF, struct(preparedUDF.columns.map(preparedUDF(_)) : _*)))
  }

  def registerUDFs(): Unit ={
    val extractIDUDF: ExtractIDUDF = new ExtractIDUDF()
    val extractCoordinatesUDF: ExtractCoordinatesUDF = new ExtractCoordinatesUDF()
    val extractDirectionsUDF: ExtractDirectionsUDF = new ExtractDirectionsUDF()
    val tmpMakeAnswerUDF: TmpMakeAnswerUDF = new TmpMakeAnswerUDF()
    val changeDirectionsToCoordinates: ChangeDirectionsToCoordinatesUDF = new ChangeDirectionsToCoordinatesUDF()
    val selectWrongTimesUDF: SelectWrongTimesUDF = new SelectWrongTimesUDF()
    val specifyGoodTimeUDF: SpecifyGoodTimeUDF = new SpecifyGoodTimeUDF()
    val finalDataUDF: FinalDataUDF = new FinalDataUDF()
    spark.udf
      .register(CollisionsStreaming.ExtractIDUDF, extractIDUDF, DataTypes.StringType)
    spark.udf
      .register(CollisionsStreaming.ExtractCoordinatesUDF, extractCoordinatesUDF, DataTypes.StringType)
    spark.udf
      .register(CollisionsStreaming.ExtractDirectionsUDF, extractDirectionsUDF, DataTypes.StringType)
    spark.udf
      .register(CollisionsStreaming.TmpMakeAnswerUDF, tmpMakeAnswerUDF, DataTypes.StringType)
    spark.udf
      .register(CollisionsStreaming.ChangeDirectionsToCoordinatesUDF, changeDirectionsToCoordinates, DataTypes.StringType)
    spark.udf
      .register(CollisionsStreaming.SelectWrongTimesUDF, selectWrongTimesUDF, DataTypes.createArrayType(DataTypes.LongType))
    spark.udf
      .register(CollisionsStreaming.SpecifyGoodTimeUDF, specifyGoodTimeUDF, DataTypes.IntegerType)
    spark.udf
      .register(CollisionsStreaming.FinalDataUDF, finalDataUDF, DataTypes.StringType)
  }

}
