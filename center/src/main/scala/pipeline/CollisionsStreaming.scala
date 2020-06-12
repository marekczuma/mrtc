package pipeline

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
        .format("pipeline.KafkaAnswersSinkProvider")
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
