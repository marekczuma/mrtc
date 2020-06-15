package pipeline

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

class KafkaAnswersSinkProvider extends StreamSinkProvider
  with DataSourceRegister {

  override def createSink(
                           sqlContext: SQLContext,
                           parameters: Map[String, String],
                           partitionColumns: Seq[String],
                           outputMode: OutputMode): Sink = {
    KafkaAnswersSink(sqlContext, parameters, partitionColumns, outputMode)
  }

  override def shortName(): String = "kafka_answers"
}