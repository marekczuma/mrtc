package kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import services.RoversManager
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object AnswersStreaming {

  def run(): Unit ={
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("answers"))
    while (true) {
      val records = consumer.poll(1000).asScala
      val valuesOfAnswers = ListBuffer(records.map(r=>r.value()).toSeq: _*)
      if(valuesOfAnswers.nonEmpty){
        RoversManager.sendAnswers(valuesOfAnswers)
      }
    }
  }


}
