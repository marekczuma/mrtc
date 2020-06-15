package kafka

import services.RoversManager
import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Streaming Consumer connected with 'answers' topic on kafka.
 * Pattern of answer: "{rover-id}-{times}".
 * E.g. answer for rover with id 1: "1-0,0,2,4,0" (it must wait 0min for first step, 0min for second,
 * 2 minutes for third, 4 minutes for fourth, 0 min for fifth).
 *
 */
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
      if(valuesOfAnswers.nonEmpty){ RoversManager.sendAnswers(valuesOfAnswers) }
    }
  }


}
