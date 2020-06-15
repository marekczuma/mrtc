package kafka

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.collection.mutable.ListBuffer

/**
 * Streaming Producer connected with 'questions' topic on kafka.
 * Pattern of question: "{rover_id}-{current_coordinates}-{directions}"
 * E.g. for rover with id 1: "1-10,12-NNWNW".
 */
object QuestionsStreaming {
  def run(values: ListBuffer[String]): Unit ={
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
    values.foreach(v=> producer.send(new ProducerRecord[String, String]("questions", v)))
    producer.close()
  }
}
