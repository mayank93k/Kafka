package kafka.spark.scala.movie.analysis

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import java.io
import java.util.Properties

object KafkaProperties {

  // Setting Kafka Producer properties
  def getProducerProperties: Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "movieProducer")

    props
  }

  // Setting Kafka Consumer properties
  def getConsumerProperties: Map[String, io.Serializable] = {
    val kafkaParams = Map("bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "group.id" -> "movieGroup"
    )
    kafkaParams
  }

}
