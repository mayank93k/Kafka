package kafka.spark.scala

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}

import java.io.File
import java.util.Properties
import scala.io.Source

object KafkaProducerApp extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val props: Properties = new Properties()
  val producer = new KafkaProducer[String, String](props)
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")
  val topic = "samplefiledata"
  private val directory = "C:\\Users\\mayan\\Pictures\\Test1"
  try {
    val d = new File(directory)
    if (d.exists && d.isDirectory) {
      val a = d.listFiles.filter(_.isFile).toList
      for (i <- a.indices) {
        for (line <- Source.fromFile(a(i)).getLines) {
          val record = new ProducerRecord[String, String](topic, line)
          val metadata = producer.send(record)
          printf(s"sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d)\n", record.key(), record.value(), metadata.get().partition(), metadata.get().offset())
        }
      }
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }
}
