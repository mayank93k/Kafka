package kafka.spark.scala

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.{Level, Logger}

import java.io.File
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

object KafkaConsumerSubscribeApp extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  private val TOPIC = "samplefiledata"

  import java.util.Properties
  val props = new Properties()
  private val list = new mutable.ListBuffer[String]
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")
  val consumer = new KafkaConsumer[String, String](props)

  private def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit): Unit = {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  while (true) {
    val records = consumer.poll(100)
    for (record <- records.asScala) {
      val a = record.value()
      list += a
      printToFile(new File("C:\\Users\\mayan\\Pictures\\Write.txt")) {
        p => list.foreach(p.println)
      }
    }
  }
}
