package org.org.spark.scala.mba.usecase

import java.util
import java.io.File
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.{Level, Logger}
import scala.collection.JavaConverters._
import scala.collection.mutable

object KafkaConsumerSubscribeApp extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit): Unit = {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

  import java.util.Properties

  val TOPIC = "samplefiledata"
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")
  val ss = new mutable.ListBuffer[String]
  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(util.Collections.singletonList(TOPIC))
  while (true) {
    val records = consumer.poll(100)
    for (record <- records.asScala) {
      val a = record.value()
      ss += a
      printToFile(new File("C:\\Users\\mayan\\Pictures\\Write.txt")) {
        p => ss.foreach(p.println)
      }
    }
  }
}
