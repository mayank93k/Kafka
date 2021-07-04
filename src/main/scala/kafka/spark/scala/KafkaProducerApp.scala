package org.org.spark.scala.mba.usecase

  import java.io.File
  import java.util.Properties
  import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
  import org.apache.log4j.{Level, Logger}
  import scala.io.Source
  object KafkaProducerApp extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val dir = "C:\\Users\\mayan\\Pictures\\Test1"
    val props:Properties = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")
    val producer = new KafkaProducer[String, String](props)
    val topic = "samplefiledata"
    try {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        val a =  d.listFiles.filter(_.isFile).toList
        for(i<- 0 until a.length) {
          for (line <- Source.fromFile(a(i)).getLines) {
           val record = new ProducerRecord[String, String](topic, line)
            val metadata = producer.send(record)
            printf(s"sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d)\n", record.key(), record.value(), metadata.get().partition(), metadata.get().offset())
          }
        }
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      producer.close()
    }
  }


