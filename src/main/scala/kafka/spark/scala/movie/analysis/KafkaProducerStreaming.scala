import Constants.{PROPERTIES_CONFIG, PROPERTIES_CONF_FILE}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import Constants._
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import scala.io.Source

object KafkaProducerStreaming {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR);

    // Loading the external config and getting all the input values needed
    val config = ConfigFactory.load(PROPERTIES_CONF_FILE).getConfig(PROPERTIES_CONFIG)
    val inputDirectory = config.getString(INPUT_DIR_PATH)
    val fileName = config.getString(FILENAME)
    val topic = config.getString(KAFKA_TOPIC)
    val filepath = inputDirectory+"\\"+fileName;

    val producerProperties = KafkaProperties.getProducerProperties()
    // creating a kafka producer object and adding producer properties to it
    val kafkaProducer : Producer[String, String]= new KafkaProducer[String, String](producerProperties)

    val file = Source.fromFile(filepath)
    var i = 0
    // reading source file line by line as a produced record and producing it through the kafka producer
    for(line <- file.getLines()){
      val producerRecord = new ProducerRecord[String, String](topic, line)
      kafkaProducer.send(producerRecord)
      println("sending line "+i)
      i = i+1
    }

    // finally closing the input file and the kafka producer object
    file.close()
    kafkaProducer.close()
  }
}
