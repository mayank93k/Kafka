import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import Constants._
import MovieUtils._

object KafkaConsumerStreaming {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR);
    System.setProperty("hadoop.home.dir","G:\\sparkResources\\winutls")
    val checkPointDirectory = "file:///G:/sparkResources/imdbMovies/temp"

    // Loading the external config and getting all the input values needed
    val config = ConfigFactory.load(PROPERTIES_CONF_FILE).getConfig(PROPERTIES_CONFIG)
    val appName = config.getString(APP_NAME)
    val topicName = config.getString(KAFKA_TOPIC)

    // creating the spark session object and the streaming context
    val spark = SparkSession.builder().appName(appName).master("local[*]").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))

    // fetching kafka consumer properties
    val consumerParams = KafkaProperties.getConsumerProperties()
    val topic = Set(topicName)

    // creating a direct stream object to receive streams sent by the producer, and attaching the properties to it as well
    // as the topic to which the consumer has to fetch from
    val stream =  KafkaUtils.createDirectStream[String, String](
                                                   ssc,
                                                   PreferConsistent,
                                                   Subscribe[String, String](topic, consumerParams)
                                                 )
    // getting only the consumer record value as a DStream[String]
    val lines = stream.map(record => record.value())

    // function that takes a list of string and converts it into a Row object
    def makeRow(line: List[String]): Row = Row(line(0).toLong, line(1),line(2), line(3),line(4), line(5),line(6), line(7),line(8), line(9),
                                                line(10), line(11),line(12), line(13),line(14), line(15),line(16), line(17),line(18),line(19))

    lines.foreachRDD((rdd, time) => {
      // mapping from RDD[String] to RDD[Row]
      val data = rdd.map(line => line.split("\\t").toList).filter(l => l.length == 20).map(makeRow)
      val movieDf = spark.createDataFrame(data, movieSchema)

      // 1. Movies with budget >= 200000000 and of genre action and science fiction
      getMoviesWithBudgetAndGenre(movieDf, spark).show(50,false)
    })

    // setting the check point directory
    ssc.checkpoint(checkPointDirectory)
    // starting the streaming context and awaitinting termination
    ssc.start()
    ssc.awaitTermination()

  }

}
