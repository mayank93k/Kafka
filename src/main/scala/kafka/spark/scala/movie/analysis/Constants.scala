package kafka.spark.scala.movie.analysis

import org.apache.spark.sql.types._

object Constants {
  val PROPERTIES_CONF_FILE = "properties.conf"
  val PROPERTIES_CONFIG = "properties"
  val APP_NAME = "appName"
  val KAFKA_TOPIC = "kafkaTopic"
  val INPUT_DIR_PATH = "inputDirPath"
  val FILENAME = "fileName"

  val movieSchema: StructType = StructType(
    Array(
      StructField("budget", LongType, nullable = false),
      StructField("genres", StringType, nullable = false),
      StructField("homepage", StringType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("keywords", StringType, nullable = false),
      StructField("original_language", StringType, nullable = false),
      StructField("original_title", StringType, nullable = false),
      StructField("overview", StringType, nullable = false),
      StructField("popularity", StringType, nullable = false),
      StructField("production_companies", StringType, nullable = false),
      StructField("production_countries", StringType, nullable = false),
      StructField("release_date", StringType, nullable = false),
      StructField("revenue", StringType, nullable = false),
      StructField("runtime", StringType, nullable = false),
      StructField("spoken_languages", StringType, nullable = false),
      StructField("status", StringType, nullable = false),
      StructField("tagline", StringType, nullable = false),
      StructField("title", StringType, nullable = false),
      StructField("vote_average", StringType, nullable = false),
      StructField("vote_count", StringType, nullable = false)
    )
  )

  val genreSchema = new StructType(Array(
    StructField("genre", new ArrayType(new StructType().
      add("id", LongType).add("name", StringType), false))
  ))
}
