import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}

object Constants {
  val PROPERTIES_CONF_FILE = "properties.conf"
  val PROPERTIES_CONFIG = "properties"
  val APP_NAME = "appName"
  val KAFKA_TOPIC = "kafkaTopic"
  val INPUT_DIR_PATH = "inputDirPath"
  val FILENAME = "fileName"

  val movieSchema = StructType(
    Array(
      StructField("budget", LongType, false),
      StructField("genres", StringType, false),
      StructField("homepage", StringType, false),
      StructField("id", StringType, false),
      StructField("keywords", StringType, false),
      StructField("original_language", StringType, false),
      StructField("original_title", StringType, false),
      StructField("overview", StringType, false),
      StructField("popularity", StringType, false),
      StructField("production_companies", StringType, false),
      StructField("production_countries", StringType, false),
      StructField("release_date", StringType, false),
      StructField("revenue", StringType, false),
      StructField("runtime", StringType, false),
      StructField("spoken_languages", StringType, false),
      StructField("status", StringType, false),
      StructField("tagline", StringType, false),
      StructField("title", StringType, false),
      StructField("vote_average", StringType, false),
      StructField("vote_count", StringType, false)
    )
  )

  val genreSchema = new StructType(Array(
    StructField("genre",new ArrayType(new StructType().
      add("id", LongType).add("name", StringType),false))
  ))

}
