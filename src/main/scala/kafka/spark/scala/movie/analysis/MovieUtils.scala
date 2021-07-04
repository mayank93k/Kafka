import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import Constants._

object MovieUtils {

  /**
   *
   * @param movieDf -> original movies dataframe
   * @param spark   -> spark session
   * @return        -> resultant dataframe containing movies with budget >= 200000000 and of genre action and science fiction
   */
  def getMoviesWithBudgetAndGenre(movieDf : DataFrame, spark : SparkSession) : DataFrame = {
    import spark.implicits._
    // keeping movies whose budget is >= 200000000
    val requiredDf = movieDf.select($"id".as("movie_id"), $"original_title".as("title"), $"budget", $"genres")
                          .filter($"budget" >= 200000000L)
    // modifying the genres column to proper json format
    // earlier -> "[ {""id"" : 12, ""name"" : ""Avatar""} ]"
    // modified -> {"genre" : [ {"id":12, "name":"Avatar"}]}
    // added a name to the json array and removed extra "
    val updatedDf = requiredDf.withColumn("genres", regexp_replace($"genres","\"\"", "\""))
                              .withColumn("genres", regexp_replace($"genres","\"\\[", "\\["))
                              .withColumn("genres", regexp_replace($"genres","\\]\"", "\\]"))
                              .withColumn("genres",concat(lit("{\"genre\" : "), $"genres",lit("}")))

    // updated genre column to Struct Type( json format) from String Type
    val jsonDf = updatedDf.withColumn("genres", from_json($"genres",genreSchema))
    // creating a genre_name column of array type from genre column, containing only the genre names and checking
    // whether it contains Action as well as Science Fiction
    val genreDf = jsonDf.select($"movie_id", $"title", $"budget", $"genres.genre".as("genre"))
                  .withColumn("genre_name",$"genre.name")
                  .withColumn("isPresent", array_contains($"genre_name","Action")
                    .and(array_contains($"genre_name","Science Fiction")))

    // keeping movies which are of Action and Science Fiction Genre and dropping the extra columns
    val resultDf = genreDf.filter($"isPresent" === true).drop("genre", "isPresent")
    resultDf
  }
}
