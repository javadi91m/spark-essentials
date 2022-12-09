package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNullsPractice_3 extends App {

  /*
  if you remember, previously we defined some schema and use that to enforce our schema while reading a DF:
  val carsSchema = StructType(Array(
    StructField("Name", StringType, nullable=false), => BE CAREFUL HERE!
    ...
  ))
  val carsWithDefinedSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  when we pass nullable=false:
    - it doesn't define a constraint
    - it's just a marker for spark to optimize for nulls
    - it can lead to exceptions or data errors if broken
    - use "nullable=false" ONLY if you're pretty sure this column may never have nulls

   */

  val spark = SparkSession.builder()
    .appName("ManagingNulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  /*
  some movies have IMDB_Rating and some have Rotten_Tomato_Rating
  we want to select Title column and one of the two ratings that is not null
   */
  // select the FIRST non-null value
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    // coalesce returns the first column that is not null, or null if all inputs are null.
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  ).show()


  // checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNotNull)


  // nulls when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)


  // removing nulls => remove rows when one value is null
  moviesDF.select("Title", "IMDB_Rating").na.drop() // remove rows containing any null value


  // na in above snippet is a special OBJECT
  // we can use .na object to replace nulls:
  moviesDF.na.fill(0, List("Rotten_Tomatoes_Rating", "IMDB_Rating")) // it'll put 0 as "Rotten_Tomatoes_Rating", "IMDB_Rating" if they're null
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  ))


  // complex operations => these operations are not available in DF API and only available in SQL
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif", // returns null if the two values are EQUAL, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first != null) second else third !!
  ).show()


}
