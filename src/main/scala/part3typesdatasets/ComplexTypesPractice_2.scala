package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import part3typesdatasets.CommonTypesPractice_1.spark
import part3typesdatasets.ComplexTypes.{moviesDF, spark}

object ComplexTypesPractice_2 extends App {

  // if you have more than one date_formats in your DF:
  // parse the DF multiple times, then union the small DFs

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // Dates
  // we already have read dates as String in a DataFrame
  // if spark fails to parse the date with the given format, it'll put null as date
  val moviesWithReleasedDate = moviesDF
    .select(col("Title"), to_date(col("Release_Date"), "d-MMM-yy").as("Actual_Release")) // conversion

  // failed records while parsing date
  moviesWithReleasedDate.select("*")
    .where(col("Actual_Release").isNull)
    .show()

  moviesWithReleasedDate.withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)
    .show()

  // similar to datediff => date_add , date_sub
  /**
    * Exercise
    * 1. How do we deal with multiple date formats?
    * 2. Read the stocks DF and parse the dates
    */

  // 1 - parse the DF multiple times, then union the small DFs

  // 2
  val stocksDF = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/stocks.csv")

  val stocksDFWithDates = stocksDF
    .withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))

  // Structures
  // Structures (or simply Structs) are groups of columns aggregated into one
  14: 55

  // 1 - with col operators
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))

  // 2 - with expression strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays

  val moviesWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // ARRAY of strings

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"), // indexing
    size(col("Title_Words")), // array size
    array_contains(col("Title_Words"), "Love") // look for value in array
  )


}
