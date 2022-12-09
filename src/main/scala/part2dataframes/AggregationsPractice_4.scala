package part2dataframes

import org.apache.spark.sql.SparkSession

object AggregationsPractice_4 extends App {

  // aggregations are wide transformations
  // we have "narrow" and "wide" transformations
  // narrow => one row in DataFrame will be mapped to exactly one row after the transformation
  // in narrow transformation, data remain in the same node
  // wide => one row in DataFrame may be mapped to 0 or multiple rows after the transformation
  // in wide transformation, data might transfer between different nodes
  // wide transformations are called "Shuffling", because data might transfer between different nodes
  // Shuffling in an expensive operation and has a big performance impact. most of the times it's better to do the shuffling at the end of our process

  val spark = SparkSession.builder()
    .appName("AggregationsPractice")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  import org.apache.spark.sql.functions._
  import spark.implicits._

  // counting
  val genresCountDF = moviesDF.select(count("Major_Genre")) // counts all the values except null
  genresCountDF.show

  moviesDF.selectExpr("count(Major_Genre)") // counts all the values except null
    .show()

  // counting all the rows
  moviesDF.select(count("*"))
    .show()


  // counting distinct values
  moviesDF.select(countDistinct("Major_Genre")) // excluding null
    .show()

  // showing all distinct values (and not only counting them)
  moviesDF.select(("Major_Genre")).distinct()
    .show()


  // you can do an approximate but fast counting
  moviesDF.select(approx_count_distinct("Major_Genre"))
    .show()


  // min and max
  moviesDF.select(min("IMDB_Rating"))
    .show()
  moviesDF.selectExpr("min(IMDB_Rating)")
    .show()

  moviesDF.select(max("IMDB_Rating"))
    .show()

  // sum
  moviesDF.select(sum("US_Gross"))
    .show()
  moviesDF.selectExpr("sum(US_Gross)")
    .show()

  // avg
  moviesDF.select(avg("US_Gross"))
  moviesDF.selectExpr("avg(US_Gross)")


  // data science: average (mean) and standard deviation
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()


  // Grouping: SQL => select count(*) from moviesDF group by Major_Genre
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .count()
    .show()

  moviesDF
    .groupBy("Major_Genre")
    .avg("IMDB_Rating")
    .show()

  // aggregations
  moviesDF
    .groupBy("Major_Genre")
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy("Avg_Rating")
    .show()


  /**
    * Exercises
    *
    * 1. Sum up ALL the profits of ALL the movies in the DF
    * 2. Count how many distinct directors we have
    * 3. Show the mean and standard deviation of US gross revenue for the movies
    * 4. Compute the average IMDB rating and the total US gross revenue PER DIRECTOR
    */

  // 139190135783 => produce wrong result because of null
  println("Sum up ALL the profits of ALL the movies in the DF")
  moviesDF.select(sum("US_Gross") + sum("Worldwide_Gross") + sum("US_DVD_Sales") as ("Total profit"))
    .show()

  // 432813952470
  moviesDF.select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total Profit"))
    .select(sum("Total Profit"))
    .show()

  println("Count how many distinct directors we have")
  moviesDF.select(countDistinct("Director"))
    .show()

  println("Show the mean and standard deviation of US gross revenue for the movies")
  moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  )
    .show()

  println("Compute the average IMDB rating and the total US gross revenue PER DIRECTOR")
  moviesDF.groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("AVG_RATING"),
      sum("US_Gross")
    )
    .orderBy(col("AVG_RATING").desc_nulls_last)
    .show()

}
