package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import part3typesdatasets.CommonTypes.{carsDF, getCarNames, moviesDF}

object CommonTypesPractice_1 extends App {
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value"))
    .show()

  //////////////////////////////////////////// Booleans
  // you can create filter/where clauses
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDF.select("Title").where(dramaFilter)
  // + multiple ways of filtering

  // adding boolean above as an additional column
  val moviesWithGoodnessFlagDF = moviesDF.select(col("Title"), preferredFilter.as("is_good_movie"))

  // now that you have added a boolean column, you can filter based on the column itself:
  moviesWithGoodnessFlagDF.filter("is_good_movie")
    .show()

  // using above, but with negation:
  moviesWithGoodnessFlagDF.filter(not(col("is_good_movie")))
    .show()

  //////////////////////////////////////////// Numbers
  // math operators
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  // correlation = number between -1 and 1 => only between two NUMBER columns
  // if correlation is close to 1, it means if one column goes up, the other one also goes up
  // if correlation is close to -1, it means if one column goes up, the other one goes down
  // if correlation i between -0.3 and +0.3, then it means the given two columns are not related at all
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /* corr is an ACTION */)


  //////////////////////////////////////////// Strings

  val carsDF = spark.read.json("src/main/resources/data/cars.json")

  // capitalization: initcap, lower, upper
  // initcap: First Letter will be capitalized
  carsDF.select(initcap(col("Name")))

  // contains
  carsDF.select("*")
    .where(col("Name").contains("volkswagen"))

  // regex => selecting
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    // 0 => this is the id of match ed sequence (if any)
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  )
    .where(col("regex_extract") =!= "")
    .drop("regex_extract")

  // regex => replacement
  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )

  /**
    * Exercise
    *
    * Filter the cars DF by a list of car names obtained by an API call
    * Versions:
    *   - contains
    *   - regexes
    */

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  // version 1 - regex
  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|") // volskwagen|mercedes-benz|ford
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
  )
    .where(col("regex_extract") =!= "")
    .drop("regex_extract")

  // version 2 - contains
  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  // literal is the initial value
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show

}
