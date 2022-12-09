package part2dataframes

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object ColumnsAndExpressionsPractice_3 extends App {

  val spark = SparkSession.builder()
    .appName("ColumnsAndExpressionsPractice")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .json("src/main/resources/data/cars.json")
  carsDF.show()

  // Columns
  val firstColumn: Column = carsDF.col("Name") // this Column object does not have any data in it and it can be used only in Select queries.

  // selecting (projection)
  var carNamesDF: DataFrame = carsDF.select(firstColumn)

  import org.apache.spark.sql.functions.{col, column, expr}
  import spark.implicits._

  val differentSelectMethods = carsDF.select(
    carsDF.col("Name"),
    col("Name"),
    column("Name"),
    expr("Name"), // EXPRESSIONS
    'Name, // Scala Symbol, auto-converted to column
    $"Name", // interpolated String,
  )
  differentSelectMethods.show()

  // select with column name (you CANNOT interchange between this way and above)
  carsDF.select("Name", "Year")


  // selecting is a simpler version of EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    // in expr you can pass a String like SQL
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg2")
  )
  carsWithWeightsDF.show()

  // selectExpr method: useful when you want to have only "expr" calls in "carsDF.select(" call
  carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2",
  )
    .show()

  // processing dataframes: 14:50

  // adding a new column to the dataframe
  carsDF.withColumn("Weight_in_kg3", col("Weight_in_lbs") / 2.2)
    .show()

  // renaming an existing column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "weight in pounds")

  // if a column name has spaces in it, you need to escape it (by using ``) while using it in expressions
  carsWithColumnRenamed.selectExpr("`weight in pounds` / 2.2")
    .show()

  // removing columns
  carsDF.drop("Miles_per_Gallon", "Cylinders")
    .show()


  // filtering
  // not equal operator is: =!= ; equal will be: ===
  carsDF.filter(col("Origin") =!= "USA")
    .show()

  // you can do the same above thing with "where"
  carsDF.where(col("Origin") =!= "USA")
    .show()

  // filtering with expression String => same thing applies for "where"
  carsDF.filter("Origin != 'USA'")
    .show()

  // chaining filters
  carsDF
    .filter("Origin == 'USA'")
    .filter("Horsepower > 150")
    .show()

  // you can combine them into one filter
  carsDF.filter("Origin == 'USA' and Horsepower > 150")
    .show()

  // another way of combining:
  carsDF.filter(
    (col("Origin") === "USA").and(col("Horsepower") > 150)
  )
    .show()

  // and method is infix, so you can write above like this
  carsDF.filter(
    col("Origin") === "USA" and col("Horsepower") > 150
  )
    .show()


  // unioning = adding more rows = appending DataFrames together
  val moreCarsDF = spark.read.json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works only if DataFrames have the SAME SCHEMA


  // distinct values
  val allCountries = carsDF.select("Origin").distinct()
  allCountries.show()



  /////////// EXERCISES
  /*
  1. read the movies DF and select 2 columns of your choice
  2. create another column and calculate the total profit of the movie: sum all the gross values (US_Gross, Worldwide_Gross, US_DVD_Sales)
  3. select all the "Comedy" movies "Major_genre" which its IMDB_rating above 6
   */

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  moviesDF.select("Title", "US_Gross")
    .show()

  moviesDF.select(
    moviesDF.col("Title").as("moviesDF.col"),
    col("Title").as("col"),
    column("Title").as("column"),
    expr("Title").as("expr"),
    'Title.as("'"),
    $"Title".as("$\"")
  )
    .show()

  moviesDF.selectExpr("Title")


  println("with total profit column")
  // this doesn't work: because some of the US_DVD_Sales values are null and make the whole result as null
  // we'll see how to treat with nulls in future
  moviesDF.select("Title", "US_Gross", "Worldwide_Gross", "US_DVD_Sales")
    .withColumn("total profit", expr("US_Gross + Worldwide_Gross + US_DVD_Sales"))
    .show()

  moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("total profit")
  ).show()

  println("moviesDF.selectExpr(")
  moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_DVD_Sales",
    "US_Gross + Worldwide_Gross + US_DVD_Sales as `total profit`"
  ).show()

  println("good comedies")
  moviesDF.filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
    .show()

  moviesDF.filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")
    .show()




}
