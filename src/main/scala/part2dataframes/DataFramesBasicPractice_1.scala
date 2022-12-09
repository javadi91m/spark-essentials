package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFramesBasicPractice_1 extends App {

  // brand, model
  val smartPhonesSeq = Seq(
    ("Samsung", "S22"),
    ("Samsung", "S21"),
    ("Apple", "iPhone 14")
  )
  val sparkSession = SparkSession.builder()
    .appName("exercise")
    .config("spark.master", "local")
    .getOrCreate()

  val phonesDF = sparkSession.createDataFrame(smartPhonesSeq)
  phonesDF.show()
  phonesDF.printSchema()

  val moviesDF = sparkSession.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.printSchema()

  println(s"number of rows: ${moviesDF.count()}")


  System.exit(0)

  val input = "just a sample input with 42 for regex"
  val pattern = """.* (\d+).*""".r
  val pattern(answer) = input
  println(answer)


  val spark = SparkSession.builder()
    .appName("DataFramesBasicPractice")
    .config("spark.master", "local")
    .getOrCreate()

  // reading a dataframe
  val carsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json");

  carsDF.show()
  carsDF.printSchema()

  carsDF.take(10).foreach(row => println(row))
  var inferredSchema: StructType = carsDF.schema

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))
  val carsWithDefinedSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  carsWithDefinedSchema.show()

  val cars = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
  )
  val manualCarsDF = spark.createDataFrame(cars)
  manualCarsDF.show()

  import spark.implicits._
  val manualCarsDfWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")
  manualCarsDfWithImplicits.show()


}
