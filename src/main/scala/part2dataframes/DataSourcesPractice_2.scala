package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import part2dataframes.DataFramesBasicPractice_1.spark

object DataSourcesPractice_2 extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

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
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast") // failFast ; dropMalformed ; permissive (default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  val carsDF2 = spark.read
    .format("json")
    .schema(carsSchema)
    // this way you can populate options dynamically
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
    ))
    .load()

  carsDF2.show()

  // writing a DF
  carsDF2.write
    .format("json")
    .mode(SaveMode.Overwrite) // overwrite, append, ignore, errorIfExists
    .option("path", "src/main/resources/data/carsDUPLICATE.json")
    .save()


  val carsSchemaWithDate = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  // JSON FLAGS
  spark.read
    .schema(carsSchemaWithDate)
    // only applicable for defined schemas, not inferred ones
    .option("dateFormat", "YYYY-MM-DD")
    .option("allowSingleQuotes", "true")
    // it can read compressed files
    .option("compression", "uncompressed") // uncompressed (default) ; bzip2 ; gzip ; lz4 ; snappy ; deflate
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .schema(stocksSchema)
    .options(Map(
      "dateFormat" -> "Mon dd YYYY",
      "header" -> "true",
      "sep" -> ",",
      "nullValue" -> "",
    ))
    .csv("src/main/resources/data/stocks.csv")
    .show()

  // Parquet : it's an open-source compressed binary data storage format, optimized for fast reading of columns. it works very well with Spark
  // Parquet is the default storage format for data frames (for reading/writing)
  // Parquet has a big advantage: e can be sure that it's always in the right format
  // when data is stored in this way, it will be up to 10 times less in size
  // default compression is snappy in Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet")

  // every single line in the text file will be considered as value in a single column DataFrame
  spark.read
    .text("src/main/resources/data/sampleTextFile.txt")
    .show()
  /*
  +-----+
  |value|
  +-----+
  | this|
  |   is|
  |Scala|
  |  and|
  |    I|
  | love|
  |Spark|
  +-----+
   */


  // reading from a remote DB
  val employeesDF = spark.read
    .format("jdbc")
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> "public.employees",
    ))
    .load()

  employeesDF.show()

  /*
  // KAFKA
  val kafkaDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "test")
    .load()

  val query = kafkaDF.writeStream
    .outputMode("append")
    .format("console")
    .start()
  query.awaitTermination()
   */

  /* exercises
  read movies DF, then write it as
  - tab-separated CSV file
  - snappy parquet
  - table "public.movies" in Postgres
   */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .option("sep", "\t")
    .option("nullValue", "")
    .option("header", "true")
    .csv("src/main/resources/data/moviesCSV.csv")

  // default compression is snappy in Parquet
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/moviesParquet.parquet")

  moviesDF.write
    .format("jdbc")
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> "public.moviesWriting",
    ))
    .save()

}
