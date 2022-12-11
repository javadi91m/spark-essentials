package part5lowlevel

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import part5lowlevel.RDDs.moviesRDD

import scala.io.Source

object RDDsPractice extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  /*
  RDD: Resilient Distributed Datasets: Distributed typed collections of JVM object
  RDD is the "first citizen" of Spark, it means all higher-level APIs (DataFrame, Dataset) reduce to RDDs

  Pros: can be highly optimized
    - partitioning can be controlled
    - order of elements can be controlled
    - order of operations matters for performance

  Cons:
    - they are hard to work with and for complex operations, we need to know the internals of spark
    - poor APIs for quick data processing, e.g. something that we can do with Dataframes with a single line of code, we need to write a lot and do lots of manual work in RDD

  for 99% of operations, use the DF/DS APIs, because spark is usually smart enough to optimize your operations without you needing to do anything at all
  if you are really familiar with spark and want to fine tune the operations to obtain better performance, then you need to use RDD
   */


  /*
  RDDs VS DataFrames VS Datasets
  we know that a DataFrame is actually a Dataset[ROW], so we'll just compare RDD and Dataset

  In common:
    - both RDD and DS are a distributed collection of JVM objects and the both have access to collection APIs: map, flatMap, filter, take, reduce, fold, ...
    - we can combine RDD/DS by using union and also call count, distinct on them
    - we can call groupBy, sortBy on them

  RDDs over Datasets
    - partition control: repartition, coalesce, partitioner, zipPartitions, mapPartitions
    - operation control: checkpoint, isCheckpointed, localCheckpoint, cache
    - storage control: cache, getStorageLevel, persist

  Datasets over RDDs:
    - select and join!
    - spark planning/optimization before running code
      Please NOTE: No input optimization engine for RDD. There is no provision in RDD for automatic optimization.
      It cannot make use of Spark advance optimizers like catalyst optimizer and Tungsten execution engine.
      We can optimize each RDD manually.

    For these two reasons, we usually use Datasets over RDDs
   */

  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  // the SparkContext is the entry point for low-level APIs, including RDDs
  val sc = spark.sparkContext

  // various ways of creating RDDs

  // 1 - parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)


  // 2 - reading from files
  case class StockValue(symbol: String, date: String, price: Double)

  def readStocks(filename: String) = {
    // plain scala code for reading file
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList
  }

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))


  // 2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    // this line is only for dropping the header
    // since RDD is a distributed set, we don't know which line is the header, i.e. the order won't be preserved after reading
    // a sample line: MSFT,Jan 1 2000,39.81
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))


  // 3 - read from a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._

  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  // you can obtain an RDD from a DF, but it will be an RDD of Rows:
  val stocksRDD4: RDD[Row] = stocksDF.rdd


  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers") // you lose the type info

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD) // you get to keep type info




  // Transformations

  // filtering
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // flazy transformation
  val msftCOunt = msftRDD.count() // eager action


  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // lazy transformation
  println(companyNamesRDD.take(10).mkString("Array(", ", ", ")"))


  // min and max
  // first we need to define an implicit comparator
  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan((first, second) => first.price < second.price)
  val minMSFT = msftRDD.min() // eager action
  println(s"min value: ${minMSFT}")


  // reducing
  val reduced = numbersRDD.reduce(_ + _)


  // grouping
  val groupedStocksRDD: RDD[(String, Iterable[StockValue])] = stocksRDD.groupBy(_.symbol)
  // very expensive, much like DF and DS => because of shuffling


  // Partitioning
  // RDDs have the capability of choosing how they are going to be partitioned

  /*
    Repartitioning
    in repartition we can increase/decrease number of partitions, that involved shuffling
    for decreasing number of partition, consider using coalesce
    Repartitioning is EXPENSIVE. Involves Shuffling.
    BEST PRACTICE: partition EARLY, then process that.
    BEST PRACTICE: Size of a partition should be 10-100MB. => by size of each partition we need to specify the number of partitions
   */
  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")


  // coalesce: coalesce will repartition an RDD to fewer partitions than currently it has
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // does NOT involve shuffling
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")


  /**
    * Exercises
    *
    * 1. Read the movies.json as an RDD.
    * 2. Show the distinct genres as an RDD.
    * 3. Select all the movies in the Drama genre with IMDB rating > 6.
    * 4. Show the average rating of movies by genre.
    */

  case class Movie(title: String, genre: String, rating: Double)

  // Read the movies.json as an RDD.
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF
    // we cannot directly transform moviesDF to a Dataset[Movie], because the number of columns is different and this makes spark crash, secondly the column names are different.
    .select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd


  // 2. Show the distinct genres as an RDD
  val distinctGenres = moviesRDD.map(_.genre).distinct()
    .reduce((a, b) => a + "," + b)
  println(s"distinctGenres: ${distinctGenres}")


  // 3. Select all the movies in the Drama genre with IMDB rating > 6.
  moviesRDD.filter(movie => movie.genre.eq("Drama") && movie.rating > 6)


  // 4. Show the average rating of movies by genre.
  case class GenreAvgRating(genre: String, rating: Double)

  val averageRating = moviesRDD.groupBy(_.genre)
    .map {
      case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
    }
  averageRating.toDF().show()


  // if we wanted to write it by using DF:
  moviesRDD.toDF.groupBy(col("genre")).avg("rating").show

}
