package part5lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.io.Source

object RDDsPractice extends App {

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


}
