package part3typesdatasets

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import part3typesdatasets.Datasets.carsDS
import part3typesdatasets.DatasetsPractice_4.carsDS

import java.sql.Date

object DatasetsPractice_4 extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  /*
  Datasets are essentially typed DataFrames: distributed collection of JVM OBJECTS,
  so far we've been working with DataFrames which are distributed collections of ROWS

  Dataset comes with a big cost: PERFORMANCE! => spark can't optimize transformations
  when we use operations on datasets (for example filter), these operations will be run on plan scala OBJECTS,
  means they will be evaluated only at RUNTIME, and it's after spark has the chance to plan for the operations in advance,
  so spark will be have to do all these operations on a row by roe basis, which is very slow
  all these means spark cannot do OPTIMIZATIONS ON TRANSFORMATIONS!!!
  so if you need performance => use DataFrames!

  in DataSet, we access to all the methods in DataFrame, because DataFrame itself is a DataFrame of Row:
  type DataFrame = Dataset[Row]
   */

  val numbersDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()
  /*
  root
   |-- numbers: integer (nullable = true)
   */

  // the problem with DataFrames is that we need resort only with DF API functions, nothing more
  // we can transform DF to a Dataset and then ew can have a distributed JVM objects and do what we want on it!

  // this Encoder has this capability to transform a row in a DF into an Int object
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  numbersDS.filter(_ < 100)
    .show()


  // dataset of a complex type

  // 1. define a case class => fields of this class need to have the exact same name in the data file
  // 2. create a DF from your data source
  // 3. define an encoder (import spark.implicits._)
  // 4. convert your DF to DS
  case class Car(
                  // String implies this field may never be null => be careful! if one rows contains null for this field, we'll face an exception!
                  Name: String,
                  // Option[Double] means that this field maybe be null
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: Date, // java.sql.Date
                  Origin: String
                )

  // 2 - read the DF from the file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  // we need to either explicitly case the Date columns (Year)
  // or we need to pass a manual schema in which we can define "Year" as Date
  val carsDF = readDF("cars.json")
    .withColumn("Year", col("Year").cast(DateType))

  // 3 - define an encoder (importing the implicits)

  // Encoders.product accepts any type that extends Product => all case classes extend Product
  implicit val carEncoder = Encoders.product[Car]

  // OR: import all implicits
  //  import spark.implicits._

  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]
  carsDS.filter(_.Cylinders <= 4)
    .show()

  // for map operation you need to import spark.implicits._
  carsDS.map(car => {
    val newCar = car.copy(Name = car.Name.toUpperCase())
    newCar
  }).show()

  // OR: instead of copying the current car, changing it and then returning a new car, we can import spark.implicits._ and:
  // this returns a Dataset containing only ONE FIELD

  import spark.implicits._

  carsDS.map(car => car.Name.toUpperCase())
    .show()


  /**
    * Exercises
    *
    * 1. Count how many cars we have
    * 2. Count how many POWERFUL cars we have (HP > 140)
    * 3. Average HP for the entire dataset
    */

  // 1.
  val allCount = carsDS.count();
  println(s"all cars count: ${allCount}")


  // 2.
  val powerfulCarsCount = carsDS.filter(_.Horsepower.exists(_ > 140))
    .count()
  println(s"powerful cars count: ${powerfulCarsCount}")


  // 3.
  val avgHP = carsDS
    // if you use "0" instead of "0L", you're going to get an exception: Unable to find encoder for type AnyVal. An implicit Encoder[AnyVal] is needed to store AnyVal instances in a Dataset. Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in future releases
    .map(_.Horsepower.getOrElse(0L))
    .reduce(_ + _) / allCount

  println(s"average HP: ${avgHP}")

  // also use the DF functions!
  // in this way, all rows with null values will be ignored and then even won't be in the count as well
  carsDS.select(avg(col("Horsepower")))


  // Joins

  // note: when you use inferSchema: true, spark will read all int numbers as BigInt (Long), so if you model them with Int in your case class, you're going to face an exception
  case class Guitar(id: Long, make: String, model: String, `type`: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val bandsDS = readDF("bands.json").as[Band]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]

  // calling "join" method on a DataSet results a DataFrame, but "joinWith", results a Dataset
  // joinWith returns a Tuple of first DS and Second
  val joined: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")

  /*
  +--------------------+--------------------+
  |                  _1|                  _2|
  +--------------------+--------------------+
  |{1, ANGUS YOUNG, ...|{1, AC/DC, Sydney...|
  |{0, JIMMY PAGE, [...|{0, Led Zeppelin,...|
  |{3, KIRK HAMMETT,...|{3, Metallica, Lo...|
  +--------------------+--------------------+

  you can see there are only two columns: _1 , _2 (obviously you can rename them by withColumnRenamed)
  the value for each of these two columns is an array of individual fields
   */

  val mappedJoined = joined.map(tuple => {
    val guitarPlayer = tuple._1
    val band = tuple._2
    val newGuitarPlayer = guitarPlayer.copy(name = guitarPlayer.name.toUpperCase())
    (newGuitarPlayer, band)
  })

  mappedJoined.show()


  /**
    * Exercise: join the guitarsDS and guitarPlayersDS, in an outer join
    * (hint: use array_contains)
    */

  val guitarWithGuitarPlayersJoined = guitarPlayersDS
    .joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")

  guitarWithGuitarPlayersJoined.show()


  // Grouping DS

  val carsGroupedByOrigin_oldWay = carsDS
    .groupBy(col("Origin"))
    .count()
    .show()

  val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin)
    .count() // => Dataset[(K, Long)]
    .show()

  // joins and groups are WIDE transformations, will involve SHUFFLE operations

}
