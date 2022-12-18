package part7bigdata

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object TaxiApplicationPractice1 extends App {

  /*
  first you need to download the data
  if you search for "nyc taxi data", you'll find "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page" that contains real data in NYC
  but above data is huge and may not be consistent, you can find a consistent data below:
  go to https://academictorrents.com/, in the right-hand-side, search for "taxi" and then select "New York Taxi Data 2009-2016 in Parquet Fomat"

  Columns are as below:
  A big DataFrame containing all taxi rides
  • tpep_pickup_datetime = pickup timestamp
  • tpep_dropoff_datetime = dropoff timestamp
  • passenger_count
  • trip_distance = length of the trip in miles
  • RatecodeID = 1 (standard), 2 (JFK), 3 (Newark), 4 (Nassau/Westchester) or 5 (negotiated)
  • PULocationID = pickup location zone ID
  • DOLocationID = dropoff location zone ID
  • payment_type = 1 (credit card), 2 (cash), 3 (no charge), 4 (dispute), 5 (unknown), 6 (voided)
  • total_amount
  • ... and 8 other columns

  Questions:
  1. Which zones have the most pickups/dropoffs overall?
  2. What are the peak hours for taxi?
  3. How are the trips distributed by length? Why are people taking the cab?
  4. What are the peak hours for long/short trips?
  5. What are the top 3 pickup/dropoff zones for long/short trips?
  6. How are people paying for the ride, on long/short trips?
  7. How is the payment type evolving with time?
  8. Can we explore a ride-sharing opportunity by grouping close short trips?

  taxi DF
  +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+
  |VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|
  +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+
  |2       |2018-01-25 00:02:56 |2018-01-25 00:10:58  |1              |2.02         |1         |N                 |48          |107         |2           |8.5        |0.5  |0.5    |0.0       |0.0         |0.3                  |9.8         |
  |2       |2018-01-25 00:57:13 |2018-01-25 01:21:17  |1              |10.13        |1         |N                 |79          |244         |2           |28.5       |0.5  |0.5    |0.0       |0.0         |0.3                  |29.8        |
  |2       |2018-01-25 02:29:32 |2018-01-25 02:41:29  |1              |3.38         |1         |N                 |239         |48          |1           |13.0       |0.0  |0.5    |2.76      |0.0         |0.3                  |16.56       |

  zones DF
  +----------+-------------+-----------------------+------------+
  |LocationID|Borough      |Zone                   |service_zone|
  +----------+-------------+-----------------------+------------+
  |1         |EWR          |Newark Airport         |EWR         |
  |2         |Queens       |Jamaica Bay            |Boro Zone   |
  |3         |Bronx        |Allerton/Pelham Gardens|Boro Zone   |

   */



  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("TaxiApplication")
    .config("spark.master", "local")
    .getOrCreate()

  // default format while calling "load" method is parquet.
  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiDF.show(false)
  println("count: " + taxiDF.count())

  val taxiZonesDF = spark.read
    .option("header", "true")
    .csv("src/main/resources/data/taxi_zones.csv")
  taxiZonesDF.show(false)


  println("1. Which zones have the most pickups/dropoffs overall")
  val groupedByZone = taxiDF.groupBy(col("PULocationID"))
    .agg(count("*").as("total_rides"))
    .join(taxiZonesDF, col("PULocationID") === taxiZonesDF.col("LocationID"))
    .drop("PULocationID", "LocationID", "service_zone")
    .orderBy(col("total_rides").desc_nulls_last)
  //    .limit(1)
  //    .show(false)

  // now we can even group them by "Borough"
  groupedByZone.groupBy(col("Borough"))
    .agg(sum("total_rides").as("total_rides"))
    .orderBy(col("total_rides").desc_nulls_last)
    .show(false)


  println("2. What are the peak hours for taxi")
  taxiDF.withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy(col("hour_of_day"))
    .agg(count("*").as("total"))
    .orderBy(col("total").desc_nulls_last)
    .show(false)


  println("3. How are the trips distributed by length? Why are people taking the cab")
  val longDistanceThreshold = 30
  taxiDF.select(col("trip_distance").as("distance"))
    .select(
      count("*").as("total"),
      min("distance").as("min"),
      max("distance").as("max"),
      avg("distance").as("avg"),
      stddev("distance").as("stddev"),
    )
    .show(false)

  val tripsWithLengthDF = taxiDF.withColumn("isLong", col("trip_distance") >= longDistanceThreshold)
  tripsWithLengthDF
    .groupBy("isLong")
    .agg(count("*"))
    .show(false)


  println("4. What are the peak hours for long/short trips")
  tripsWithLengthDF.groupBy(hour(col("tpep_pickup_datetime")), col("isLong"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)
    .show(100, truncate = false)

  println("5. What are the top 3 pickup/dropoff zones for long/short trips?")
  tripsWithLengthDF
    // we don't consider long trips
    .where(not(col("isLong")))
    .groupBy(col("PULocationID"), col("DOLocationID"))
    .agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === taxiZonesDF.col("LocationID"))
    .withColumnRenamed("Borough", "Borough_PU")
    .withColumnRenamed("Zone", "Zone_PU")
    .drop("LocationID", "service_zone")
    .join(taxiZonesDF, col("DOLocationID") === taxiZonesDF.col("LocationID"))
    .withColumnRenamed("Borough", "Borough_DO")
    .withColumnRenamed("Zone", "Zone_DO")
    .drop("LocationID", "service_zone", "PULocationID", "DOLocationID")
    .orderBy(col("totalTrips").desc_nulls_last)
    .show(3, false)


  println("6. How are people paying for the ride, on long/short trips")
  tripsWithLengthDF.groupBy(col("payment_type"), col("isLong"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)
    .show(false)


  println("7. How is the payment type evolving with time")
  // provided data is for only one day, so we need to have more data in order to be more meaningful!
  // we're grouping by day and payment_type, so for each day, we're going to get some payment_types
  // we need some tools to visualize this data
  taxiDF.groupBy(to_date(col("tpep_pickup_datetime")).as("pickup_day"), col("payment_type"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("pickup_day"))


  println("8. Can we explore a ride-sharing opportunity by grouping close short trips")

  val groupAttemptsDF = taxiDF
    .select(
      // unix_timestamp is number of seconds since 1970/1/1
      // we're getting a 5 minutes (5 * 60 = 300) bucket_id => because we want to find all the trips that have been in the same zone id (pickup location) and have a maximum time distance of 5 minutes,
      // it means we want to find trips that have been started almost at the same time in the same zone id
      // we're rounding it to get an integer instead of double and then we explicitly cast it
      round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").as("fiveMinId"),
      col("PULocationID"),
      col("total_amount")
    )
    .where(col("passenger_count") < 3)
    .groupBy(col("fiveMinId"), col("PULocationID"))
    .agg(count("*").as("total_trips"), sum(col("total_amount")).as("total_amount"))
    // getting back approximate time from fiveMinId
    .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
    .drop("fiveMinId")
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")
    .orderBy(col("total_trips").desc_nulls_last)
    .show(false)




}
