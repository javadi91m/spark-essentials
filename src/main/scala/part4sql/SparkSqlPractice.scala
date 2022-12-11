package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkSqlPractice extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")

    // when we create databases through spark-sql, spark will create a "warehouse" directory in in which stores all the data related to that DB
    // by default it'll create it in "src" side-by-side, but we can override it and tell it where to store
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    // only for Spark 2.4 users:
    // .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  val carsDF = spark.read.json("src/main/resources/data/cars.json")

  // regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")


  // by calling "createOrReplaceTempView", spark will create an alias for this DF so it can refer to it as a table
  carsDF.createOrReplaceTempView("cars")

  // you can run ANY sql command and get a DF as result
  val americanCarsDF = spark.sql(
    """
      |select * from cars where Origin = 'USA'
      |""".stripMargin
  )
  americanCarsDF.show()

  // you can even create databases and tables
  spark.sql("create database rtjvm") // returns an empty dataframe

  // you can run any sql commands
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")

  databasesDF.show()



  // transfer tables from a DB to Spark tables

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  // now we can read a dataframe from an external source (PostgreSQL) and then write it to spark-sql as a MANAGED table
  // it'll be saved in rtjvm database, since we're using rtjvm db
  val employeesDF = readTable("employees")

//  employeesDF.write
//    .mode(SaveMode.Overwrite)
//    .saveAsTable("employees")

//  spark.sql("describe extended employees")
//    .show() // it's a MANAGED table in spark


  // we can write a method to read tables and store them in spark as MANAGED tables
  // in this way tables will be loaded in memory
  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)

    if (shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager")
  )


  // now we can even read these Spark tables as dataframes
  val employeesDF2 = spark.read.table("employees")
  employeesDF2.show(false)


}
