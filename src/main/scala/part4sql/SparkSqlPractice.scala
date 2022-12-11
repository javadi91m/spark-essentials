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


  // now we can even read these LOADED Spark tables as dataframes
  val employeesDF2 = spark.read.table("employees")
  employeesDF2.show(false)


  /**
    * Exercises
    *
    * 1. Read the movies DF and store it as a Spark table in the rtjvm database.
    * 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000.
    * 3. Show the average salaries for the employees hired in between those dates, grouped by department.
    * 4. Show the name of the best-paying department for employees hired in between those dates.
    */


  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("movies")

  // 2
  spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1999-01-01' and hire_date < '2000-01-01'
    """.stripMargin
  )

  // 3
  spark.sql(
    """
      |select de.dept_no, avg(s.salary)
      |from employees e, dept_emp de, salaries s
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      |group by de.dept_no
    """.stripMargin
  )

  // 4
  // note: on sql, when we have groupedBy, we are allowed to select only that field in select clause + aggregate functions
  // here group by is on d.dept_name , so we're allowed to select that field only (+ aggregation functions of course)
  spark.sql(
    """
      |select avg(s.salary) payments, d.dept_name
      |from employees e, dept_emp de, salaries s, departments d
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      | and de.dept_no = d.dept_no
      |group by d.dept_name
      |order by payments desc
      |limit 1
    """.stripMargin
  ).show()

}
