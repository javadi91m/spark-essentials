package part2dataframes

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, expr, max}
import part2dataframes.Joins.{bandsDF, deptManagersDF, guitaristsBandsDF, guitaristsDF, guitarsDF, maxSalariesPerEmpNoDF}

object JoinsPractice_6 extends App {

  // Joining combines data from multiple dataframes
  // it means one (or more) column from table 1 (left DF) is compared with one (or more) column from table 2 (right DF)
  // if the condition passes, rows are combined, and non-matching rows will be discarded
  // Joining in Spark is wide transformation => data will be moved around across different nodes in cluster

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read.json("src/main/resources/data/guitars.json")
  val guitaristsDF = spark.read.json("src/main/resources/data/guitarPlayers.json")
  val bandsDF = spark.read.json("src/main/resources/data/bands.json")

  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")

  // joins
  guitaristsDF.join(bandsDF, joinCondition, "inner") // joinType parameter is optional, default value is "inner"; inner, left, right, outer
    .select(guitaristsDF.col("name").as("guitarists"), bandsDF.col("name").as("band"))
    .show()


  // semi-join => like the inner join, but only shows data from the left table
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")
    .show()

  // anti-join => it's the opposite of semi-join, meaning it only shows data from left table that there is no matching row for them in then right table
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")
    .show()




  // things to bear in mind
  // guitaristsBandsDF.select("id", "band").show // this crashes

  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristsDF.join(bandsDF, joinCondition, "inner")
    .drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))





  // joining when one of the columns is an array => using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))
    .show()


  /**
    * Exercises
    *
    * 1. show all employees and their max salary
    * 2. show all employees who were never managers
    * 3. find the job titles of the best paid 10 employees in the company
    */

  def getTableDF(tableName: String): DataFrame = {
    spark.read
      .format("jdbc")
      .options(Map(
        "driver" -> "org.postgresql.Driver",
        "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
        "user" -> "docker",
        "password" -> "docker",
        "dbtable" -> s"public.${tableName}",
      ))
      .load()
  }

  val salariesDF = getTableDF("salaries")
  val employeesDF = getTableDF("employees")
  val managersDF = getTableDF("dept_manager")
  val titlesDF = getTableDF("titles")

  // 1.

  println("1. show all employees and their max salary")
  val maxSalaryDF = salariesDF.groupBy("emp_no")
    .agg(max("salary").as("salary_max"))

  employeesDF.join(maxSalaryDF, employeesDF.col("emp_no") === maxSalaryDF.col("emp_no"))
    .show()

  // 2.

  println("2. show all employees who were never managers")
  employeesDF.join(managersDF, employeesDF.col("emp_no") === managersDF.col("emp_no"), "left_anti")
    .show()

  // 3.
  println("3. find the job titles of the best paid 10 employees in the company")

  val latestTitlesDF = titlesDF.groupBy("emp_no", "title")
    .agg(max("to_date"))

  val top10Salaries = maxSalaryDF.orderBy(col("salary_max").desc_nulls_last).limit(10)
  top10Salaries.join(latestTitlesDF, top10Salaries.col("emp_no") === latestTitlesDF.col("emp_no"))
    .show()


}
