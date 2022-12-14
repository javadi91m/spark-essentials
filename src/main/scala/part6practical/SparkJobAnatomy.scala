package part6practical

import org.apache.spark.sql.SparkSession

object SparkJobAnatomy extends App {

  /**
    * This is the code we wrote during the Spark Job Anatomy lecture.
    * We tested these RDDs, DataFrames and Datasets in the Spark shell in the cluster.
    *
    * This code is not meant to be run standalone (although of course you could).
    * It was mostly used to deconstruct Spark jobs and exemplify stages, tasks and the DAG.
    *
    * We will keep the code here for posterity so you can reference it later.
    * You can copy these expressions and paste them (even multi-line) into the Spark shell.
    */


  /*
  A job has multiple stages and each stage has multiple tasks
  Stage: stage is a set of computations in between shuffles
  shuffle: exchange of data between spark nodes
  task: a unit of computation, per partition => if we have 10 tasks in a stage, it means that we have 10 partitions

  DAG: Directed Acyclic Graph
    it's a graph of RDD dependencies, meaning the transformation of RDDs (how an RDD produced by previous RDD)

  we said Stages are delimited by shuffling, meaning per each shuffle, we have a new stage
  every stage has a unique id which is just a Number starting from 0 and then increments in every SparkSession

  if we create a DF or DS and then see the DAG, we'll notice that it has many more steps for the same,
  it's because when we use these higher level APIs, we don't have much of control over steps (Step: every single piece in DAG)

   */



  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Spark Job Anatomy")
    .getOrCreate()

  val sc = spark.sparkContext

  import spark.implicits._

  // start cluster
  // docker-compose up --scale spark-worker=3
  // in another terminal: docker-exec -it spark-cluster_spark-master_1 bash

  val rdd1 = sc.parallelize(1 to 1000000)
  rdd1.count
  // inspect the UI, one stage with 6 tasks; DAG in a single step
  // task = a unit of computation applied to a unit of data (a partition)

  rdd1.map(_ * 2).count
  // inspect the UI, another job with one stage, 6 tasks but one more step in the DAG - that's for the map

  rdd1.repartition(23).count
  // UI: 2 stages, one with 6 tasks, one with 23 tasks. Each stage is delimited by shuffles

  rdd1.toDF.show
  // suddenly from one step we get 5 steps: converting to DF does a lot of things behind the scenes

  val ds1 = spark.range(1, 1000000)
  // show the physical plan o
  ds1.explain
  ds1.show
  // one stage, one task

  /**
    *
    * Complex job 1
    * This executes two JOBS!
    * The Spark optimizer is able to pre-determine the job/stage/task planning before running any code.
    */
  val ds2 = spark.range(1, 100000, 2)
  val ds3 = ds1.repartition(7)
  val ds4 = ds2.repartition(9)
  val ds5 = ds3.selectExpr("id * 5 as id")
  val joined = ds5.join(ds4, "id")
  val sum = joined.selectExpr("sum(id)")
  sum.show

  /**
    * Complex job 2
    * This executes a single job with a massive DAG, and 6 stages:
    * - two for the toDF calls, 6 tasks each
    * - two for the repartitioning of both datasets (7 and 9 tasks respectively)
    * - one for the join (200 tasks) => when spark wants to join 2 RDD, it'll repartition both into 200 (default value) partitions
    *     here spark do a sort before joining, because joining two sorted sets are much faster that non-sorted
    * - one for the aggregation (1 task)
    *
    * The default number of partitions for a joined DF (and any unspecified repartition) is 200.
    * You can change it by setting the `spark.sql.shuffle.partitions` config.
    */
  val df1 = sc.parallelize(1 to 1000000).toDF.repartition(7)
  val df2 = sc.parallelize(1 to 100000).toDF.repartition(9)
  val df3 = df1.selectExpr("value * 5 as value")
  val df4 = df3.join(df2, "value")
  val sum2 = df4.selectExpr("sum(value)")
  sum2.show

}
