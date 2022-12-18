package part6practical

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object TestDeployApp2 {

  /*
  a Spark cluster is kept in place by the spark-cluster MANAGER, which is series of processes. each process is started on a separate machine in the cluster.
  one of these process (nodes) is called driver and will manage the state of the entire cluster. the other processes (nodes) do the actual work related to the job.
  Cluster Driver: manages the state of the entire cluster (Blue box in 01.png)
  Cluster Workers: do the actual work related to the job (Green box in 01.png)

  spark supports three different cluster managers: Standalone, (Hadoop) YARN, (Apache) Meson

  Spark driver: (red dot in 01.png)
    - manages the state of the stages/tasks of the application
    - interfaces with the cluster manager

  Spark executors: (yellow dot in 01.png)
    - run the tasks assigned by the Spark driver
    - report their state and results to the driver
   =========================
   do not confuse cluster driver (driver-NODE) and cluster worker (worker-NODE) with spark driver and spark executors
   it means that first we need to designate one node as cluster driver and some other nodes as spark workers.
   then we need to deploy spark-driver one of the cluster nodes and then deploy spark executors into other cluster worker nodes.

   based on how we deploy driver/executors into cluster nodes (i.e. how we deploy red/yellow dots in blue/green boxes in 01.png), we can have three different EXECUTION MODES (it means how we can launch spark applications):
    - cluster (02.png):
      * the Spark driver (not cluster driver!) is launched on a worker node
      * the cluster manager is responsible for Spark processes
      in this way all communications between spark-driver and spark-executors will be done inside cluster and cluster manager will take care of running processes correctly
      In cluster mode, the driver runs on one of the worker nodes, and this node shows as a driver on the Spark Web UI of your application. cluster mode is used to run production jobs.

    - client (03.png):
      * spark-driver is on the client application (the application which submits the job)
      * the client is responsible for the spark processes and state management
      first client application talks to spark cluster manager and after that, cluster manager will spawn spark-driver on the client machine (and not the actual cluster),
      then spark cluster manager will take care to spin up the executors on the worker nodes,
      and after that, spark-driver starts to communicate with spark-executors residing on the cluster
      In client mode, the driver runs locally from where you are submitting your application using spark-submit command. client mode is majorly used for interactive and debugging purposes. Note that in client mode only the driver runs locally and all tasks run on cluster worker nodes.

    - local:
      the entire application runs on the same machine. you have as much parallelism as many cores you have on your computer
   */

  def main(args: Array[String]): Unit = {
    /**
      * Movies.json as args(0) (input path)
      * GoodComedies.json as args(1) (it's the output path)
      *
      * good comedy = genre == Comedy and IMDB > 6.5
      */

    if (args.length != 2) {
      println("Need input path and output path")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Test Deploy App")
      .getOrCreate()

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json(args(0))

    val goodComediesDF = moviesDF.select(
      col("Title"),
      col("IMDB_Rating").as("Rating"),
      col("Release_Date").as("Release")
    )
      .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6.5)
      .orderBy(col("Rating").desc_nulls_last)

    goodComediesDF.show

    goodComediesDF.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))
  }

  /*
   * Build a JAR to run a Spark application on the Docker cluster
   *
   *   - project structure -> artifacts, add artifact from "module with dependencies"
   *   - (important) check "copy to the output folder and link to manifest"
   *   - (important) then from the generated folder path, delete so that the folder path ends in src/
   *
   * Build the JAR: Build -> Build Artifacts... -> select the jar -> build
   * Copy the JAR and movies.json to spark-cluster/apps
   * (the apps and data folders are mapped to /opt/spark-apps and /opt/spark-data in the containers)
   *
   *
   * */

  /*
    * How to run the Spark application on the Docker cluster
    *
    * 1. Start the cluster
    * docker-compose up --scale spark-worker=3
    *
    * 2. Connect to the master node
    * docker exec -it spark-cluster-spark-master-1 bash
    *
    * 3. Run the spark-submit command


    /spark/bin/spark-submit \
    --class part6practical.TestDeployApp2 \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --verbose \
    --supervise \
    /opt/spark-apps/spark-essentials.jar /opt/spark-data/movies.json /opt/spark-data/goodMovies


    */
}
