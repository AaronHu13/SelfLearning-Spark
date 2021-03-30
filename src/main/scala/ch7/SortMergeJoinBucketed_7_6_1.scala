package ch7

import org.apache.spark.sql.SparkSession


object SortMergeJoinBucketed_7_6_1 {
  // curried function to benchmark any code or function
  def benchmark(name: String)(f: => Unit) {
    val startTime = System.nanoTime
    f
    val endTime = System.nanoTime
    println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds")
  }

  // main class setting the configs
  def main (args: Array[String] ) {

    val spark = SparkSession.builder
      .appName("SortMergeJoinBucketed")
      .config("spark.sql.codegen.wholeStage", true)
      .config("spark.sql.join.preferSortMergeJoin", true)
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.defaultSizeInBytes", 100000)
      .config("spark.sql.shuffle.partitions", 16)
      .master("local[*]")
      .getOrCreate ()

    var states = scala.collection.mutable.Map[Int, String]()
    var items = scala.collection.mutable.Map[Int, String]()
    val rnd = new scala.util.Random(42)

    import spark.implicits._
    // Initialize states and items purchased

    states += (0 -> "AZ", 1 -> "CO", 2-> "CA", 3-> "TX", 4 -> "NY", 5-> "MI")
    items += (0 -> "SKU-0", 1 -> "SKU-1", 2-> "SKU-2", 3-> "SKU-3", 4 -> "SKU-4", 5-> "SKU-5")

    // Create DataFrames
    val usersDF = (0 to 1000000).map(id => (id, s"user_${id}", s"user_${id}@databricks.com", states(rnd.nextInt(5))))
      .toDF("uid", "login", "email", "user_state")
    val ordersDF = (0 to 1000000) .map(r => (r, r, rnd.nextInt(10000), 10 * r* 0.2d, states(rnd.nextInt(5)), items(rnd.nextInt(5))))
      .toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")

    //joinUsersOrdersBucketDF.explain("formatted")
    val usersOrdersDF = ordersDF.join(usersDF, $"users_id" === $"uid")
    usersOrdersDF.show(false)
    // uncomment to view the SparkUI otherwise the program terminates and shutdowsn the UI
    Thread.sleep(200000000)
  }
}