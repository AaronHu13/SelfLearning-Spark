package day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 这个没咋看懂
 */
object CustomizedSorting5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CustomizedSorting")
    val sc = new SparkContext(conf)

    val data: RDD[(String, Int, Int)] = sc.parallelize(Array(("zhangsan", 28, 9999), ("lisi", 28, 999), ("wanger", 29, 999), ("zhangmazi", 28, 99)))
    import SortRules.OrderingUser04
    val sorted: RDD[(String, Int, Int)] = data.sortBy(u => User04(u._2, u._3))
    println(sorted.collect().toBuffer)
    sc.stop()
  }

}

case class User04(age: Int, fv: Int)
