package day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 这个也没咋看懂
 */
object CustomizedSorting6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CustomizedSorting")
    val sc = new SparkContext(conf)

    val data: RDD[(String, Int, Int)] = sc.parallelize(Array(("zhangsan", 28, 9999), ("lisi", 28, 999), ("wanger", 29, 999), ("zhangmazi", 28, 99)))
    implicit val rules = Ordering[(Int, Int)].on[(String, Int, Int)](t =>(-t._3, t._2))
    val sorted: RDD[(String, Int, Int)] = data.sortBy(u => u)
    println(sorted.collect().toBuffer)
    sc.stop()
  }

}
