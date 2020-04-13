package day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 利用了元组的特性，直接进行排序
 */
object CustomizedSorting1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CustomizedSorting")
    val sc = new SparkContext(conf)

    val data: RDD[(String, Int, Int)] = sc.parallelize(Array(("zhangsan", 28, 9999), ("lisi", 28, 999), ("wanger", 29, 999), ("zhangmazi", 28, 99)))
    val sorted: RDD[(String, Int, Int)] = data.sortBy(i => (-i._3, i._2))
    println(sorted.collect().toBuffer)
    sc.stop()
  }

}
