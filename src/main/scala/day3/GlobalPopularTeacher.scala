package day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GlobalPopularTeacher {
  def main(args: Array[String]): Unit = {
    val input = "teacher.log"
    val output = "output"
    // 创建Spark配置
    val conf = new SparkConf().setAppName("GlobalPopularTeacher")
    conf.setMaster("local")
    //创建spark执行的入口
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(input)
    //切分压平
    val words: RDD[String] = lines.flatMap(line => Array(line.split("/")(3)))
    //将单词和一组合
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    //按key进行聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    val top3 = sc.parallelize(sorted.take(3))
    top3.saveAsTextFile(output)
    sc.stop()
  }
}
