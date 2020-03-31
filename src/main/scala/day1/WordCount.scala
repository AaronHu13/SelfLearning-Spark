package day1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
//    val input = "hdfs://localhost:9001/l-gfs.txt"
//    val output = "hdfs://localhost:9001/output"
    // 创建Spark配置
    val conf = new SparkConf().setAppName("SparkWordCount")
    conf.setMaster("local[4]")
    //创建spark执行的入口
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))
    // val lines: RDD[String] = sc.textFile(input)
    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将单词和一组合
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    //按key进行聚合
    val reduced:RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //将结果保存到HDFS中
//    sorted.saveAsTextFile(output)
     sorted.saveAsTextFile(args(1))
    sc.stop()
  }
}
