package day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupPopularTeacherV2 {
  def main(args: Array[String]): Unit = {
    /**
     * 适用于数据量比较大，可能会压爆内存的情况
     */
    val input = "teacher.log"
    // 假设实际运行是从配置文件/数据库进行读取的
    val subjects = Array("javaee", "bigdata", "php")
    // 创建Spark配置
    val conf = new SparkConf().setAppName("GlobalPopularTeacher")
    conf.setMaster("local[4]")
    //创建spark执行的入口
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(input)
    //切分压平
    // "."是特殊字符，需要转义
    val subjectAndTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val splitRes = line.split("/")
      val subject = splitRes(2).split("\\.")(0)
      val teacher = splitRes(3)
      ((subject, teacher), 1)
    })

    val reduced: RDD[((String, String), Int)] = subjectAndTeacherAndOne.reduceByKey(_ + _)
    // 需要提交多次任务，消耗比较大
    for(subject <- subjects) {
      val filteredBigdata: RDD[((String, String), Int)] = reduced.filter(_._1._1 == subject)
      val sorted: Array[((String, String), Int)] = filteredBigdata.sortBy(_._2, false).take(3)
      println(sorted.toBuffer)
    }
    sc.stop()
  }
}
