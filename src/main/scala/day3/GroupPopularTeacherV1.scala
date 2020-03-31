package day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupPopularTeacherV1 {
  def main(args: Array[String]): Unit = {
    /**
     * 适用于数据量不大的情况
     */
    val input = "teacher.log"
    val output = "output"
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
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy((v: ((String, String), Int)) => v._1._1, 3)
    // toList会将executor的数据全部拉回到driver端，如果迭代器的数据特别多，可能会导致数据爆炸->面对这种场景程序需要做改进
    val groupedSortedTop3: RDD[(String, List[(String, Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.map(v => (v._1._2, v._2)).take(3))
    //    groupedSortedTop3.mapValues(v => (v.))
    val top3 = sc.parallelize(groupedSortedTop3.take(3))
    top3.saveAsTextFile(output)
    sc.stop()
  }
}
