package day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object GroupPopularTeacherV4 {
  def main(args: Array[String]): Unit = {
    /**
     * 自定义分区器：让相同的学科都进入相同的分区
     * 且在reduced的时候就进行分区，减少shuffle的次数，降低网络传输的负载
     *
     */
    val input = "teacher.log"
    val output = "output"
    val conf = new SparkConf().setAppName("GlobalPopularTeacher")
    conf.setMaster("local[4]")
    //创建spark执行的入口
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(input)
    // 切分压平
    // "."是特殊字符，需要转义
    val subjectAndTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val splitRes = line.split("/")
      val subject = splitRes(2).split("\\.")(0)
      val teacher = splitRes(3)
      ((subject, teacher), 1)
    })

    //计算有多少学科
    val subjects: Array[String] = subjectAndTeacherAndOne.map(_._1._1).distinct().collect()
    //自定义一个分区器，并且按照指定的分区器进行分区
    val sbPatitioner = new SubjectParitioner(subjects);

    //聚合，将学科和老师联合当做key, partitionBy按照指定的分区规则进行分区
    val reduced: RDD[((String, String), Int)] = subjectAndTeacherAndOne.reduceByKey(sbPatitioner, _ + _)

    //如果一次拿出一个分区(可以操作一个分区中的数据了)
    val sorted: RDD[((String, String), Int)] = reduced.mapPartitions(it => {
      // 将迭代器转换成list，然后排序，在转换成迭代器返回
      // 这里不还是有可能会爆掉内存嘛--可以创建一个最小堆，遍历数据从而避免爆内存
      it.toList.sortBy(_._2).reverse.take(3).iterator
    })

    // 分区数和学科数量一致
    sorted.saveAsTextFile(output)
//    val r: Array[((String, String), Int)] = sorted.collect()
//    println(r.toBuffer)

    sc.stop()
  }
}


