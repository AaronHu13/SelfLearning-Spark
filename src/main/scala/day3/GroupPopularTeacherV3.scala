package day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object GroupPopularTeacherV3 {
  def main(args: Array[String]): Unit = {
    /**
     * 自定义分区器：让相同的学科都进入相同的分区
     *
     */
    val input = "teacher.log"
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

    //聚合，将学科和老师联合当做key
    val reduced: RDD[((String, String), Int)] = subjectAndTeacherAndOne.reduceByKey(_ + _)

    //计算有多少学科
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()

    //自定义一个分区器，并且按照指定的分区器进行分区
    val sbPatitioner = new SubjectParitioner(subjects);

    //partitionBy按照指定的分区规则进行分区
    //调用partitionBy时RDD的Key是(String, String)
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(sbPatitioner)

    //如果一次拿出一个分区(可以操作一个分区中的数据了)
    val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions(it => {
      // 将迭代器转换成list，然后排序，在转换成迭代器返回
      // 这里不还是有可能会爆掉内存嘛
      it.toList.sortBy(_._2).reverse.take(3).iterator
    })

    val r: Array[((String, String), Int)] = sorted.collect()
    println(r.toBuffer)
    sc.stop()
  }
}

//自定义分区器
class SubjectParitioner(sbs: Array[String]) extends Partitioner {

  //相当于主构造器（new的时候回执行一次）
  //用于存放规则的一个map
  val rules = new mutable.HashMap[String, Int]()
  var i = 0
  for (sb <- sbs) {
    rules(sb) = i
    i += 1
  }

  //返回分区的数量（下一个RDD有多少分区）
  override def numPartitions: Int = sbs.length

  //根据传入的key计算分区标号
  //key是一个元组（String， String）
  override def getPartition(key: Any): Int = {
    //获取学科名称
    val subject = key.asInstanceOf[(String, String)]._1
    //根据规则计算分区编号
    rules(subject)
  }
}

