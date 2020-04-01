package day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object GroupPopolarTeacherV5 {
  val K = 3
  val ord = Ordering.by[((String, String), Int), Int](_._2).reverse
  def main(args: Array[String]): Unit = {
    val input = "teacher.log"
    val output = "output"
    val conf = new SparkConf().setAppName("GlobalPopularTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(input)
    val subjectAndTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val splitRes: Array[String] = line.split("/")
      val subject: String = splitRes(2).split("\\.")(0)
      val teacher: String = splitRes(3)
      ((subject, teacher), 1)
    })

    def putToHeap(heap: mutable.PriorityQueue[((String, String), Int)], iter: ((String,String), Int)): Unit = {
      if (heap.nonEmpty && heap.size == K) {
        if (heap.head._2 < iter._2) {
          heap += iter
          heap.dequeue()
        }
      } else {
        heap += iter
      }
    }
    val subjects: Array[String] = subjectAndTeacherAndOne.map(_._1._1).distinct().collect()
    val subjectParitioner = new SubjectParitioner2(subjects)

    val reduced: RDD[((String, String), Int)] = subjectAndTeacherAndOne.reduceByKey(subjectParitioner, _ + _)

    val res: RDD[((String, String), Int)] = reduced.mapPartitions((it: Iterator[((String, String), Int)]) => {
      val heap = new mutable.PriorityQueue[((String, String), Int)]()(ord)
      while (it.hasNext) {
        val n = it.next
        putToHeap(heap, n)
      }
      heap.toList.sortBy(_._2).reverse.iterator
    })
    println(res.collect().toBuffer)
  }
}

class SubjectParitioner2(sbs: Array[String]) extends Partitioner {
  val partitionMap = new mutable.HashMap[String, Int]()
  var index = 0
  for(sb <- sbs) {
    partitionMap(sb) = index
    index += 1
  }
  override def numPartitions: Int = sbs.length

  override def getPartition(key: Any): Int = {
    val subject: String = key.asInstanceOf[(String, String)]._1
    partitionMap(subject)
  }
}
