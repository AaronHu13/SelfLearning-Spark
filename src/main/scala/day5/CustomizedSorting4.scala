package day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 不将数据转化为自定义类，而只是在排序时传入一个排序的规则
 * 注意，这里类可以不实现序列化，因为用的是case class（默认实现了序列化），且不需要用new关键字来创新实例
 */
object CustomizedSorting4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CustomizedSorting")
    val sc = new SparkContext(conf)

    val data: RDD[(String, Int, Int)] = sc.parallelize(Array(("zhangsan", 28, 9999), ("lisi", 28, 999), ("wanger", 29, 999), ("zhangmazi", 28, 99)))
    val sorted: RDD[(String, Int, Int)] = data.sortBy(u => User03(u._2, u._3))
    println(sorted.collect().toBuffer)
    sc.stop()
  }

}

case class User03(age: Int, fv: Int) extends Ordered[User03] {

  override def compare(that: User03): Int = {
    if (this.fv != that.fv) {
      return (that.fv - this.fv)
    }
    (this.age - that.age)
  }
}
