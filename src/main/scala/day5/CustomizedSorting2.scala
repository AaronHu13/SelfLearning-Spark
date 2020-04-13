package day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将数据转化为一自定义的排序类，重写类的compare方法随后进行排序
 */
object CustomizedSorting2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CustomizedSorting")
    val sc = new SparkContext(conf)

    val data: RDD[(String, Int, Int)] = sc.parallelize(Array(("zhangsan", 28, 9999), ("lisi", 28, 999), ("wanger", 29, 999), ("zhangmazi", 28, 99)))
    val mapped: RDD[User] = data.map(i => new User(i._1, i._2, i._3))
    val sorted: RDD[User] = mapped.sortBy(u => u)
    println(sorted.collect().toBuffer)
    sc.stop()
  }

}

class User(val name: String, val age: Int, val fv: Int) extends Ordered[User] with Serializable {

  override def compare(that: User): Int = {
    if(this.fv != that.fv){
      return (that.fv - this.fv)
    }
    (this.age - that.age)
  }

  override def toString: String = s"name: $name, age: $age, fv: $fv"
}
