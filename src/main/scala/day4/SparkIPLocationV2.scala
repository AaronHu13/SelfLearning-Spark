package day4

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkIPLocationV2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("IPLocationV2").setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf)
    val rulePath = "ip.txt"
    val rules: RDD[String] = sc.textFile(rulePath)
    val rangeAndLocation: RDD[(Long, Long, String)] = rules.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    })

    val ruleValues: Array[(Long, Long, String)] = rangeAndLocation.collect()
    val rulesBoardCast: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ruleValues)
    val logPath = "access.log"
    val ips: RDD[String] = sc.textFile(logPath)
    val provinceAndOne: RDD[(String, Int)] = ips.map(line => {
      val fileds = line.split("[ - -]")
      val ip = fileds(0)
      val ipNum = MyUtils.ip2Long(ip)
      val ruleValue: Array[(Long, Long, String)] = rulesBoardCast.value
      val index = MyUtils.binarySearch(ruleValue, ipNum)
      var province = "未知"
      if (index != -1) {
        province = ruleValue(index)._3
      }
      (province, 1)
    })

    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_ + _)
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)

    sorted.foreachPartition(it =>MyUtils.rdd2DataBase(it))
    println("done")
  }
}
