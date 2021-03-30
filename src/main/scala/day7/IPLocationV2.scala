package day7

import day4.MyUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object IPLocationV2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IPLocationV1")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val rulePath = "ip.txt"
    val rules: Dataset[String] = spark.read.textFile(rulePath)
    val ipAndLocations: DataFrame = rules.map(line => {
      val fields: Array[String] = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    }).toDF("StartNum", "EndNum", "Province")

    val logPath = "access.log"
    val ips: Dataset[String] = spark.read.textFile(logPath)
    val ipTable: DataFrame = ips.map(line => {
      val fileds = line.split("[ - -]")
      val ip = fileds(0)
      val ipNum = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    ipAndLocations.createTempView("v_ip_location")
    ipTable.createTempView("v_ips")

    val res: DataFrame = spark.sql("select v_ip_location.Province, count(*) as number FROM v_ip_location JOIN v_ips ON (v_ips.ip_num >= v_ip_location.StartNum and v_ips.ip_num <= v_ip_location.EndNum) GROUP BY Province ORDER BY number DESC")

    println(res.collect().toBuffer)
  }
}
