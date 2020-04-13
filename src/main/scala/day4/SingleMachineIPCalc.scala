package day4

import scala.io.{BufferedSource, Source}

object SingleMachineIPCalc {
  def main(args: Array[String]): Unit = {
    val rulePath = "ip.txt"
    val rules: Array[(Long, Long, String)] = MyUtils.readRules(rulePath)

    val logPath = "access.log"
    val bf: BufferedSource = Source.fromFile(logPath)
    val lines: Iterator[String] = bf.getLines()
    //对ip规则进行整理，并放入到内存
    val ips: Array[Long] = lines.map(line => {
      val fileds = line.split("[ - -]")
      val ip = fileds(0)
      val ipNum = MyUtils.ip2Long(ip)
      (ipNum)
    }).toArray

    val locationAndOne: Array[(String, Int)] = ips.map(ip => {
      val index = MyUtils.binarySearch(rules, ip)
      var province = "未知"
      if (index != -1) {
        province = rules(index)._3
      }
      (province, 1)
    })

    println(locationAndOne.toBuffer)
  }

}
