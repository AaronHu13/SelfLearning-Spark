package day4

import java.sql.{Connection, DriverManager, PreparedStatement}

import scala.io.{BufferedSource, Source}

object MyUtils {
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def readRules(path: String): Array[(Long, Long, String)] = {
    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //对ip规则进行整理，并放入到内存
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }

  def binarySearch(lines: Array[(Long, Long, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def rdd2DataBase(it: Iterator[(String, Int)]) = {
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/ssm?serverTimezone=UTC", "root", "123123123")
    val pstm: PreparedStatement = conn.prepareStatement("insert into access_log values(?, ?)")
    it.foreach(i => {
      pstm.setString(1, i._1)
      pstm.setInt(2, i._2)
      pstm.execute()
    })
    if(pstm != null) {
      pstm.close()
    }

    if(conn != null) {
      conn.close()
    }

  }

}
