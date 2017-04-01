package com.hb.utils

/**
  * Created by Simon on 2017/4/1.
  */
object IpToInt {
  /**
    * @param ipAddress ip 字符串
    * @return ip int
    */
  def ip2Int(ipAddress: String): Int = {
    ipAddress.split("\\.").zipWithIndex.foldLeft(0) {
      case (result, (ip, index)) ⇒
        result | ip.toInt << (index * 8)
    }
  }

  /**
    * @param ip 整数ip地址
    * @return ip 点分十进制ip地址
    */
  def int2Ip(ip: Int): String = {
    s"${(0 to 3).map(i ⇒ (ip >> 8 * i) & 0xFF).mkString(".")}"
  }

}
