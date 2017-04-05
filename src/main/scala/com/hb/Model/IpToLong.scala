package com.hb.Model

import java.net.InetAddress

/**
  * Created by Simon on 2017/4/1.
  */
object IpToLong {
  /**
    * @param ipAddress ip 字符串
    * @return ip long
    */
  def IPv4ToLong(ipAddress: String): Long = {
    val addrArray: Array[String] = ipAddress.split("\\.")
    var num: Long = 0
    var i: Int = 0
    while (i < addrArray.length) {
      val power: Int = 3 - i
      num = num + ((addrArray(i).toInt % 256) * Math.pow(256, power)).toLong
      i += 1
    }
    num
  }

  /**
    * @param ip 整数ip地址
    * @return ip 点分十进制ip地址
    */

  def LongToIPv4 (ip : Long) : String = {
    val bytes: Array[Byte] = new Array[Byte](4)
    bytes(0) = ((ip & 0xff000000) >> 24).toByte
    bytes(1) = ((ip & 0x00ff0000) >> 16).toByte
    bytes(2) = ((ip & 0x0000ff00) >> 8).toByte
    bytes(3) = (ip & 0x000000ff).toByte
    InetAddress.getByAddress(bytes).getHostAddress()
  }

}
