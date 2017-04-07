package com.hb.model

/**
  * Created by Simon on 2017/3/30.
  */
class IPRecord extends java.io.Serializable{
  private val IP_SEP = " "
  private val China = "中国"
  private var ipBegin : Long = 0
  private var ipEnd : Long = 0
  private var country : String = ""
  private var locProc : String = ""
  private var isForeign : Boolean = false

  /**
    * 初始化IP段和归属城市的对应关系
    * @param lineText  Ip Record
    * @return Boolean
    */
  def updateIPRecord(lineText:String) = {

    val lineSplitArray = lineText.split(IP_SEP)
    if (lineSplitArray.length <= 4) {
      false
    } else {
      this.ipBegin = lineSplitArray(0).toLong
      this.ipEnd = lineSplitArray(1).toLong
      val tempCountry = lineSplitArray(2)
      this.country = if (tempCountry.equals("*")) "unknown_country" else tempCountry
      val tempProc = lineSplitArray(4)
      val proc = if (tempProc.equals("*")) "unknown_proc" else tempProc

      if (country.equals("中国")) {
        this.locProc = proc
      } else {
        this.locProc = country
        this.isForeign = true
      }
      true
    }
  }

  /**
    * 判断Ip是否属于该IPRecord IP段
    * @param ip 整数型Ip
    * @return  Boolean
    */
  def isIPMatch(ip:Long) = {
    if (ip >= ipBegin && ip <= ipEnd ) true else false
  }

  override def toString: String = {
    ipBegin + " " + ipEnd + " " + locProc + " " + isForeign
  }

  def getLocation = {
    locProc
  }

}
