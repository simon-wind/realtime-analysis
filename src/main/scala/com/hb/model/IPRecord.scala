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
