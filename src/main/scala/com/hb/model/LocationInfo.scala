package com.hb.model

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Simon on 2017/4/1.
  */
object LocationInfo {
  /**
    * 通过IP获取所在省市
    * @param arrBuffer IPRecord数组
    * @param ipInt 点分十进制IP
    * @return
    */
  def findLocation(arrBuffer: ArrayBuffer[IPRecord],ipInt : Long) = {
    var loc : String = null
    var foundLocation : Boolean = false
    for (elems <- arrBuffer if !foundLocation ){
      if (elems.isIPMatch(ipInt)){
        loc = elems.getLocation
        foundLocation = true
      }
    }
    loc
  }
}
