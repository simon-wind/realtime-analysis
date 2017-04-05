package com.hb.model

import java.io.{BufferedReader, IOException, InputStreamReader}

import org.apache.commons.lang.StringUtils

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Simon on 2017/3/30.
  */
object IPMapping {
  def getIpMapping(fileName:String) = {
    val IPArray = new ArrayBuffer[IPRecord]()
    val inputStream = IPMapping.getClass.getResourceAsStream(fileName)
    val bufferReader = new BufferedReader(new InputStreamReader(inputStream))
    var line : String = null
    try {
      line = bufferReader.readLine()
    } catch {
      case e: IOException => e.printStackTrace()
    }
    while (line != null) {
      line = StringUtils.trimToEmpty(line)
      if (!StringUtils.isEmpty(line)) {
        val record = new IPRecord()
        if (record.updateIPRecord(line)) {
          IPArray += record
        }

        try {
          line = bufferReader.readLine()
        } catch {
          case e:IOException => e.printStackTrace()
        }
      }
    }

    try {
      bufferReader.close()
    } catch {
      case e:IOException => e.printStackTrace()
    }
    IPArray
  }

}
