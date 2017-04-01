package com.hb.utils

import java.io._
import java.util.Properties
import org.apache.log4j.Logger


/**
  * Created by Simon on 2017/4/1.
  */
class ConfigProvider(filePath : String) {
  val file = filePath
  val logger = Logger.getLogger(this.getClass.getName.stripSuffix("$"))

  def getValueByKey(key: String) = {
    try {
      val properties = new Properties()
      val fs = new File(file)
      val reader = new BufferedReader(new FileReader(fs))
      properties.load(reader)
      properties.get(key)
      reader.close()
    }
    catch {
      case e: FileNotFoundException => {
        logger.error("file does not exists" + e)
        null
      }
      case e: IOException => {
        logger.error("IO error while loading conf.properties" + e)
        null
      }
    }
  }

}
