package producer

import com.hb.Model.{IPMapping, IPRecord}

/**
  * Created by Simon on 2017/3/30.
  */
object IpMappingTest {
  def main(args:Array[String]) = {
    val a = IPMapping.getIpMapping("ipCity.properties")
  }

}
