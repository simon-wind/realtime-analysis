package producer

import com.hb.utils.IPRecord
import com.hb.utils.IPMapping

/**
  * Created by Simon on 2017/3/30.
  */
object IpMappingTest {
  def main(args:Array[String]) = {
    val a = IPMapping.getIpMapping("ipCity.properties")
  }

}
