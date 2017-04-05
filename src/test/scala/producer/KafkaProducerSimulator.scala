package producer

/**
  * Created by Simon on 2017/2/24.
  */
import java.util.HashMap

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import scala.util.Random


/**
  * 模拟flume将日志传输到kafka,测试使用
  */


object KafkaProducerSimulator {

  def getRandomString(length : Int){
    val str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    val random=new Random()
    val sb=new StringBuffer()
     for (i <- (0 until length)){
      val number = random.nextInt(62)
      sb.append(str.charAt(number))
    }
    sb.toString()
  }

  def main(args: Array[String]) {

    val Array(brokers, topic, messagesPerSec) = Array("192.168.2.245:9092", "nginx", "100")

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Send some messages
    while (true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val appendString = getRandomString(3)
        val random = Math.random()
        val str = "183.0.227.212 ^^A 05/Apr/2017:17:12:59 +0800 ^^A GET /api/Message/GetUserMessageNotify HTTP/1.1 ^^A - ^^A 200 ^^A 166 ^^A - ^^A 7D9A692FA70A158C84E4FF61BC8A2490 ^^A iPhone6plus ^^A - ^^A deviceid/d41d8cd98f00b204e9800998ecf8427e4e8fe389 os/iOS manufacturer/Apple appversion/3.9.7 ip/192.168.9.102 systemversion/9.3.5 idfa/F6F2FBD8-17AA-4FCF-BD15-4AF2C8B5336E signalType/4 beta/0 ^^A - ^^A 0.163 ^^A 0.163"
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }
      Thread.sleep(1000)
    }

  }
}