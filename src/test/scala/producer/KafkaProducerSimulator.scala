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

    def getRandomString(length : Int){
      val str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
      val random=new Random()
      val sb=new StringBuffer()
      for (0 <- length){
        val number = random.nextInt(62)
        sb.append(str.charAt(number))
      }
      sb.toString()
    }
    // Send some messages
    while (true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val appendString = getRandomString(3)
        val random = Math.random()
        val str = s"113.77.233.31 ^^A 29/Mar/2017:19:28:44 +0800 ^^A GET /api/User/UpdateUserInfo  ^^A - ^^A 200 ^^A 692 ^^A - ^^A ZZZZZZZZZZZZZZZZZZZZZZZZZZZZ+${appendString} ^^A PE-TL20 ^^A Huawei ^^A deviceid/zzzzzzzzzzzzzzzzzzzz  ^^A - ^^A ${random} ^^A ${random} "
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }
      Thread.sleep(1000)
    }

  }
}