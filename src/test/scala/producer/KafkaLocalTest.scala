package producer

/**
  * Created by Simon on 2017/2/24.
  */
import java.util.HashMap
import java.util.Date
import java.text.SimpleDateFormat

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
  *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
  *   <group> is the name of kafka consumer group
  *   <topics> is a list of one or more kafka topics to consume from
  *   <numThreads> is the number of threads the kafka consumer should use
  *
  * Example:
  *    `$ bin/run-example \
  *      org.apache.spark.examples.streaming.KafkaWordCount zoo01,zoo02,zoo03 \
  *      my-consumer-group topic1,topic2 1`
  */


object KafkaNginxLogProducer {

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
        val str1 = "113.77.233.31 ^^A 29/Mar/2017:19:28:44 +0800 ^^A GET /api/User/UpdateUserInfo HTTP/1.1 ^^A - ^^A 200 ^^A 692 ^^A - ^^A 092A122C0E6D8B1111635CB8484FAC10 ^^A PE-TL20 ^^A Huawei ^^A deviceid/d81edb61-61ae-451b-baf4-7cebc2e7cd22 os/android systemversion/6.0 androidversion/23 androidabi/armeabi-v7a appversion/3.9.6.0 signalType/4 beta/0 ^^A - ^^A 0.072 ^^A 0.072"
        val message1 = new ProducerRecord[String, String](topic, null, str1)
        producer.send(message1)

      }
      Thread.sleep(1000)
    }

  }
}