import java.io.{File, FileInputStream}
import java.util
import java.util.{Date, Properties}

import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import com.hb.model.{IPMapping, IpToLong, LocationInfo}
import com.hb.pool.ConnectionPool
import consumer.kafka.ProcessedOffsetManager
import consumer.kafka.ReceiverLauncher
import com.hb.falcon.{Pack, Sender}
import com.hb.utils.Pencentile

/**
  * Created by Simon on 2017/2/23.
  */

object NginxFlowAnalysisTest {
  private var ssc : StreamingContext = null


  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: spark-2.0.0/bin/spark-submit --class com.hb.analysis.NginxFlowAnalysis --master yarn --num-executors 4 --executor-memory 8G --executor-cores 4 --driver-memory 1000M  log-analysis.jar --files conf/log4j.properties --files conf/conf.properties")
      //      System.exit(0)
    }

//    PropertyConfigurator.configure("conf/log4j.properties")

    System.setProperty("hadoop.home.dir", "C:\\Program Files (x86)\\Hadoop")
    //获取应用相关配置
    val in = new FileInputStream(new File("conf/conf.properties"))
    val properties = new Properties
    properties.load(in)

    val master = properties.getProperty("master")
    val zkHosts = properties.getProperty("zkAddress").split(",").map(line => line.split(":")(0)).mkString(",")
    val zkPort = properties.getProperty("zkAddress").split(",")(0).split(":")(1)
    val zkAddress = properties.getProperty("zkAddress")
    val group = properties.getProperty("group")
    val url = properties.getProperty("falconUrl")
    val topic = properties.getProperty("topics")
    val numberOfReceivers = properties.getProperty("numberOfReceivers").toInt

    val splitColumns = properties.getProperty("splitColumns")
    val percentileNums = properties.getProperty("percentileNums")


    //split提取ip,请求api,状态码,设备id,时延五个维度的数据
    val column1 = splitColumns.split(",")(0).toInt
    val column2 = splitColumns.split(",")(1).toInt
    val column3 = splitColumns.split(",")(2).toInt
    val column4 = splitColumns.split(",")(3).toInt
    val column5 = splitColumns.split(",")(4).toInt


    val kafkaProperties: Map[String, String] =
      Map("zookeeper.hosts" -> zkHosts,
        "zookeeper.port" -> zkPort,
        "kafka.topic" -> topic,
        "zookeeper.consumer.connection" -> zkAddress,
        "kafka.consumer.id" -> group
      )

    val props = new java.util.Properties()
    kafkaProperties foreach { case (key, value) => props.put(key, value) }

    val conf = new SparkConf().setAppName("NginxPerformanceMonitor")
      .setMaster(master)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    ssc = new StreamingContext(conf, Seconds(60))

    ssc.checkpoint("analysisCheckpoint")

    val messages = ReceiverLauncher.launch(ssc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY)
    val partitonOffset_stream = ProcessedOffsetManager.getPartitionOffset(messages, props)
    ProcessedOffsetManager.persists(partitonOffset_stream, props)

    /**
      * 分三个JOB计算指标。
      */
    val filterMessages = messages.map { x => new String(x.getPayload) }
      .filter(s => s.contains("GET") || s.contains("POST"))
      .map(line => line.split("\\^\\^A"))
      .map(line => Array(line(column1), line(column2).split(" ")(1), line(column3), line(column4), line(column5))).cache()
    println("test-filterMessages")
    filterMessages.print()
    ssc.start()
    ssc.stop()
  }}
