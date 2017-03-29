package com.hb.analysis

import java.io._
import java.util
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import consumer.kafka.ProcessedOffsetManager
import consumer.kafka.ReceiverLauncher

import com.hb.falcon.{Pack, Sender}
import com.hb.utils.Pencentile

/**
  * Created by Simon on 2017/2/23.
  */


object NginxFlowAnalysis {
  def getValueByKey(key: String) = {
    try {
      val properties = new Properties()
      val fs = new File("conf/conf.properties")
      val reader = new BufferedReader(new FileReader(fs))
      properties.load(reader)
      properties.get(key)
    }
    catch {
      case e: FileNotFoundException => {
        println("conf.propertoies file does not exists")
        null
      }
      case e: IOException => {
        println("IO error while loading conf.properties")
        null
      }
    }
  }

  val updateCardinal = (values: Seq[String], state: Option[HyperLogLogPlus]) => {
    if (state.nonEmpty) {
      val hll = state.get
      for (value <- values) { hll.offer(value) }
      Option(hll)
    } else {
      val hll = new HyperLogLogPlus(14)
      for (value <- values) { hll.offer(value) }
      Option(hll)
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: spark-2.0.0/bin/spark-submit --class com.hb.analysis.NginxFlowAnalysis --num-executors 4 --executor-memory 8G --executor-cores 4 --driver-memory 1000M  log-analysis.jar " )
    }

    val logger = Logger.getLogger(NginxFlowAnalysis.getClass.getName)
    PropertyConfigurator.configure("conf/log4j.properties")

        System.setProperty("hadoop.home.dir", "C:\\Program Files (x86)\\Hadoop")

    val master = getValueByKey("master").toString
    val zkHosts = getValueByKey("zkAddress").toString.split(",").map(line => line.split(":")(0)).mkString(",")
    val zkPort = getValueByKey("zkAddress").toString.split(",")(0).split(":")(1)
    val zkAddress = getValueByKey("zkAddress").toString
    val group = getValueByKey("group").toString
    val url = getValueByKey("falconUrl").toString
    val topic = getValueByKey("topics").toString
    val numberOfReceivers = getValueByKey("numberOfReceivers").toString.toInt

    logger.info("master address is : " + master)
    logger.info("zkHosts is : " + zkHosts)
    logger.info("zkPort is : " + zkPort)
    logger.info("zkAddress is : " + zkAddress)
    logger.info("consumer group id is : " + group)
    logger.info("falcon http interface address is : " + url)
    logger.info("consumer topic  is : " + topic)
    logger.info("numberOfReceivers  is : " + numberOfReceivers)


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
    logger.info("initializing spark config")

    val ssc = new StreamingContext(conf, Seconds(60))
    logger.info("starting spark streaming job")

    ssc.checkpoint("analysisCheckpoint")

    val messages = ReceiverLauncher.launch(ssc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY)
    val partitonOffset_stream = ProcessedOffsetManager.getPartitionOffset(messages, props)
    logger.info("fetching current offset from zookeeper")

    /**
      * 分三个JOB计算指标。
      */
    val filterMessages = messages.map { x => new String(x.getPayload) }
      .filter(s => s.contains("GET") || s.contains("POST"))
      .map(line => line.toString.split("\\^A"))
      .map(line => Array(line(0), line(2).split(" ")(1), line(4), line(7), line(12)))

    filterMessages.cache()
    filterMessages.print()
    //    import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER
    //    filterMessages.persist(MEMORY_AND_DISK_SER)

    filterMessages.foreachRDD(rdd => {
      val endpoint = "ngxmaster"
      val step = 60
      val counterType = "GAUGE"
      val tags = "_Minute"

      val metric1 = "pv_min"
      val metric2 = "errcounts"
      val metric3 = "errs"
      val metric4 = "uv_Min"
      val ls = new util.ArrayList[Any]
      /**
        * 计算每分钟请求数
        */
      val counts = rdd.count()
      val countsJson = Pack.pack(endpoint, metric1, step, counts, counterType,tags)
      ls.add(countsJson)

      /**
        * 计算每分钟错误请求数
        */
      val errRecords = rdd.filter(_(2).trim().toInt >= 400).cache()
      val errCounts = errRecords.count()
      val errCountsJson = Pack.pack(endpoint, metric2, step, errCounts, counterType,tags)
      ls.add(errCountsJson)

      /**
        * 计算每分钟不同错误请求数
        */
      val diffErrors = errRecords.map(x => (x(2), 1)).reduceByKey(_+_).collect()
      diffErrors.foreach{ x =>
        println("errrrecords key = " + x._1 + "value = " + x._2)
        ls.add(Pack.pack(endpoint, metric3 + x._1.trim, step, x._2.toString.toDouble, counterType,tags))
      }

      /**
        * 每分钟用户数
        */
      val uniqueVisitor =  rdd.map(x => (x(3),1)).reduceByKey(_ + _).count()
      val uniqueVisitorJson = Pack.pack(endpoint, metric4, step, uniqueVisitor, counterType,tags)
      ls.add(uniqueVisitorJson)

      Sender.sender(ls,url)

    }
    )

    filterMessages.foreachRDD(rdd => {
      /**
        * 各百分位时延迟,99,95,75,50百分位
        * 每分钟的数据量不大的时候,为简化逻辑,用repartition函数进行partition合并,在一个worker进行计算,数据量大了应分布式计算再合并
        */
      val metric5 = "pen99th"
      val metric6 = "pen95th"
      val metric7 = "pen75th"
      val metric8 = "pen50th"
      val endpoint = "ngxmaster"
      val step = 60
      val counterType = "GAUGE"
      val tags = "_Minute"

      rdd.map(x => x(4).toDouble).repartition(1).foreachPartition { partitionRecords =>

        val arrRecords = partitionRecords.toArray
        if (arrRecords.length > 0) {
          val ls = new util.ArrayList[Any]()
          val pen99th = Pencentile.percentile(arrRecords, 0.99)
          val pen95th = Pencentile.percentile(arrRecords, 0.99)
          val pen75th = Pencentile.percentile(arrRecords, 0.99)
          val pen50th = Pencentile.percentile(arrRecords, 0.99)

          val pen99thJson = Pack.pack(endpoint, metric5, step, pen99th, counterType,tags)
          val pen95thJson = Pack.pack(endpoint, metric6, step, pen95th, counterType,tags)
          val pen75thJson = Pack.pack(endpoint, metric7, step, pen75th, counterType,tags)
          val pen50thJson = Pack.pack(endpoint, metric8, step, pen50th, counterType,tags)

          ls.add(pen99thJson)
          ls.add(pen95thJson)
          ls.add(pen75thJson)
          ls.add(pen50thJson)

          Sender.sender(ls,url)

        } else {println("Do not have any latency records!")}
      }
    })

    /**
      * 总用户数UV，采用基数估计
      */
    filterMessages.map(x => (null, x(3))).updateStateByKey(updateCardinal)
      .map(x => x._2.cardinality).foreachRDD(rdd => {
      val metric9 = "uvTotal"
      val endpoint = "ngxmaster"
      val step = 60
      val counterType = "GAUGE"
      val tags = "_Minute"

      rdd.foreach { x =>
        val ls = new util.ArrayList[Any]
        val uvTotalJson = Pack.pack(endpoint, metric9, step, x, counterType, tags)
        ls.add(uvTotalJson)
        Sender.sender(ls, url)
      }
    }
    )
    //消费完成手动提交zookeeper的offset
    ProcessedOffsetManager.persists(partitonOffset_stream, props)
    logger.info("persist current offset in zookeeper cluster")

    ssc.start()
    ssc.awaitTermination()
  }
}


