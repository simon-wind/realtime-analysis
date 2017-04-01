package com.hb.analysis

import java.util

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
import com.hb.utils.{ConfigProvider, IPMapping, IpToInt, Pencentile,LocationInfo}

/**
  * Created by Simon on 2017/2/23.
  */

object NginxFlowAnalysis {
  private var ssc : StreamingContext = null
  //接口参数
  private val endpoint = "ngxmaster"
  private val step = 60
  private val counterType = "GAUGE"
  private val tags = "_Minute"
  private val ip_file = "/ipCity.properties"

  //计算指标
  private val metric1 = "pv_min"
  private val metric2 = "errcounts"
  private val metric3 = "errs"
  private val metric4 = "uv_Min"
  private val metric5 = "pen99th"
  private val metric6 = "pen95th"
  private val metric7 = "pen75th"
  private val metric8 = "pen50th"
  private val metric9 = "uvTotal"

  val logger = Logger.getLogger(NginxFlowAnalysis.getClass.getName)

  /**
    * 更新HyperLogLogPlus对象
    */
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
      println("Usage: spark-2.0.0/bin/spark-submit --class com.hb.analysis.NginxFlowAnalysis --master yarn --num-executors 4 --executor-memory 8G --executor-cores 4 --driver-memory 1000M  log-analysis.jar --files conf/log4j.properties --files conf/conf.properties" )
      System.exit(0)
    }

    PropertyConfigurator.configure(args(0))

//    System.setProperty("hadoop.home.dir", "C:\\Program Files (x86)\\Hadoop")
    val properties = new ConfigProvider(args(1))
    val master = properties.getValueByKey("master").toString
    val zkHosts = properties.getValueByKey("zkAddress").toString.split(",").map(line => line.split(":")(0)).mkString(",")
    val zkPort = properties.getValueByKey("zkAddress").toString.split(",")(0).split(":")(1)
    val zkAddress = properties.getValueByKey("zkAddress").toString
    val group = properties.getValueByKey("group").toString
    val url = properties.getValueByKey("falconUrl").toString
    val topic = properties.getValueByKey("topics").toString
    val numberOfReceivers = properties.getValueByKey("numberOfReceivers").toString.toInt

    logger.info("master address is : " + master)
    logger.info("zkHosts is : " + zkHosts)
    logger.info("zkPort is : " + zkPort)
    logger.info("zkAddress is : " + zkAddress)
    logger.info("consumer group id is : " + group)
    logger.info("falcon http interface address is : " + url)
    logger.info("consumer topic  is : " + topic)
    logger.info("numberOfReceivers  is : " + numberOfReceivers)

    val splitColumns = properties.getValueByKey("splitColumns").toString
    val percentileNums = properties.getValueByKey("percentileNums").toString


    //split提取ip,请求api,状态码,设备id,时延五个维度的数据
    val column1 = splitColumns.split(",")(0).toInt
    val column2 = splitColumns.split(",")(1).toInt
    val column3 = splitColumns.split(",")(2).toInt
    val column4 = splitColumns.split(",")(3).toInt
    val column5 = splitColumns.split(",")(4).toInt

    //各百分位指标
    val percentile1 = percentileNums.split(",")(0).toFloat
    val percentile2 = percentileNums.split(",")(1).toFloat
    val percentile3 = percentileNums.split(",")(2).toFloat
    val percentile4 = percentileNums.split(",")(3).toFloat


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

    ssc = new StreamingContext(conf, Seconds(60))
    logger.info("starting spark streaming job")

    val ipMap = IPMapping.getIpMapping(ip_file)
    val ipMapBroadCast = ssc.sparkContext.broadcast(ipMap)

    ssc.checkpoint("analysisCheckpoint")

    val messages = ReceiverLauncher.launch(ssc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY)
    val partitonOffset_stream = ProcessedOffsetManager.getPartitionOffset(messages, props)
    logger.info("fetching current offset from zookeeper cluster")

    /**
      * 分三个JOB计算指标。
      */
    val filterMessages = messages.map { x => new String(x.getPayload) }
      .filter(s => s.contains("GET") || s.contains("POST"))
      .map(line => line.split("\\^\\^A"))
      .map(line => Array(line(column1), line(column2).split(" ")(1), line(column3), line(column4), line(column5)))

    import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
    filterMessages.persist(MEMORY_AND_DISK)

    filterMessages.foreachRDD(rdd => {
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

      rdd.map(x => x(4).toDouble).repartition(1).foreachPartition { partitionRecords =>

        val arrRecords = partitionRecords.toArray
        if (arrRecords.length > 0) {
          val ls = new util.ArrayList[Any]()
          val pen99th = Pencentile.percentile(arrRecords, percentile1)
          val pen95th = Pencentile.percentile(arrRecords, percentile2)
          val pen75th = Pencentile.percentile(arrRecords, percentile3)
          val pen50th = Pencentile.percentile(arrRecords, percentile4)

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

        rdd.foreach { x =>
          val ls = new util.ArrayList[Any]
          val uvTotalJson = Pack.pack(endpoint, metric9, step, x, counterType, tags)
          ls.add(uvTotalJson)
          Sender.sender(ls, url)
      }
    }
    )

    filterMessages.map(x => IpToInt.ip2Int(x(0)))
      .map(x => LocationInfo.findLocation(ipMapBroadCast.value,x)).map(x => (x,1)).reduceByKey(_+_).print()

    //消费完成手动提交zookeeper的offset
    ProcessedOffsetManager.persists(partitonOffset_stream, props)
    logger.info("persist current offset in zookeeper cluster")

    ssc.start()
    ssc.awaitTermination()
  }
}


