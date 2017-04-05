package com.hb.analysis

import java.io.{File, FileInputStream}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, Properties}

import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import com.hb.Model.{IPMapping, IpToLong, LocationInfo}
import com.hb.Pool.ConnectionPool
import consumer.kafka.ProcessedOffsetManager
import consumer.kafka.ReceiverLauncher
import com.hb.falcon.{Pack, Sender}
import com.hb.utils.Pencentile

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
      println("Usage: spark-2.0.0/bin/spark-submit --class com.hb.analysis.NginxFlowAnalysis --master yarn --num-executors 4 --executor-memory 8G --executor-cores 4 --driver-memory 1000M  log-analysis.jar --files conf/log4j.properties --files conf/conf.properties --files conf/c3p0.properties" )
      System.exit(0)
    }

    PropertyConfigurator.configure("conf/log4j.properties")

//    System.setProperty("hadoop.home.dir", "C:\\Program Files (x86)\\Hadoop")
    //获取应用相关配置
    val in= new FileInputStream(new File("conf/conf.properties"))
    val properties = new Properties
    properties.load(in)

    val master = properties.getProperty("master")
    logger.info("master address is : " + master)
    val zkHosts = properties.getProperty("zkAddress").split(",").map(line => line.split(":")(0)).mkString(",")
    logger.info("zkHosts is : " + zkHosts)
    val zkPort = properties.getProperty("zkAddress").split(",")(0).split(":")(1)
    logger.info("zkPort is : " + zkPort)
    val zkAddress = properties.getProperty("zkAddress")
    logger.info("zkAddress is : " + zkAddress)
    val group = properties.getProperty("group")
    logger.info("consumer group id is : " + group)
    val url = properties.getProperty("falconUrl")
    logger.info("falcon http interface  is : " + url)
    val topic = properties.getProperty("topics")
    logger.info("consumer topic  is : " + topic)
    val numberOfReceivers = properties.getProperty("numberOfReceivers").toInt
    logger.info("numberOfReceivers  is : " + numberOfReceivers)

    val splitColumns = properties.getProperty("splitColumns")
    val percentileNums = properties.getProperty("percentileNums")


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

    val inC3p0 = new FileInputStream(new File("conf/c3p0.properties"))
    val propC3p0 = new Properties()
    propC3p0.load(inC3p0)
    val propC3p0BroadCast = ssc.sparkContext.broadcast(propC3p0)


    val ipMap = IPMapping.getIpMapping(ip_file)
    val ipMapBroadCast = ssc.sparkContext.broadcast(ipMap)

    ssc.checkpoint("analysisCheckpoint")

    val messages = ReceiverLauncher.launch(ssc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY)
    val partitonOffset_stream = ProcessedOffsetManager.getPartitionOffset(messages, props)
    logger.info("fetching current offset from zookeeper cluster")
    ProcessedOffsetManager.persists(partitonOffset_stream, props)

    /**
      * 分三个JOB计算指标。
      */
    val filterMessages = messages.map { x => new String(x.getPayload) }
      .filter(s => s.contains("GET") || s.contains("POST"))
      .map(line => line.split("\\^\\^A"))
      .map(line => Array(line(column1), line(column2).split(" ")(1), line(column3), line(column4), line(column5)))
    println("test")

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

    /**
      * 各省用户数（笛卡尔乘积比较耗cpu，暂时没想到好方法）
      */

    filterMessages.map(x => IpToLong.IPv4ToLong(x(0).trim)).map(x => (x,1)).reduceByKey(_+_)
      .map(x => (LocationInfo.findLocation(ipMapBroadCast.value,x._1),x._2))
      .reduceByKey(_+_) foreachRDD( rdd => {
      rdd.foreachPartition { data =>
        if (data != null) {
          val conn = ConnectionPool.getConnectionPool(propC3p0BroadCast.value).getConnection
          conn.setAutoCommit(false)
          val calendar = Calendar.getInstance();
          val now = calendar.getTime
          val currentTimestamp = new java.sql.Timestamp(calendar.getTime().getTime())

          val sql = "insert into uv_province(time,province,uv) values (?,?,?)"
          val preparedStatement = conn.prepareStatement(sql)
          data.foreach(r => {
            preparedStatement.setTimestamp(1, currentTimestamp)
            preparedStatement.setString(2, r._1.toString)
            preparedStatement.setInt(3, r._2)
            preparedStatement.addBatch()
          })

          preparedStatement.executeBatch()
          conn.commit()
          preparedStatement.clearBatch()
          conn.close()
        }
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


