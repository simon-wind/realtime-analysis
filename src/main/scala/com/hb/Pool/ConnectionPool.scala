package com.hb.Pool

import java.io.{File, FileInputStream, InputStream}
import java.util.Properties

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.spark.SparkFiles

/**
  * Created by Simon on 2017/4/5.
  */
class ConnectionPool private(prop : Properties) extends Serializable{
  private val cpds:ComboPooledDataSource = new ComboPooledDataSource(true)
  private var in : InputStream = null
  try {
    cpds.setJdbcUrl(prop.getProperty("jdbcUrl"))
    cpds.setDriverClass(prop.getProperty("driverClass"))
    cpds.setUser(prop.getProperty("user"))
    cpds.setPassword(prop.getProperty("password"))
    cpds.setMaxPoolSize(prop.getProperty("maxPoolSize").toInt)
    cpds.setMinPoolSize(prop.getProperty("minPoolSize").toInt)
    cpds.setAcquireIncrement(prop.getProperty("acquireIncrement").toInt)
    cpds.setInitialPoolSize(prop.getProperty("initialPoolSize").toInt)
    cpds.setMaxIdleTime(prop.getProperty("maxIdleTime").toInt)
    } catch {
    case ex:Exception => ex.printStackTrace()
    }

  /**
    *从c3p0连接池里面获取一个连接
    */
  def getConnection  = {
    try {
      cpds.getConnection
    } catch {
      case ex : Exception => ex.printStackTrace
        null
    }
  }
}

object ConnectionPool {
  var connectionPool : ConnectionPool = null

  /**
    * 单例模式获取连接池对象
    * @return MysqlManager 连接池对象
    */
  def getConnectionPool (prop : Properties) : ConnectionPool = {
    synchronized{
      if (connectionPool == null) {
        connectionPool = new ConnectionPool(prop)
      }
    }
    connectionPool
  }

}
