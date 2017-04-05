package com.hb.falcon


import com.google.gson.Gson

/**
  * Created by Simon on 2017/3/27.
  */
object Pack {
  /**
    * case class转换成json格式字符串
    * @param endpoint 终端名称
    * @param metric 指标名称
    * @param step 时间间隔
    * @param value 指标值
    * @param counterType  Counter or Gauge
    * @param tags tags
    * @return json格式字符串
    */
  def pack(endpoint: String,
           metric: String,
           step: Int,
           value: Double,
           counterType: String ,
           tags: String) = {
    val timestamp = (System.currentTimeMillis()/1000).toInt
    val nginxFalcon = new NginxFalcon(endpoint, metric, timestamp , step, value, counterType, tags)
    new Gson().toJson(nginxFalcon)
  }
}
