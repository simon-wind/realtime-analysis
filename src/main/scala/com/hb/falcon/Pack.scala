package com.hb.falcon


import com.google.gson.Gson

/**
  * Created by Simon on 2017/3/27.
  */
object Pack {
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
