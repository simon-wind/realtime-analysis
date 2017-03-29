package com.hb.falcon

/**
  * Created by Simon on 2017/2/24.
  */
case class NginxFalcon(endpoint: String,
                       metric: String,
                       timestamp: Int,
                       step: Int,
                       value: Double,
                       counterType: String ,
                       tags: String )

