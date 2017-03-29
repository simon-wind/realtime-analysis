package com.hb.falcon

import java.io.IOException
import java.util
import javax.xml.ws.http.HTTPException

import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.entity.StringEntity

/**
  * Created by Simon on 2017/3/1.
  */

object Sender {

  def sender (dataList:util.ArrayList[Any], url: String) = {
    val post = new HttpPost(url)
    post.setEntity(new StringEntity(dataList.toString))
    println("dataList is " + dataList.toString)
    post.setHeader("Content-Type","application/json")
    val client = new DefaultHttpClient

    try {
      val responce = client.execute(post)
    } catch {
      case e:HTTPException => e.printStackTrace()
      case e:IOException  => e.printStackTrace()
    } finally {
      post.releaseConnection()
    }
  }

}

