package tech.zhangchi.bigdata.utils

import org.json.{JSONException, JSONObject}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangchi on 17/3/12.
  */
object JsonUtils {

  def getArray(json: JSONObject, key: String): ArrayBuffer[String] = {
    try {
      val jsonArray = json.getJSONArray(key)
      val num = jsonArray.length()
      val array = new ArrayBuffer[String]()
      if (jsonArray != null && jsonArray.length > 0) {
        for (i <- 0 until num) array += jsonArray.getString(i)
        return array
      }
      null
    } catch {
      case ex: JSONException => {
        //        ex.printStackTrace()
        null
      }
    }
  }

  def getParam(json: JSONObject, key: String): String = {
    try {
      val jsonArray = json.getJSONArray(key)
      if (jsonArray != null && jsonArray.length > 0) return jsonArray.getString(0)
      null
    } catch {
      case ex: JSONException => {
        //        ex.printStackTrace()
        null
      }
    }
  }

  def getString(json: JSONObject, key: String): String = {
    try {
      json.getString(key)
    } catch {
      case ex: JSONException => {
        //        ex.printStackTrace()
        null
      }
    }
  }

  def getLong(json: JSONObject, key: String): Long = {
    try {
      json.getLong(key)
    } catch {
      case ex: JSONException => {
        //        ex.printStackTrace()
        0L
      }
    }
  }

  def getInt(json: JSONObject, key: String): Int = {
    try {
      json.getInt(key)
    } catch {
      case ex: JSONException => {
        //        ex.printStackTrace()
        0
      }
    }
  }

}


