package tech.zhangchi.bigdata.utils

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by zhangchi on 17/3/13.
  */
object DateUtils {
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def parseTime(time: String): Date = {
    try {
      return sdf.parse(time)
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    null
  }

  def before(time1: String, time2: String): Boolean = {
    try {
      val dateTime1 = sdf.parse(time1)
      val dateTime2 = sdf.parse(time2)
      if (dateTime1.before(dateTime2)) return true
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    false
  }

  def after(time1: String, time2: String): Boolean = {
    try {
      val dateTime1 = sdf.parse(time1)
      val dateTime2 = sdf.parse(time2)
      if (dateTime1.after(dateTime2)) return true
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    false
  }

}
