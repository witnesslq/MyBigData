package tech.zhangchi.bigdata.conf

import java.io.FileInputStream
import java.util.Properties

/**
  * Created by zhangchi on 17/3/11.
  */
object ConfManager {

  val properties = new Properties()

  def loadProperties(): Unit = {
    val path = Thread.currentThread().getContextClassLoader.getResource("my.properties").getPath
    properties.load(new FileInputStream(path))
  }

  def getString(key: String): String = {
    properties.getProperty(key)
  }

  def getBoolean(key: String): Boolean = {
    properties.getProperty(key).toBoolean
  }

  def getInt(key: String): Long = {
    properties.getProperty(key).toLong
  }

}