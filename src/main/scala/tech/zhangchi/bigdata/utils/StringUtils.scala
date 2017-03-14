package tech.zhangchi.bigdata.utils

/**
  * Created by zhangchi on 17/3/12.
  */
object StringUtils {

  def trimComa(str: String): String = {
    if (str == "") null
    else str.substring(0, str.length - 1)
  }

  def getStringPara(key: String, str: String): String = {
    val array = str.split("\\|").map(r => {
      val kv = r.split("=")
      kv(0) -> kv(1)
    })
    for (each <- array) {
      if (each._1.equals(key)) return each._2
    }
    null
  }

  def setStringPara(key: String, value: String, str: String): String = {
    var result = ""
    val array = str.split("\\|").map(r => {
      val kv = r.split("=")
      kv(0) -> kv(1)
    })
    for (each <- array) {
      if (each._1.equals(key)) result += s"${each._1}=$value|"
      else result += s"${each._1}=${each._2}|"
    }
    result.substring(0, result.length - 1)

  }


}
