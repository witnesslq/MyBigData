package tech.zhangchi.bigdata.accumulator

import org.apache.spark.AccumulatorParam
import tech.zhangchi.bigdata.conf.Constants
import tech.zhangchi.bigdata.utils.StringUtils

/**
  * Created by zhangchi on 17/3/13.
  */
object SessionStatAccumulator extends AccumulatorParam[String] {


  override def addInPlace(r1: String, r2: String): String = {

    try {
      /**
        * 如果初始值为空,那么返回v2
        */
      if (r1 == "") {
        r2
      } else {
        /**
          * 从现有的连接串中提取v2所对应的值
          */
        val oldValue = StringUtils.getStringPara(r2, r1)
        /**
          * 累加1
          */
        val newValue = oldValue.toInt + 1
        /**
          * 改链接串中的v2设置新的累加后的值
          */
        StringUtils.setStringPara(r2, newValue.toString, r1)
      }
    } catch {
      case ex: NumberFormatException => {
        r2
      }
    }
  }

  override def zero(initialValue: String): String = {
    Constants.SESSION_COUNT + "=0|" + Constants.TIME_PERIOD_1s_3s + "=0|" + Constants.TIME_PERIOD_4s_6s + "=0|" + Constants.TIME_PERIOD_7s_9s + "=0|" +
      Constants.TIME_PERIOD_10s_30s + "=0|" + Constants.TIME_PERIOD_30s_60s + "=0|" + Constants.TIME_PERIOD_1m_3m + "=0|" + Constants.TIME_PERIOD_3m_10m +
      "=0|" + Constants.TIME_PERIOD_10m_30m + "=0|" + Constants.TIME_PERIOD_30m + "=0|" + Constants.STEP_PERIOD_1_3 + "=0|" + Constants.STEP_PERIOD_4_6 + "=0|" +
      Constants.STEP_PERIOD_7_9 + "=0|" + Constants.STEP_PERIOD_10_30 + "=0|" + Constants.STEP_PERIOD_30_60 + "=0|" + Constants.STEP_PERIOD_60 + "=0"
  }

}
