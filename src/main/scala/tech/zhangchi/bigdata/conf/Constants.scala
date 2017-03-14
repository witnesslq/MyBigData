package tech.zhangchi.bigdata.conf

/**
  * Created by zhangchi on 17/3/11.
  */
object Constants {
  /**
    * Spark作业相关的常量
    */
  val FIELD_DATE = "date"
  val FIELD_USER_ID = "user_id"
  val FIELD_SESSION_ID = "session_id"
  val FIELD_SEARCH_KEYWORD = "search_keyword"
  val FIELD_CLICK_CATEGORY_ID = "click_category_id"
  val FIELD_ACTION_TIME = "action_time"
  val FIELD_CITY = "city"
  val FIELD_SEX = "sex"
  val FIELD_PROFESSIONAL = "professional"
  val FIELD_AGE = "age"
  val FIELD_VISIT_LENGTH = "visitLength"
  val FIELD_STEP_LENGTH = "stepLength"
  /**
    * 项目配置相关的常量
    */
  val JDBC_URL = "jdbc.driver"
  val JDBC_USER = "jdbc.driver"
  val JDBC_DRIVER = "com.mysql.jdbc.driver"
  val JDBC_PASSWORD = "jdbc.password"
  val JDBC_USER_PROD = ""
  val JDBC_URL_PROD = ""
  val JDBC_PASSWORD_PROD = ""

  val TABLE_USER_VISIT = "Click"
  val TABLE_USER = "User"
  val TABLE_PRODUCT = "Product"

  val SPARK_APP_NAME = "UserVisistAnalyze"
  val SPARK_LOCAL = "spark.local"

  //session分析任务类型
  val SPARK_LOCAL_SESSION_TASKID = "spark.local.taskid.session";
  val LOCAL_SESSION_DATA_PATH = "spark.local.session.data.path";
  val LOCAL_USER_DATA_PATH = "spark.local.user.data.path";
  val LOCAL_PRODUCT_DATA_PATH = "spark.local.product.data.path";


  val PARAM_START_DATE = "startDate"
  val PARAM_END_DATE = "endDate"
  val PARAM_START_AGE = "startAge"
  val PARAM_END_AGE = "endAge"
  val SESSION_COUNT: String = "session_count"
  val TIME_PERIOD_1s_3s: String = "1s_3s"
  val TIME_PERIOD_4s_6s: String = "4s_6s"
  val TIME_PERIOD_7s_9s: String = "7s_9s"
  val TIME_PERIOD_10s_30s: String = "10s_30s"
  val TIME_PERIOD_30s_60s: String = "30s_60s"
  val TIME_PERIOD_1m_3m: String = "1m_3m"
  val TIME_PERIOD_3m_10m: String = "3m_10m"
  val TIME_PERIOD_10m_30m: String = "10m_30m"
  val TIME_PERIOD_30m: String = "30m"
  val STEP_PERIOD_1_3: String = "1_3"
  val STEP_PERIOD_4_6: String = "4_6"
  val STEP_PERIOD_7_9: String = "7_9"
  val STEP_PERIOD_10_30: String = "10_30"
  val STEP_PERIOD_30_60: String = "30_60"
  val STEP_PERIOD_60: String = "60"

}
