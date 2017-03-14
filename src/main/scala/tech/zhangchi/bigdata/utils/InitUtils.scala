package tech.zhangchi.bigdata.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import tech.zhangchi.bigdata.conf.{Constants, ConfManager}

/**
  * Created by zhangchi on 17/3/11.
  */
object InitUtils {

  /**
    * load my.properties
    */
  ConfManager.loadProperties()

  /**
    * sessionschema
    */
  val sessionschema = StructType(
    List(
      StructField("date", StringType, true),
      StructField("user_id", LongType, true),
      StructField("session_id", StringType, true),
      StructField("page_id", LongType, true),
      StructField("action_date", StringType),
      StructField("action_time", StringType, true),
      StructField("search_keyword", StringType, true),
      StructField("click_category_id", StringType, true),
      StructField("click_product_id", StringType, true),
      StructField("order_category_ids", StringType, true),
      StructField("order_product_ids", StringType, true),
      StructField("pay_category_ids", StringType, true),
      StructField("pay_product_ids", StringType, true),
      StructField("city_id", LongType, true)
    )
  )

  /**
    * userschema
    */
  val userschema = StructType(
    List(
      StructField("user_id", LongType, true),
      StructField("username", StringType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("professional", StringType, true),
      StructField("city", StringType, true),
      StructField("sex", StringType, true)
    )
  )

  /**
    * productchema
    */
  val productchema = StructType(
    List(
      StructField("product_id", LongType, true),
      StructField("product_title", StringType, true),
      StructField("extend_info", StringType, true)
    )
  )

  /**
    * function init
    *
    * @param sc
    * @param sqlContext
    */
  def init(sc: SparkContext, sqlContext: SQLContext): Unit = {


    /**
      * create click table
      */
    val sessionRDD = sc.textFile(ConfManager.getString(Constants.LOCAL_SESSION_DATA_PATH)).map(_.split(" "))

    val sessionrowRDD = sessionRDD.map(s => Row(s(0).trim, s(1).toLong, s(2).trim,
      s(3).toLong, s(4).trim, s(5).trim, s(6).trim, s(7).trim, s(8).trim,
      s(9).trim, s(10).trim, s(11).trim, s(12).trim, s(13).toLong))
    val sessionDataFrame = sqlContext.createDataFrame(sessionrowRDD, sessionschema)
    sessionDataFrame.registerTempTable(Constants.TABLE_USER_VISIT)

    /**
      * create User table
      */
    val userRDD = sc.textFile(ConfManager.getString(Constants.LOCAL_USER_DATA_PATH)).map(_.split(" "))
    val userrowRdd = userRDD.map(u => Row(u(0).toLong, u(1).trim, u(2).trim, u(3).toInt, u(4).trim,
      u(5).trim, u(6).trim))
    val userDataFrame = sqlContext.createDataFrame(userrowRdd, userschema)
    userDataFrame.registerTempTable(Constants.TABLE_USER)

    /**
      * create Product table
      */
    val productRDD = sc.textFile(ConfManager.getString(Constants.LOCAL_PRODUCT_DATA_PATH)).map(_.split(" "))
    val productrowRdd = userRDD.map(u => Row(u(0).toLong, u(1).trim, u(2).trim))
    val productDataFrame = sqlContext.createDataFrame(productrowRdd, productchema)
    productDataFrame.registerTempTable(Constants.TABLE_PRODUCT)

  }

}
