package tech.zhangchi.bigdata.analyse


import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext}
import tech.zhangchi.bigdata.accumulator.SessionStatAccumulator
import tech.zhangchi.bigdata.conf.Constants
import tech.zhangchi.bigdata.entity.Condition
import tech.zhangchi.bigdata.utils.{DateUtils, StringUtils}

/**
  * Created by zhangchi on 17/3/11.
  */

object UserVisitAnalyze {

  /**
    * 获取指定条件的UserActionRDD
    * @param sqlContext
    * @param condition
    * @return
    */
  def getSessionIdRdd(sqlContext: SQLContext, condition: Condition): RDD[Row] = {
    val table = Constants.TABLE_USER_VISIT
    val sql = s"select * from $table " +
      s"${condition.getDate} ${condition.getSearchKeyWords} ${condition.getClickCategoryIds}"
    sqlContext.sql(sql).rdd
  }

  /**
    * 获取指定条件的UserRDD
    * @param sqlContext
    * @param condition
    * @return
    */
  def getUserRdd(sqlContext: SQLContext, condition: Condition): RDD[Row] = {
    val table = Constants.TABLE_USER
    val sql = s"select * from $table where age > 0" +
      s"${condition.getAge} ${condition.getProfessionals} ${condition.getSex} ${condition.getCites}"
    sqlContext.sql(sql).rdd
  }

  /**
    * 获取物品RDD
    * @param sqlContext
    * @return
    */
  def getProductRdd(sqlContext: SQLContext): RDD[Row] = {
    val table = Constants.TABLE_PRODUCT
    val sql = s"select * from $table"
    sqlContext.sql(sql).rdd
  }

  /**
    *
    * @param dateRangeRdd
    * @param userRdd
    * @param sqlContext
    * @return
    */
  def getAggregatedRdd(dateRangeRdd: RDD[Row],
                      userRdd: RDD[Row],
                      sqlContext: SQLContext): RDD[(String, String)] = {

    val sessionIdRdd = dateRangeRdd.map(r => (r.getString(2), r)).groupByKey()

    val userIdWithActionInfoRdd = sessionIdRdd.map { kv =>
      val sessionId = kv._1
      val iterator = kv._2.iterator
      var userId = 0l
      var searchKeyWords = ""
      var clickCategories = ""

      var startTime: Date = null
      var endTime: Date = null
      var stepLength = 0

      while (iterator.hasNext) {
        val row = iterator.next()
        userId = row.getLong(1)
        val searchKeyWord = row.getString(6)
        val clickCategory = row.getString(7)
        if (searchKeyWord != "null" && !searchKeyWords.contains(searchKeyWord)) {
          searchKeyWords += (searchKeyWord + ",")
        }
        if (clickCategory != "null" && !clickCategory.contains(searchKeyWord)) {
          clickCategories += (clickCategory + ",")
        }
        val actionTime = DateUtils.parseTime(s"${row.getString(4)} ${row.getString(5)}")

        if (startTime == null && endTime == null) {
          startTime = actionTime
          endTime = actionTime
        }
        else if (actionTime.before(startTime)) startTime = actionTime
        else if (actionTime.after(endTime)) endTime = actionTime
        stepLength += 1
      }

      val visitLength = (endTime.getTime - startTime.getTime) / 1000


      val trimmedSearchKeyWords = StringUtils.trimComa(searchKeyWords)
      val trimmedClickCategories = StringUtils.trimComa(clickCategories)

      val userInfo = s"${Constants.FIELD_SESSION_ID}=$sessionId|" +
        s"${Constants.FIELD_SEARCH_KEYWORD}=$trimmedSearchKeyWords|" +
        s"${Constants.FIELD_CLICK_CATEGORY_ID}=$trimmedClickCategories|" +
        s"${Constants.FIELD_VISIT_LENGTH}=$visitLength|" +
        s"${Constants.FIELD_STEP_LENGTH}=$stepLength"

      (userId, userInfo)

    }

    val userIdWithInfoRdd = userRdd.map(row => (row.getLong(0), row))
    val joinRDD = userIdWithActionInfoRdd.join(userIdWithInfoRdd)

    joinRDD.map(t => {

      val userActionInfo = t._2._1
      val userInfo = t._2._2

      val sessionID = StringUtils.getStringPara(Constants.FIELD_SESSION_ID, userActionInfo)
      val age = userInfo.getInt(3)
      val professional = userInfo.getString(4)
      val city = userInfo.getString(5)
      val sex = userInfo.getString(6)
      val fullInfo = s"$userActionInfo|" +
        s"${Constants.FIELD_AGE}=$age|" +
        s"${Constants.FIELD_PROFESSIONAL}=$professional|" +
        s"${Constants.FIELD_CITY}=$city|" +
        s"${Constants.FIELD_SEX}=$sex"

      (sessionID, fullInfo)

    })

  }

  /**
    * 已废弃
    */
//    def conditionAnalyse(sqlContext: SQLContext, condition: Condition): RDD[Row] = {
//    val tableSess、ion = Constants.TABLE_USER_VISIT
//    val tableUser = Constants.TABLE_USER
//    val sql = s"select * from $tableSession join $tableUser on $tableSession.user_id = $tableUser.user_id " +
//      s"${condition.getDate} ${condition.getSex} ${condition.getAge} ${condition.getProfessionals} " +
//      s"${condition.getCites} ${condition.getSearchKeyWords} ${condition.getClickCategoryIds}"
//    sqlContext.sql(sql).rdd
//  }
//
//  def getAggregatedJoinedRdd(searchRdd: RDD[Row]): RDD[(String, String)] = {
//
//    val aggregatedRdd = searchRdd.map(r => (r.getString(2), r)).groupByKey().map(kv => {
//
//      val sessionId = kv._1
//      val iterator = kv._2.iterator
//      var userId = 0l
//      var searchKeyWords = ""
//      var clickCategories = ""
//      var age = 0
//      var professional = ""
//      var city = ""
//      var sex = ""
//
//      var startTime: Date = null
//      var endTime: Date = null
//      var stepLength = 0
//
//      while (iterator.hasNext) {
//        val row = iterator.next()
//        userId = row.getLong(1)
//        val searchKeyWord = row.getString(6)
//        val clickCategory = row.getString(7)
//        if (searchKeyWord != "null" && !searchKeyWords.contains(searchKeyWord)) {
//          searchKeyWords += (searchKeyWord + ",")
//        }
//        if (clickCategory != "null" && !clickCategory.contains(searchKeyWord)) {
//          clickCategories += (clickCategory + ",")
//        }
//
//        val actionTime = DateUtils.parseTime(s"${row.getString(4)} ${row.getString(5)}")
//
//        if (startTime == null && endTime == null) {
//          startTime = actionTime
//          endTime = actionTime
//        }
//        else if (actionTime.before(startTime)) startTime = actionTime
//        else if (actionTime.after(endTime)) endTime = actionTime
//
//        stepLength += 1
//
//        age = row.getInt(17)
//        professional = row.getString(18)
//        city = row.getString(19)
//        sex = row.getString(20)
//      }
//
//      val visitLength = (endTime.getTime - startTime.getTime) / 1000
//
//      val trimmedSearchKeyWords = StringUtils.trimComa(searchKeyWords)
//      val trimmedClickCategories = StringUtils.trimComa(clickCategories)
//
//      val userInfo = s"${Constants.FIELD_SESSION_ID}=$sessionId|" +
//        s"${Constants.FIELD_SEARCH_KEYWORD}=$trimmedSearchKeyWords|" +
//        s"${Constants.FIELD_CLICK_CATEGORY_ID}=$trimmedClickCategories|" +
//        s"${Constants.FIELD_VISIT_LENGTH}=$visitLength|" +
//        s"${Constants.FIELD_STEP_LENGTH}=$stepLength|" +
//        s"${Constants.FIELD_AGE}=$age|" +
//        s"${Constants.FIELD_PROFESSIONAL}=$professional|" +
//        s"${Constants.FIELD_CITY}=$city|" +
//        s"${Constants.FIELD_SEX}=$sex"
//
//      (sessionId, userInfo)
//    })
//
//    aggregatedRdd
//  }


  def accumulate(sc: SparkContext, rdd: RDD[(String, String)]): String = {

    val sessionStatAccumulator = sc.accumulator("")(SessionStatAccumulator)

    rdd.foreach(v => {
      val visitLength = Integer.valueOf(StringUtils.getStringPara(Constants.FIELD_VISIT_LENGTH, v._2))
      val stepLength = Integer.valueOf(StringUtils.getStringPara(Constants.FIELD_STEP_LENGTH, v._2))

      sessionStatAccumulator.add(Constants.SESSION_COUNT)

      if (visitLength <= 3) sessionStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
      else if (visitLength <= 6) sessionStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
      else if (visitLength <= 9) sessionStatAccumulator.add(Constants.TIME_PERIOD_7s_9s)
      else if (visitLength <= 30) sessionStatAccumulator.add(Constants.TIME_PERIOD_10s_30s)
      else if (visitLength <= 60) sessionStatAccumulator.add(Constants.TIME_PERIOD_30s_60s)
      else if (visitLength <= 180) sessionStatAccumulator.add(Constants.TIME_PERIOD_1m_3m)
      else if (visitLength <= 600) sessionStatAccumulator.add(Constants.TIME_PERIOD_3m_10m)
      else if (visitLength <= 1800) sessionStatAccumulator.add(Constants.TIME_PERIOD_10m_30m)
      else sessionStatAccumulator.add(Constants.TIME_PERIOD_30m)

      if (stepLength <= 3) sessionStatAccumulator.add(Constants.STEP_PERIOD_1_3)
      else if (stepLength <= 6) sessionStatAccumulator.add(Constants.STEP_PERIOD_4_6)
      else if (stepLength <= 9) sessionStatAccumulator.add(Constants.STEP_PERIOD_7_9)
      else if (stepLength <= 30) sessionStatAccumulator.add(Constants.STEP_PERIOD_10_30)
      else if (stepLength <= 60) sessionStatAccumulator.add(Constants.STEP_PERIOD_30_60)
      else sessionStatAccumulator.add(Constants.STEP_PERIOD_60)
    })

    sessionStatAccumulator.value
  }

}