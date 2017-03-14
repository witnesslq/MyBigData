package tech.zhangchi.bigdata.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject
import tech.zhangchi.bigdata.conf.Constants
import tech.zhangchi.bigdata.entity.Condition
import tech.zhangchi.bigdata.analyse.UserVisitAnalyze
import tech.zhangchi.bigdata.utils.InitUtils

/**
  * Created by zhangchi on 17/3/12.
  */
object Test {

  def main(args: Array[String]): Unit = {

    /**
      * 获取 SparkContext SQLContext
      */
    val conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //初始化
    InitUtils.init(sc, sqlContext)

    /**
      * 任务条件（JSON格式:{'startDate':['2016-11-22'],'endDate':['2017-10-23'],
      * 'sex':['female'],'professional':['professional14','professional13'],
      * 'search_keyword':['小米5','蚊帐','U盘']}）
      */
    //    val taskParam = new JSONObject("{'startDate':['2016-11-22'],'endDate':['2017-10-23'],'sex':['female'],'professional':['professional14','professional13'],'search_keyword':['小米5','蚊帐','U盘']}")
    val taskParam = new JSONObject("{'startDate':['2016-11-22'],'endDate':['2017-10-23'],'search_keyword':['蚊帐']}")

    //获取条件（将JSON格式条件解析）
    val condition = new Condition(taskParam)

    //获取指定条件的Click表RDD
    val sessionRdd = UserVisitAnalyze.getSessionIdRdd(sqlContext, condition)

    //获取指定条件的User表RDD
    val userRdd = UserVisitAnalyze.getUserRdd(sqlContext,condition)

    //讲两个表按SessionId为Key聚合
    val aggregatedRdd = UserVisitAnalyze.getAggregatedRdd(sessionRdd, userRdd, sqlContext)

    println(aggregatedRdd.collect().toBuffer)

    println(UserVisitAnalyze.accumulate(sc, aggregatedRdd))


    sc.stop()

  }
}
