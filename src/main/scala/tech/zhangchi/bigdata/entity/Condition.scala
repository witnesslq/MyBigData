package tech.zhangchi.bigdata.entity

import org.json.JSONObject
import tech.zhangchi.bigdata.conf.Constants
import tech.zhangchi.bigdata.utils.JsonUtils

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangchi on 17/3/13.
  */
class Condition(val json: JSONObject) {

  /**
    * startDate,endDate,sex,startAge,endAge 是String对象
    * professional,city,searchKeyWord,clickCategoryId 是ArrayBuffer对象
    */

  val startDate: String = JsonUtils.getParam(json, Constants.PARAM_START_DATE)
  val endDate: String = JsonUtils.getParam(json, Constants.PARAM_END_DATE)
  val sex: String = JsonUtils.getParam(json, Constants.FIELD_SEX)
  val startAge: String = JsonUtils.getParam(json, Constants.PARAM_START_AGE)
  val endAge: String = JsonUtils.getParam(json, Constants.PARAM_END_AGE)
  val professionals: ArrayBuffer[String] = JsonUtils.getArray(json, Constants.FIELD_PROFESSIONAL)
  val cities: ArrayBuffer[String] = JsonUtils.getArray(json, Constants.FIELD_CITY)
  val searchKeyWords: ArrayBuffer[String] = JsonUtils.getArray(json, Constants.FIELD_SEARCH_KEYWORD)
  val clickCategoryIds: ArrayBuffer[String] = JsonUtils.getArray(json, Constants.FIELD_CLICK_CATEGORY_ID)

  var flag = true

  def getDate: String = {
    s"where ${Constants.FIELD_DATE}>='$startDate' and ${Constants.FIELD_DATE}<='$endDate'"
  }

  def getSex: String = {
    if (sex != null) {
      return s"and ${Constants.FIELD_SEX}='$sex'"
    }
    ""
  }

  def getAge: String = {
    if (startAge != null && endAge != null) {
      return s"and ${Constants.FIELD_AGE}>='$startAge' and ${Constants.FIELD_AGE}<='$endAge'"
    }
    ""
  }

  def getProfessionals: String = {
    if (professionals != null) {
      val len = professionals.length
      var result = "and ("
      for (i <- 0 until len) {
        if (i != len - 1) result += s"${Constants.FIELD_PROFESSIONAL} = '${professionals(i)}' or "
        else result += s"${Constants.FIELD_PROFESSIONAL} = '${professionals(i)}')"
      }
      return result
    }
    ""
  }

  def getCites: String = {
    if (cities != null) {
      val len = cities.length
      var result = "and ("
      for (i <- 0 until len) {
        if (i != len - 1) result += s"${Constants.FIELD_CITY} = '${cities(i)}' or "
        else result += s"${Constants.FIELD_CITY} = '${cities(i)}')"
      }
      return result
    }
    ""
  }

  def getSearchKeyWords: String = {
    if (searchKeyWords != null) {
      val len = searchKeyWords.length
      var result = "and ("
      for (i <- 0 until len) {
        if (i != len - 1) result += s"${Constants.FIELD_SEARCH_KEYWORD} = '${searchKeyWords(i)}' or "
        else result += s"${Constants.FIELD_SEARCH_KEYWORD} = '${searchKeyWords(i)}')"
      }
      return result
    }
    ""
  }

  def getClickCategoryIds: String = {
    if (clickCategoryIds != null) {
      val len = clickCategoryIds.length
      var result = "and ("
      for (i <- 0 until len) {
        if (i != len - 1) result += s"${Constants.FIELD_CLICK_CATEGORY_ID} = '${clickCategoryIds(i)}' or "
        else result += s"${Constants.FIELD_CLICK_CATEGORY_ID} = '${clickCategoryIds(i)}')"
      }
      return result
    }
    ""
  }

}
