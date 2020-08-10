package test

import java.text.SimpleDateFormat
import java.util.Date

import bean.SessionInfo
import com.atguigu.sparkmall.common.bean.UserVisitAction
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

class SessionExtractApp {
  val extractNum = 1000


  def sessionExtract(sessionCount: Long, taskId: String, userSessionRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    /**
      * 1   所有session集合，以sessionId为单位
      * 2   抽取
      * 某个小时要抽取得session个数=某个小时的session个数 /总session数量 *1000
      * RDD[sessionId,Iterable[UserAction
      * 按天+小时进行聚合 ，求出每个【天+小时】的session个数
      * 3.写一个抽取方法
      * 实现从一个集合中 按照规定的数量随机抽取放到另一个集合中
      */
    val sessionInfoRDD: RDD[SessionInfo] = userSessionRDD.map { case (sessionId, userVisitActions) =>
      var maxActionTime = -1L
      var minActionTime = Long.MaxValue
      val keywordBuffer = new ListBuffer[String]()
      val clickBuffer = new ListBuffer[String]()
      val orderBuffer = new ListBuffer[String]()
      val payBuffer = new ListBuffer[String]()

      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:ss:mm")
      for (userVisitAction <- userVisitActions) {
        val time: Long = dateFormat.parse(userVisitAction.action_time).getTime
        maxActionTime = Math.max(maxActionTime, time)
        minActionTime = Math.min(minActionTime, time)
        //判断每个action的动作
        if (userVisitAction.search_keyword != null) {
          keywordBuffer += userVisitAction.search_keyword
        } else if (userVisitAction.click_product_id != -1) {
          clickBuffer += userVisitAction.click_product_id.toString
        } else if (userVisitAction.order_product_ids != null) {
          orderBuffer += userVisitAction.order_product_ids
        } else if (userVisitAction.pay_product_ids != null) {
          payBuffer += userVisitAction.pay_product_ids
        }
      }
      val visitLenth = maxActionTime - minActionTime
      val stepLenth = userVisitActions.size
      val startTime: String = dateFormat.format(new Date(minActionTime))
      SessionInfo(taskId, sessionId, startTime, stepLenth, visitLenth,
        keywordBuffer.mkString(","), clickBuffer.mkString(","), orderBuffer.mkString(","), payBuffer.mkString(","))
    }

    val dayHourSessionsRDD: RDD[(String, SessionInfo)] = sessionInfoRDD.map { sessionInfo =>
      val dayHour: String = sessionInfo.startTime.split(":")(0)
      (dayHour, sessionInfo)
    }
    val groupByHourRDD: RDD[(String, Iterable[SessionInfo])] = dayHourSessionsRDD.groupByKey()

    groupByHourRDD.flatMap{
      case(dayHour,sessionInfos)=>
        val extrectHourNum: Long = Math.round(sessionInfos.size / sessionCount.toDouble * extractNum)
        //抽取
      randomExtract(sessionInfos.toArray,extrectHourNum)
    }

  }

  def randomExtract(toArray: Array[SessionInfo], extrectHourNum: Long) = {
    val extractSet = new mutable.HashSet[SessionInfo]()
    while(extractSet.size<extrectHourNum){
      val index = new Random().nextInt(toArray.length)
      val value = toArray(index)
      extractSet+=value
    }
    extractSet
  }

}
