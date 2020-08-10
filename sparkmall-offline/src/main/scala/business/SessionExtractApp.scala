package business

import java.text.SimpleDateFormat
import java.util.Date

import bean.SessionInfo
import com.atguigu.sparkmall.common.bean.UserVisitAction
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object SessionExtractApp {
  val extractNum = 1000

  def main(args: Array[String]): Unit = {
      println(randomExtract(Array(1,2,3,4,5),3))
  }

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
    //1.userSessionRDD: RDD[(String, Iterable[UserVisitAction])] => RDD[sessionInfo]
    val sessionInfoRDD: RDD[SessionInfo] = userSessionRDD.map { case (sessionId, userVisitActions) =>
      //要把UserVisitAction转换为SessionInfo,需要获取要取的属性
      //求时长和开始时间
      var maxActionTime = -1L
      var minActionTime = Long.MaxValue
      val keywordBuffer = new ListBuffer[String]()
      val clickBuffer = new ListBuffer[String]()
      val orderBuffer = new ListBuffer[String]()
      val payBuffer = new ListBuffer[String]()


      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:ss:mm")

      for (userVisitAction <- userVisitActions) {

        val actionTimeSec: Long = dateFormat.parse(userVisitAction.action_time).getTime
        maxActionTime = Math.max(maxActionTime, actionTimeSec)
        minActionTime = Math.min(minActionTime, actionTimeSec)
        //判断每个action的操作
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


    //2 RDD[ sessionInfo]=>RDD[ day_hour,sessionInfo]
    val dayHourSessionsRDD: RDD[(String, SessionInfo)] = sessionInfoRDD.map { sessionInfo =>
      val dayHour: String = sessionInfo.startTime.split(":")(0)
      (dayHour, sessionInfo)
    }

    //3.RDD[dayHour，sessionInfo] =>group by key
    val dayHourGroupRDD: RDD[(String, Iterable[SessionInfo])] = dayHourSessionsRDD.groupByKey()

    //4.抽取方法
    dayHourGroupRDD.flatMap { case (dayHour, sessionInfos) =>
      //1,计算抽取规则
      val extractDayHourNum: Long = Math.round(sessionInfos.size / sessionCount.toDouble * extractNum)
      //抽取
      randomExtract(sessionInfos.toArray, extractDayHourNum)
    }

  }

  def randomExtract[T](array: Array[T], extractDayHourNum: Long) = {

    val resultSet = new mutable.HashSet[T]()
    //循环抽取
    while (resultSet.size < extractDayHourNum) {
      val index: Int = new Random().nextInt(array.length)
      val value = array(index)
      resultSet += value
    }
    resultSet
  }
}

