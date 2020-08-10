package business

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.JdbcUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageConvertRatioApp {

  def calcPageConvertRatio(sparkSession: SparkSession, taskId: String, conditionJsonString: String, userActionRDD: RDD[UserVisitAction]): Unit = {
    //1.转化率的公式：跳转第二个页面的次数 / 第一个页面的访问次数
    //2.1，2, 3,4,5,6,7  ->        1-2,2-3,3-4,4-5,5-6,6-7次数  /  1,2,3,4,5,6次数
    //取得要计算的 跳转页面
    val pageVisitArray: Array[String] = JSON.parseObject(conditionJsonString).getString("targetPageFlow") split (",") //1,2,3,4,5,6,7
    //3.用拉链来构成想要的结构
    val pageJumpTunple: Array[(String, String)] = pageVisitArray.slice(0, pageVisitArray.length - 1).zip(pageVisitArray.slice(1, pageVisitArray.length))
    val targetVisitPagesBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(pageVisitArray.slice(0, pageVisitArray.length - 1)) //1,2,3,4,5,6


    val pageJumpArray: Array[String] = pageJumpTunple.map { case (page1, page2) => page1 + "-" + page2 } //1-2,2-3,3-4,4-5,5-6,6-7
    val targePageJumpsBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(pageJumpArray)

    //4.前一个页面的访问次数 -> 1-6页面的访问次数，取pageid为1-6的访问次数
    val filteredUserActionRDD: RDD[UserVisitAction] = userActionRDD.filter { userAction =>
      targetVisitPagesBC.value.contains(userAction.page_id.toString)
    }
    //pageid为1-6的访问记录->按照pageid进行count
    val userPageVisitCountMap: collection.Map[Long, Long] = filteredUserActionRDD.map {
      userAction =>
        (userAction.page_id, 1L)
    }.countByKey()

    //5.两个页面跳转的次数   每个sessionId为key做聚合
    val userActionsBySessionRDD: RDD[(String, Iterable[UserVisitAction])] = userActionRDD.map { userAction =>
      (userAction.session_id, userAction)
    }.groupByKey()
    //对每个session的访问记录按时间大小进行排序
    val pageJumpRDD: RDD[String] = userActionsBySessionRDD.flatMap { case (sessionId, iterUserActions) =>
      val sortedUserActionList: List[UserVisitAction] = iterUserActions.toList.sortWith((x1, x2) =>
        x1.action_time < x2.action_time)
      //访问转换成页面的跳转：1-2，2-3，...   每次访问（session）都对应页面的跳转
      val pageVisitList: List[Long] = sortedUserActionList.map(userAction => userAction.page_id)
      val pageJumpList: List[String] = pageVisitList.slice(0, pageVisitList.length - 1).zip(pageVisitList.slice(1, pageVisitList.length)).map { case (pageId1, pageId2) =>
        pageId1 + "-" + pageId2
      }
      pageJumpList
    }
    val filteredPageJumpRDD: RDD[String] = pageJumpRDD.filter { pageJumps =>
      targePageJumpsBC.value.contains(pageJumps)
    }
    //每个跳转页面的计数
    val pageJumpCountMap: collection.Map[String, Long] = filteredPageJumpRDD.map { pageJump =>
      (pageJump, 1L)
    }.countByKey()

    println(userPageVisitCountMap.mkString("\n"))
    println(pageJumpCountMap.mkString("\n"))

    //6.两个map分别进行除法得到转换率
    val pageCovertRatios: Iterable[Array[Any]] = pageJumpCountMap.map { case (pageJump, count) =>
      val pageAndJumpArray: Array[String] = pageJump.split("-")
      val prefixPageId: String = pageAndJumpArray(0)
      val prefixPageCount: Long = userPageVisitCountMap.getOrElse(prefixPageId.toLong, 0L)
      val radio: Double = Math.round(count / prefixPageCount.toDouble * 1000) / 10.0
      Array(taskId, pageJump, radio)
    }
    //7.库存
    JdbcUtil.executeBatchUpdate("insert into page_convert_rate values(?,?,?)", pageCovertRatios)
  }

}
