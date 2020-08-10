package business.offline

import java.text.SimpleDateFormat
import java.util.UUID

import bean.{CategoryTopN, SessionInfo}
import business.{CategorySessionTon10App, PageConvertRatioApp, SessionExtractApp}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.{ConfigUtil, JdbcUtil}
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import util.{CategoryActionCountAccumulator, SessionAccumulator}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object OfflineApp {

  def main(args: Array[String]): Unit = {
    val sparkconf: SparkConf = new SparkConf().setAppName("offline").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkconf).enableHiveSupport().getOrCreate()
    val accumulator = new SessionAccumulator()
    sparkSession.sparkContext.register(accumulator)
    val taskId: String = UUID.randomUUID().toString
    val conditionConfig: FileBasedConfiguration = ConfigUtil("conditions.properties").config
    val conditionJsonString: String = conditionConfig.getString("condition.params.json")

    //    1 \ 筛选  要关联用户  sql   join user_info  where  contidition  =>DF=>RDD[UserVisitAction]
    val userActionRDD: RDD[UserVisitAction] = readUserVisitActionRDD(sparkSession, conditionJsonString)
    //      2  rdd=>  RDD[(sessionId,UserVisitAction)] => groupbykey => RDD[(sessionId,iterable[UserVisitAction])]
    val userSessionRDD: RDD[(String, Iterable[UserVisitAction])] = userActionRDD.map(userAction => (userAction.session_id, userAction)).groupByKey()
    userSessionRDD.cache()

    //      3 求 session总数量，
    val userSessionCount: Long = userSessionRDD.count() //总数
    println(s"userSessionCount = ${userSessionCount}")
    //    遍历一下全部session，对每个session的类型进行判断 来进行分类的累加  （累加器）
    //    4  分类 ：时长 ，把session里面的每个action进行遍历 ，取出最大时间和最小事件 ，求差得到时长 ，再判断时长是否大于10秒
    //    步长： 计算下session中有多少个action, 判断个数是否大于5
    //
    userSessionRDD.foreach { case (sessionId, actions) =>
      var maxActionTime = -1L
      var minActionTime = Long.MaxValue
      for (action <- actions) {
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val actionTimeMillSec: Long = format.parse(action.action_time).getTime
        maxActionTime = Math.max(maxActionTime, actionTimeMillSec)
        minActionTime = Math.min(minActionTime, actionTimeMillSec)
      }
      val visitTime: Long = maxActionTime - minActionTime
      if (visitTime > 10000) {
        accumulator.add("session_visitLength_gt_10_count")
        //累加
      } else {
        //累加
        accumulator.add("session_visitLength_le_10_count")
      }
      if (actions.size > 5) {
        //累加
        accumulator.add("session_stepLength_gt_5_count")
      } else {
        //累加
        accumulator.add("session_stepLength_le_5_count")
      }
    }

    userSessionRDD

    //5 提取累加器中的值
    val sessionCountMap: mutable.HashMap[String, Long] = accumulator.value

    println(sessionCountMap.mkString(","))
    //6 把累计值计算为比例
    val session_visitLength_gt_10_ratio = Math.round(1000.0 * sessionCountMap("session_visitLength_gt_10_count") / userSessionCount) / 10.0
    val session_visitLength_le_10_ratio = Math.round(1000.0 * sessionCountMap("session_visitLength_le_10_count") / userSessionCount) / 10.0
    val session_stepLength_gt_5_ratio = Math.round(1000.0 * sessionCountMap("session_stepLength_gt_5_count") / userSessionCount) / 10.0
    val session_stepLength_le_5_ratio = Math.round(1000.0 * sessionCountMap("session_stepLength_le_5_count") / userSessionCount) / 10.0

    val resultArray = Array(taskId, conditionJsonString, userSessionCount, session_visitLength_gt_10_ratio, session_visitLength_le_10_ratio, session_stepLength_gt_5_ratio, session_stepLength_le_5_ratio)
    //7 保存到mysql中
    JdbcUtil.executeUpdate("insert into session_stat_info values (?,?,?,?,?,?,?) ", resultArray)


    //需求二 按比例抽取session
    val sessionExtractRDD: RDD[SessionInfo] = SessionExtractApp.sessionExtract(userSessionCount, taskId, userSessionRDD)

    import sparkSession.implicits._
    val config: FileBasedConfiguration = ConfigUtil("config.properties").config
    sessionExtractRDD.toDF.write.format("jdbc")
      .option("url", config.getString("jdbc.url"))
      .option("user", config.getString("jdbc.user"))
      .option("password", config.getString("jdbc.password"))
      .option("dbtable", "random_session_info")
      .mode(SaveMode.Append).save()

    //需求三  根据点击、下单、支付进行排序，取前十名
    //    1 、遍历所有的访问日志
    //    map
    //    2 按照cid+操作类型进行分别累加
    //    累加器 hashMap[String ,Long]
    val categoryActionCountAccumulator = new CategoryActionCountAccumulator()
    sparkSession.sparkContext.register(categoryActionCountAccumulator)
    userActionRDD.foreach { userAction =>
      if (userAction.click_category_id != -1L) {
        //给品类的点击项+1
        categoryActionCountAccumulator.add(userAction.click_category_id + "_click")
      } else if (userAction.order_product_ids != null && userAction.order_product_ids.size != 0) {
        val orderCids: Array[String] = userAction.order_product_ids.split(",")
        for (orderCid <- orderCids) {
          categoryActionCountAccumulator.add(orderCid + "_order")
        }
      } else if (userAction.pay_category_ids != null && userAction.pay_category_ids.size != 0) {
        val payCids: Array[String] = userAction.pay_category_ids.split(",")
        for (payCid <- payCids) {
          categoryActionCountAccumulator.add(payCid + "_pay")
        }
      }
    }
    println("categoryActionCountAccumulator:"+categoryActionCountAccumulator.value.mkString("\n"))
    val groupedCidMap: Map[String, mutable.HashMap[String, Long]] = categoryActionCountAccumulator.value.groupBy {
      case (cidAction, count) =>
        val cid: String = cidAction.split("_")(0)
        //用cid分组
        cid
    }
    val categoryTopNList: List[CategoryTopN] = groupedCidMap.map {
      case (cid, actionMap) =>
        CategoryTopN(taskId, cid, actionMap.getOrElse(cid + "_click", 0L), actionMap.getOrElse(cid + "_order", 0L), actionMap.getOrElse(cid + "_pay", 0L))
    }.toList

    val categoryTop10List: List[CategoryTopN] = categoryTopNList.sortWith { (x1, x2) =>
      if (x1.click_count > x2.click_count) {
        true
      } else if (x1.click_count == x2.click_count) {
        if (x1.order_count > x2.order_count) {
          true
        } else {
          false
        }
      } else if (x1.order_count == x2.order_count) {
        if (x1.pay_count > x2.pay_count) {
          true
        } else {
          false
        }
      } else {
        false
      }
    }.take(10)


    println("categoryTopNList:" + categoryTop10List.mkString(","))
    //组合参数插入数据库
    //task:String,category_id:String,click_count:Long,order_count:Long,pay_count:Long)
    val topNList = new ListBuffer[Array[Any]]
    for (categoryTopN <- categoryTop10List) {
      val array = Array(taskId, categoryTopN.category_id, categoryTopN.click_count, categoryTopN.order_count, categoryTopN.pay_count)
      topNList.append(array)
    }
    JdbcUtil.executeBatchUpdate("insert into category_top10 values (?,?,?,?,?)", topNList)


    //需求四：Top10 热门品类中 Top10 活跃 Session 统计
    CategorySessionTon10App.statCategorySessionTop10(config,sparkSession,taskId,userActionRDD,categoryTop10List)
    println("需求四保存完成")

    //需求五：页面单跳转化率统计
    PageConvertRatioApp.calcPageConvertRatio(sparkSession,taskId,conditionJsonString,userActionRDD)
  }


  def readUserVisitActionRDD(sparkSession: SparkSession, conditionJsonString: String): RDD[UserVisitAction] = {
    //1 查库  2 条件
    val config: FileBasedConfiguration = ConfigUtil("config.properties").config
    val databaseName: String = config.getString("hive.database")
    sparkSession.sql("use " + databaseName)

    val jSONObject: JSONObject = JSON.parseObject(conditionJsonString)
    jSONObject.getString("startDate")

    //sql    //left join ->翻译   inner join 过滤
    var sql = new StringBuilder("select v.* from user_visit_action v join user_info u on v.user_id=u.user_id  where 1=1")

    if (jSONObject.getString("startDate") != null) {
      sql.append(" and  date>='" + jSONObject.getString("startDate") + "'")
    }
    if (jSONObject.getString("endDate") != null) {
      sql.append(" and  date<='" + jSONObject.getString("endDate") + "'")
    }
    if (jSONObject.getString("startAge") != null) {
      sql.append(" and  u.age>=" + jSONObject.getString("startAge"))
    }
    if (jSONObject.getString("endAge") != null) {
      sql.append(" and  u.age<=" + jSONObject.getString("endAge"))
    }
    if (!jSONObject.getString("professionals").isEmpty) {
      sql.append(" and  u.professional in (" + jSONObject.getString("professionals") + ")")
    }
    println(sql)
    import sparkSession.implicits._
    val rdd: RDD[UserVisitAction] = sparkSession.sql(sql.toString()).as[UserVisitAction].rdd
    rdd
  }

}
