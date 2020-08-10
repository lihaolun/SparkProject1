package test

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import bean.SessionInfo
import business.SessionExtractApp
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.{ConfigUtil, JdbcUtil}
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object UserActionAnalyze {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserActionAnalyze")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val taskId: String = UUID.randomUUID().toString
    val config: FileBasedConfiguration = ConfigUtil("conditions.properties").config
    val conditionString: String = config.getString("condition.params.json")
    val accumulator = new SessionAcc()
    sparkSession.sparkContext.register(accumulator)
    val userVisitActionRDD: RDD[UserVisitAction] = readUserVisiActionRDD(sparkSession, conditionString)
    val userSessionRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.map(userAction => (userAction.session_id,userAction)).groupByKey()
    userSessionRDD.cache()


    val userSessionCount: Long = userSessionRDD.count()
    //遍历全部RDD,对RDD进行分类判断累加
    userSessionRDD.foreach{case(sessionId,actions)=>
        var maxActionTime = -1L
        var minActionTime = Long.MaxValue
        for(action<-actions) {
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val actionDate: Date = format.parse(action.action_time)
          val actionTime: Long = actionDate.getTime
          maxActionTime=Math.max(maxActionTime,actionTime)
          minActionTime=Math.min(minActionTime,actionTime)
        }
        val visitTime: Long = maxActionTime-minActionTime
        if(visitTime>10000){
          accumulator.add("session_visitLenth_gt_10_count")
        }else{
          accumulator.add("session_visitLenth_le_10_count")
        }
        if(actions.size>5){
          accumulator.add("session_stepLenth_gt_5_count")
        }else{
          accumulator.add("session_stepLenth_le_5_count")
        }
    }

        //提取累加器中的值
    val sessionCountMap: mutable.HashMap[String, Long] = accumulator.value

    val session_visitLenth_gt_10_ratio: Double = Math.round(1000.0*sessionCountMap("session_visitLenth_gt_10_count")/userSessionCount)/10.0
    val session_visitLenth_le_10_ratio: Double = Math.round(1000.0*sessionCountMap("session_visitLenth_le_10_count")/userSessionCount)/10.0
    val session_stepLenth_gt_5_ratio: Double = Math.round(1000.0*sessionCountMap("session_stepLenth_gt_5_count")/userSessionCount)/10.0
    val session_stepLenth_le_5_ratio: Double = Math.round(1000.0*sessionCountMap("session_stepLenth_le_5_count")/userSessionCount)/10.0

    val array: Array[Any] = Array(taskId,conditionString,userSessionCount,session_visitLenth_gt_10_ratio,session_visitLenth_le_10_ratio,session_stepLenth_gt_5_ratio,session_stepLenth_le_5_ratio)

    JdbcUtil.executeUpdate("insert into session_stat_info values (?,?,?,?,?,?,?)",array)

    //需求二：按比例抽取session
    val sessionExtractRDD: RDD[SessionInfo] = SessionExtractApp.sessionExtract(userSessionCount,taskId,userSessionRDD)

    import sparkSession.implicits._
    val conf: FileBasedConfiguration = ConfigUtil("config.properties").config
    sessionExtractRDD.toDF.write.format("jdbc")
      .option("url", conf.getString("jdbc.url"))
      .option("user", conf.getString("jdbc.user"))
      .option("password", conf.getString("jdbc.password"))
      .option("dbtable", "random_session_info")
      .mode(SaveMode.Append).save()

    //需求三 根据点击、下单、支付进行排序，取前十名
    //1.取所有访问日志 2.按照cid+操作类型进行累加  累加器HashMap[Sring,Long]
    userVisitActionRDD.foreach{ userAction =>
      if(userAction.click_product_id != -1L){

      }
    }
  }


  def readUserVisiActionRDD(sparkSession: SparkSession, conditionString: String): RDD[UserVisitAction] = {
    //1.查库 2.条件
    val config: FileBasedConfiguration = ConfigUtil("config.properties").config
    val database: String = config.getString("hive.database")
    sparkSession.sql("use" + database);

    val jSONObject: JSONObject = JSON.parseObject(conditionString)
    var sql = new StringBuilder("select v.* from user_visit_action v join user_info u on v,user_id = u.user_id where 1= 1")

    if (jSONObject.getString("startDate") != null) {
      sql.append("and date >= '" + jSONObject.getString("startDate") + "'")
    }
    if (jSONObject.getString("endDate") != null) {
      sql.append("and date <= '" + jSONObject.getString("endDate") + "'")
    }
    if (jSONObject.getString("startAge") != null) {
      sql.append("and u.age >= '" + jSONObject.getString("startAge") + "'")
    }
    if (jSONObject.getString("endAge") != null) {
      sql.append("and u.age <= '" + jSONObject.getString("endAge") + "'")
    }
    if (jSONObject.getString("professionals").isEmpty) {
      sql.append("and u.professional in (" + jSONObject.getString("professional") + ")")
    }
    println(sql)
    import sparkSession.implicits._
    val userVisitActionRDD: RDD[UserVisitAction] = sparkSession.sql(sql.toString()).as[UserVisitAction].rdd
    userVisitActionRDD
  }

}
