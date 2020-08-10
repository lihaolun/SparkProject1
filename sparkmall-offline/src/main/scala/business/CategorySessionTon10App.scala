package business

import bean.{CategorySessionTop, CategoryTopN}
import com.atguigu.sparkmall.common.bean.UserVisitAction
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object CategorySessionTon10App {
  def statCategorySessionTop10(config: FileBasedConfiguration, sparkSession: SparkSession, taskId: String, userActionRDD: RDD[UserVisitAction], categoryTopNList: List[CategoryTopN]) = {
    val categoryTopNListBC: Broadcast[List[CategoryTopN]] = sparkSession.sparkContext.broadcast(categoryTopNList)
    //1.过滤所有排名前十名的userAction
    val filteredUserActionRDD: RDD[UserVisitAction] = userActionRDD.filter { userAction =>
      var flag = false
      for (elem <- categoryTopNListBC.value) {
        if (elem.category_id == userAction.click_category_id.toString) {
          flag = true
        }
      }
      flag
    }


    //2.相同的cid+sessionId进行累加
    val cidSessionCountRDD: RDD[(String, Long)] = filteredUserActionRDD.map {
      userAction =>
        (userAction.click_category_id + "_" + userAction.session_id, 1L)
    }.reduceByKey(_ + _)

    //3.根据cid进行聚合
    val sessionCountByCid: RDD[(String, Iterable[(String, Long)])] = cidSessionCountRDD.map {
      case (cid_session, count) =>
        val cidSessionArray: Array[String] = cid_session.split("_")
        val cid: String = cidSessionArray(0)
        val sessionId: String = cidSessionArray(1)
        (cid, (sessionId, count))
    }.groupByKey()

    //4.聚合后进行排序，截取
    val categorySessionTop10RDD: RDD[CategorySessionTop] = sessionCountByCid.flatMap { case (cid, sessionCount) =>
      val sortedSessionCount: List[(String, Long)] = sessionCount.toList.sortWith { (x1, x2) =>
        x1._2 > x2._2
      }.take(10)

      //转换成对象，存储到数据库中
      val categorySession10List: List[CategorySessionTop] = sortedSessionCount.map {
        case (sessionId, count) =>
          CategorySessionTop(taskId, cid, sessionId, count)
      }
      categorySession10List
    }

    //4.聚合后进行排序，截取
    println(categorySessionTop10RDD.collect().mkString("\n"))

    import sparkSession.implicits._
    categorySessionTop10RDD.toDF().write.format("jdbc")
        .option("url",config.getString("jdbc.url"))
        .option("user",config.getString("jdbc.user"))
        .option("password",config.getString("jdbc.password"))
        .option("dbtable","category_top10_session_count")
        .mode(SaveMode.Append).save()
  }
}
