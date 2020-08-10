package com.atguigu.sparkmall.realtime.app

import com.atguigu.sparkmall.common.bean.RealtimeAdsLog
import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import redis.clients.jedis.Jedis

object LastHourAdsCount {
  //RealtimeAdsLog:logdate:Date,area:String,city:String,userId:String,adsId:String
  def calcLastHourCountPerAds(filteredRealtimeLog: DStream[RealtimeAdsLog]) = {
    //要统计最近一小时的日志，数据是变化的，所以要用滑动窗口
    // RDD[RealtimeAdsLog] => window =>map=>
    val lastHourLogDStream: DStream[RealtimeAdsLog] = filteredRealtimeLog.window(Minutes(60), Seconds(10))

    val adsHourMinCountDStream: DStream[(String, Long)] = lastHourLogDStream.map {
      case (lastHourLog) =>
        val adsId: String = lastHourLog.adsId
        val hourMinStr: String = lastHourLog.getHourMinString()
        val key = adsId + "_" + hourMinStr
        (key, 1L)
    }.reduceByKey(_ + _)

    // =>RDD[adsid_hour_min,count]  按广告id进行聚合，把本小时内相同的广告聚合到一起
    // =>RDD[adsId,Iterable[(hour_min,count)]]
    val lastHourAdsCountDStream: DStream[(String, Iterable[(String, Long)])] = adsHourMinCountDStream.map { case (adsId_hourMinStr, count) =>
      val keyArray: Array[String] = adsId_hourMinStr.split("_")
      val adsId: String = keyArray(0)
      val hourMinStr: String = keyArray(1)
      (adsId, (hourMinStr, count))
    }.groupByKey()

    //
    val hourMinJsonAdsDStream: DStream[(String, String)] = lastHourAdsCountDStream.map { case (adsId, hourMinuItr) =>
      val hourMinList: List[(String, Long)] = hourMinuItr.toList
      val hourMinJson: String = compact(render(hourMinList))
      (adsId, hourMinJson)
    }

    //保存到redis中,redis只能存java对象，所以要引入隐式转换
    val jedis: Jedis = RedisUtil.getJedisClient
    hourMinJsonAdsDStream.foreachRDD { rdd =>
      val hourMinJsonAdsArray: Array[(String,String)] = rdd.collect()
      import collection.JavaConversions._
      jedis.hmset("last_hour_ads_click",hourMinJsonAdsArray.toMap)
    }
  }

}
