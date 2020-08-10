package com.atguigu.sparkmall.realtime.app

import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import redis.clients.jedis.Jedis

object AreaTop3AdsDayApp {
  def calcTop3Ads(filteredKVTotalCount: DStream[(String, Long)]) = {
    //filteredKVTotalCount:RDD[are_city_adsId_day,count] => RDD[(area_asdId_day,count)] 地区一样的聚合
    val areaAdsDayTotalCountDStream: DStream[(String, Long)] = filteredKVTotalCount.map {
      case (are_city_adsId_day, count) =>
        val keyArray: Array[String] = are_city_adsId_day.split(":")
        val area: String = keyArray(0)
        val city: String = keyArray(1)
        val adsId: String = keyArray(2)
        val day: String = keyArray(3)
        val area_asdId_day: String = area + ":" + adsId + ":" + day
        (area_asdId_day, count)
    }

    //RDD[(area_asdId_day,count)]=>RDD(day,(area,(adsId,count)))
    val reaAdsIdGroupbyDayDStream: DStream[(String, Iterable[(String, (String, Long))])] = areaAdsDayTotalCountDStream.map {
      case (area_adsId_day, count) =>
        val keyArray: Array[String] = area_adsId_day.split(":")
        val area: String = keyArray(0)
        val adsId: String = keyArray(1)
        val day: String = keyArray(2)
        (day, (area, (adsId, count)))
    }.groupByKey()

    val dayAreaAdsTop3DStream: DStream[(String, Map[String, String])] = reaAdsIdGroupbyDayDStream.map { case (daykey, areaIter) =>
      //    //RDD(day,iterable(area,(adsId,count)))   => RDD(day,Map(area,(adsId,count)))   按照area进行分组聚合
      val adsIdCountGroupbyArea: Map[String, Iterable[(String, (String, Long))]] = areaIter.groupBy {
        case (area, (adsId, count)) => area
      }
      //    =>Map[day,Map[area,jsonString]]
      val areaAdsCountTop3JsonMap: Map[String, String] = adsIdCountGroupbyArea.map { case (area, areaAdsIterable) =>
        val adsCountIter: Iterable[(String, Long)] = areaAdsIterable.map { case (area, (adsId, count)) => (adsId, count) }
        val adsCountTop3List: List[(String, Long)] = adsCountIter.toList.sortWith { (x1, x2) =>
          x1._2 > x2._2
        }.take(3)
        //转为json
        val adsCountTop3ListJson: String = compact(render(adsCountTop3List))
        (area, adsCountTop3ListJson)
      }
      (daykey, areaAdsCountTop3JsonMap)
    }
    //声明到每个分区内，executor
    dayAreaAdsTop3DStream.foreachRDD { rdd =>
      rdd.foreachPartition { dayAreaAdsTop3Iter =>
        val jedis: Jedis = RedisUtil.getJedisClient
        for ((daykey, areaMap) <- dayAreaAdsTop3Iter) {
          import collection.JavaConversions._
          jedis.hmset("top3_ads_per_day:" + daykey, areaMap)
        }
        jedis.close()
      }
    }
  }

}
