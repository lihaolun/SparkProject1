package com.atguigu.sparkmall.realtime.app

import java.util
import java.util.Date

import com.atguigu.sparkmall.common.bean.RealtimeAdsLog
import com.atguigu.sparkmall.common.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object RealtimeAdsLogApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReaktimeAdsLogApp")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    //需求7. 每天各地区各城市各广告的点击流量实时统计
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log", ssc)
    val adsDStream: DStream[String] = inputDStream.map {
      record =>
        record.value()
    }
    adsDStream.foreachRDD { rdd =>
      println(rdd.collect().mkString("\n"))
    }
    //1544609883741 华北 天津 3 4
    val realtimeLogDStream: DStream[RealtimeAdsLog] = adsDStream.map {
      adsStr => {
        val logArry: Array[String] = adsStr.split(" ")
        val dateSes: String = logArry(0)
        val area: String = logArry(1)
        val city: String = logArry(2)
        val userId: String = logArry(3)
        val adsId: String = logArry(4)
        val date = new Date(dateSes.toLong)
        RealtimeAdsLog(date, area, city, userId, adsId)
      }
    }


    val jedisClient: Jedis = RedisUtil.getJedisClient

    //过滤掉黑名单中的用户日志
    val filteredRealtimeLog: DStream[RealtimeAdsLog] = realtimeLogDStream.transform { rdd =>
      val blackList: util.Set[String] = jedisClient.smembers("user_blacklist")
      val blackListBC: Broadcast[util.Set[String]] = sc.broadcast(blackList)
      val filteredRealtimeLogRDD: RDD[RealtimeAdsLog] = rdd.filter { realtimeLog =>
        !blackListBC.value.contains(realtimeLog.userId)
      }
      filteredRealtimeLogRDD
    }
    //需求8.每天各地区各城市各广告的点击流量实时统计
    //把明细变为keyvalue结构  地区+城市+广告+天 =>count
    val filteredKVCount: DStream[(String, Long)] = filteredRealtimeLog.map {
      filteredRealtimeLog =>
        val key = filteredRealtimeLog.area + ":" + filteredRealtimeLog.city + ":" + filteredRealtimeLog.adsId + ":" +
          filteredRealtimeLog.getDateString()
        (key, 1L)
    }.reduceByKey(_ + _)

    //将每五秒count进行合并，这里用redis做
    sc.setCheckpointDir("./checkpoint")
    val filteredKVTotalCount: DStream[(String, Long)] = filteredKVCount.updateStateByKey {
      (adsCountSeq: Seq[Long], totalCount: Option[Long]) =>
        val adsCountSum: Long = adsCountSeq.sum
        val newTotalCount: Long = totalCount.getOrElse(0L) + adsCountSum
        Some(newTotalCount)
    }
    //把结果写入redis
    filteredKVTotalCount.foreachRDD {
      rdd: RDD[(String, Long)] =>
        val filteredKVCountArray: Array[(String, Long)] = rdd.collect()
        for ((key, count) <- filteredKVCountArray) {
          jedisClient.hset("area_city_ads_day_clickcount", key, count.toString)
        }
    }


    //需求9.实时数据分析：每天各地区 top3 热门广告
    AreaTop3AdsDayApp.calcTop3Ads(filteredKVTotalCount)
    println("需求九完成")

    //需求10.
    LastHourAdsCount.calcLastHourCountPerAds(filteredRealtimeLog)
    println("需求十完成")
    //按天+用户+广告 进行聚合  计算点击量
    //           ->rdd[(userid_adsid_date,1L)  ]-> reducebykey->rdd[(userid_adsid_date,count)]
    val userAdsCountPerDayDSream: DStream[(String, Long)] = filteredRealtimeLog.map { realtimelog =>
      val key: String = realtimelog.userId + ":" + realtimelog.adsId + ":" + realtimelog.getDateString()
      (key, 1L)
    }.reduceByKey(_ + _)

    //需求7.向redis中存放用户点击广告的累计值
    userAdsCountPerDayDSream.foreachRDD { rdd =>
      /* rdd.foreachPartition { itrabc =>
       val jedisClient: Jedis = RedisUtil.getJedisClient
       for (abc <- itrabc) {
          jedisClient //处理abc
       }
      }*/
      val jedisClient: Jedis = RedisUtil.getJedisClient

      val userAdsCountPerDayArr: Array[(String, Long)] = rdd.collect()

      for ((key, count) <- userAdsCountPerDayArr) {
        val countString: String = jedisClient.hget("user_ads_count_perday", key)
        //达到阈值 进入黑名单
        if (countString != null && countString.toLong >= 10000) {
          val userId: String = key.split(":")(0)
          //黑名单 结构 set
          jedisClient.sadd("user_blacklist", userId)
        } else {
          jedisClient.hincrBy("user_ads_count_perday", key, count)
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}