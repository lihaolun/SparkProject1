package com.atguigu.sparkmall.realtime.app

import com.atguigu.sparkmall.common.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestData {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReaktimeAdsLogApp")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val inputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log",ssc)
    val recordStream: DStream[String] = inputStream.map {
      record => record.value()
    }
    recordStream.foreachRDD{
      rdd =>
        println(rdd.collect().mkString(","))
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
