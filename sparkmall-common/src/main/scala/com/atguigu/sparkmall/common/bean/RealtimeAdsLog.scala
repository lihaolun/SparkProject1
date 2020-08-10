package com.atguigu.sparkmall.common.bean

import java.text.SimpleDateFormat
import java.util.Date

case class RealtimeAdsLog(logdate:Date,area:String,city:String,userId:String,adsId:String) {
   def getDateString(): String ={
     val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
     dateFormat.format(logdate)
   }

  def getHourMinString():String={
    val dateFormat = new SimpleDateFormat("HH:mm")
    dateFormat.format(logdate)
  }
}
