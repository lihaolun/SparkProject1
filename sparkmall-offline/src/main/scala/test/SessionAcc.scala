package test

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class SessionAcc extends AccumulatorV2[String,mutable.HashMap[String,Long]]{
  var sessionMap = new mutable.HashMap[String,Long]()
  //判断是否是初始值
  override def isZero: Boolean =
    sessionMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val sessionAcc = new SessionAcc()
    sessionAcc.sessionMap ++= sessionMap
    sessionAcc
  }
  override def reset(): Unit = {
    sessionMap = new mutable.HashMap[String,Long]()
  }

  override def add(key: String): Unit = {
    sessionMap(key) = sessionMap.getOrElse(key,0L)+1L
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val otherMap: mutable.HashMap[String, Long] = other.value
    sessionMap.foldLeft(otherMap){
      case(otherMap,(key,count))=>
        otherMap(key) = otherMap.getOrElse(key,0L)+count
        otherMap
    }
  }

  override def value: mutable.HashMap[String, Long] =
    sessionMap
}
