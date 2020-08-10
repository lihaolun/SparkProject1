package util

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CategoryActionCountAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {
  var categoryActionMap = new mutable.HashMap[String, Long]()

  override def isZero: Boolean = {
    categoryActionMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accumulator = new CategoryActionCountAccumulator
    accumulator.categoryActionMap ++= categoryActionMap
    accumulator
  }

  override def reset(): Unit = categoryActionMap = new mutable.HashMap[String, Long]()

  override def add(key: String): Unit = {
    categoryActionMap(key) = categoryActionMap.getOrElse(key, 0L) + 1L
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val otherMap: mutable.HashMap[String, Long] = other.value
    categoryActionMap = categoryActionMap.foldLeft(otherMap) {
      case (otherMap, (key, count)) =>
        otherMap(key) = otherMap.getOrElse(key, 0L) + count
        otherMap
    }
  }

  override def value: mutable.HashMap[String, Long] = categoryActionMap
}
