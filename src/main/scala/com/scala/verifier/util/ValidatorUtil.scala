package com.scala.verifier.util

import org.apache.avro.JsonProperties.Null
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeParseException
import java.time.format.DateTimeFormatter
class ValidatorUtil(spark: SparkSession) {

  val DATE_FORMAT = "yyyy-MM-dd"
  val DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"


  def checkCount(sourceCount: Any,targetCount: Any): Boolean = {
    var countMatch = false
    if(toInt(sourceCount) == toInt(targetCount))
      countMatch = true
    return countMatch
  }

  def checkSum(sourceSum: Double, targetSum: Double): Boolean = {
    var sumMatch = false
    if(sourceSum == targetSum)
      sumMatch = true
    return sumMatch
  }

  def checkStringValues(sourceValue: String, targetValue: String): Boolean = {
    var valueMatch = false
    if(sourceValue == targetValue)
      valueMatch = true
    return valueMatch
  }

  def checkSimilarity(sourceRecords: DataFrame, targetRecords: DataFrame): Long = {
    var recordMatch = true
    var unMatchedRecordCount : Long = 0
    var targetVariance = targetRecords.except(sourceRecords)
    var sourceVariance = sourceRecords.except(targetRecords)
    if (checkCount(sourceRecords.count(),targetRecords.count())){
      if ((targetVariance != null | sourceVariance != null) & (sourceVariance.count() > 0 | targetVariance.count() > 0 ))
        recordMatch = false
    }
    else
      recordMatch = false
    if  (!recordMatch) {
    {
      if (targetVariance != null) {
          unMatchedRecordCount = unMatchedRecordCount + targetVariance.count()
          println(targetVariance.count().toString + " unmatched target records. Sample unmatched target records are : ")
          println(targetVariance.show(10))
      }
      if (sourceVariance != null)
          unMatchedRecordCount = unMatchedRecordCount + sourceVariance.count()
          println(sourceVariance.count().toString + " unmatched source records. Sample unmatched source records are : ")
          println(sourceVariance.show(10))
    }
    }
    return unMatchedRecordCount
  }

  def checkForZeroRecords(targetCount: Long): Boolean = {
    var zeroRecordMatch = false
    if(targetCount == 0)
      zeroRecordMatch = true
    return zeroRecordMatch
  }

  def checkDateFormat(targetRecords: DataFrame): Long = {
    var incorrectDateRecordCount : Long = 0
    return incorrectDateRecordCount
  }

  def checkDatetimeFormat(targetRecords: DataFrame): Long = {
    var incorrectDateRecordCount : Long = 0
    return incorrectDateRecordCount
  }

  def toInt(x: Any): Option[Int] = x match {
    case i: Int => Some(i)
    case _ => None
  }
  def toDouble(x: Any): Option[Double] = x match {
    case i: Double => Some(i)
    case _ => None
  }
}