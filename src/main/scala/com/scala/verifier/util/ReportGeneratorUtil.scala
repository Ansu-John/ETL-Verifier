package com.scala.verifier.util

import org.apache.spark.sql.SparkSession

class ReportGeneratorUtil(spark: SparkSession) {

  var testResultList:List[List[String]] = List(List("testcaseDesc", "sourceTable", "targetTable","sourceSql","targetSql","totalSourceRecords","totalTargetRecords","sourceResult","targetResult","variance","testStatus"))

  def writeToList(testResult : List[String]) = {
    testResultList = testResultList :+ testResult
  }
  def writeToConsole()={
    for (testResult <- testResultList){
      println(testResult);
    }
  }
}
