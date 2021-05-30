package com.scala.verifier.util
import org.apache.spark.sql.{DataFrame, SparkSession}

class CommonUtil(spark: SparkSession) {

  def createSourceDF(tableName: String):DataFrame = {
    println("Creating source DF for tableName " + tableName)
    var path = ""
    if (tableName == "TABLE1")
      path = "resources/table/table1Source.csv"
    else
      path = "resources/table/table2Source.csv"
    return spark.read.format("csv").option("delimiter", ",").option("header", "true").load(path)
  }

  def createTargetDF(tableName: String):DataFrame = {
    println("Creating target DF for tableName " + tableName)
    var path = ""
    if (tableName == "TABLE1")
      path = "resources/table/table1Target.csv"
    else
      path = "resources/table/table2Target.csv"
    return spark.read.format("csv").option("delimiter", ",").option("header", "true").load(path)
  }

  def createTransformDF(tableName: String):DataFrame = {
    println("Creating transformation matrix DF for tableName " + tableName)
    return spark.read.format("csv").option("delimiter", ",").option("header", "true").load("resources/table/transformationMatrix.csv")
  }
}
