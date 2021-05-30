package com.scala.verifier.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.scala.verifier.table.Config
import com.scala.verifier.params.{Parameter, TableParameters}
import org.apache.spark.sql.functions._

class Verifier(tableParams: TableParameters) {

  val tableName : String = tableParams.getTableName()
  val sourceTable: String = tableParams.getSourceTable()
  val targetTable: String = tableParams.getTargetTable()
  val params: Parameter = tableParams.getParameter()
  val bizDate = params.getBizDate()
  val verificationType: String = params.getVerificationType()
  val mode: String = params.getMode()
  val config = params.getConfigObj()
  val util = params.getCommonObj()
  val validator = params.getValidatorObj()
  val writer = params.getReportGeneratorObj()
  var spark = params.getSparkSession()

  // Added to create temp table to query against instead of hive table
  var targetDF: DataFrame = util.createTargetDF(tableName)
  var sourceDF: DataFrame = util.createSourceDF(tableName)
  var transformationDF: DataFrame = util.createTransformDF(tableName)
  var totalTargetRecords = targetDF.count()
  var totalSourceRecords = sourceDF.count()
  if (tableName == "TABLE1") {
    println("Creating temp table for table 1")
    sourceDF.createOrReplaceTempView("table1Source")
    targetDF.createOrReplaceTempView("table1Target")
  }
  else{
    println("Creating temp table for table 2")
    sourceDF.createOrReplaceTempView("table2Source")
    targetDF.createOrReplaceTempView("table2Target")
  }
  transformationDF.createOrReplaceTempView("codeTable")

  def checkRecordCount(): Unit ={
    var testcaseDescription = "Verifying total number of records in " + tableName
    var targetSQL = config.targetSQL("COUNT_DAY1").format(targetTable,bizDate)
    var sourceSQL = config.sourceSQL("COUNT_DAY1").format(sourceTable,bizDate)
    if(verificationType == "DAILY") {
      testcaseDescription = testcaseDescription + " for " + bizDate
      targetSQL = config.targetSQL("COUNT_DAILY").format(targetTable,bizDate)
      sourceSQL = config.sourceSQL("COUNT_DAILY").format(sourceTable,bizDate)
    }
    println("testcaseDescription = " + testcaseDescription +" targetSQL = " + targetSQL +" sourceSQL = " + sourceSQL)
    var targetRecord = spark.sql(targetSQL)
    var sourceRecord = spark.sql(sourceSQL)
    var sourceResult = sourceRecord.first()
    var targetResult =targetRecord.first()
    var variance : Long = sourceResult.getLong(0) - targetResult.getLong(0)

    println("sourceResult = " + sourceResult +" targetResult = " + targetResult + " variance = " + variance )
    if(validator.checkCount(sourceResult,targetResult)) {
      val testResult :List[String] = List(testcaseDescription, sourceTable, targetTable,sourceSQL,targetSQL,
        totalSourceRecords.toString,totalTargetRecords.toString,sourceResult.toString,targetResult.toString,variance.toString,"Pass")
      writer.writeToList(testResult)
    }
    else{
      val testResult :List[String] = List(testcaseDescription, sourceTable, targetTable,sourceSQL,targetSQL,
        totalSourceRecords.toString,totalTargetRecords.toString,sourceResult.toString,targetResult.toString,variance.toString,"Fail")
      writer.writeToList(testResult)
    }
  }

  def checkTransformation(column:String) ={

    var testcaseDescription = "Verifying transformation of column " + column + " of table " + tableName
    var sourceColumn = config.sourceColumn("%s_%s".format(tableName,column))
    var targetColumn = config.targetColumn("%s_%s".format(tableName,column))
    var targetSQL = config.targetSQL("TRANSFORM_DAY1").format(targetColumn,targetTable,bizDate)
    var sourceSQL = config.sourceSQL("TRANSFORM_DAY1").format(sourceColumn,sourceTable,bizDate)
    if(verificationType == "DAILY") {
      testcaseDescription = testcaseDescription + " for " + bizDate
      targetSQL = config.targetSQL("TRANSFORM_DAILY").format(targetColumn,targetTable,bizDate)
      sourceSQL = config.sourceSQL("TRANSFORM_DAILY").format(sourceColumn,sourceTable,bizDate)
    }
    var transformSQL = config.transformSQL("%s_%s".format(tableName,column))
    println("testcaseDescription = " + testcaseDescription +" targetSQL = " + targetSQL +" sourceSQL = " + sourceSQL +" transformSQL = " + transformSQL)
    var targetResult = spark.sql(targetSQL)
    var sourceResult = spark.sql(sourceSQL)
    var transformResult = spark.sql(transformSQL)
    var sourceJoin = config.sourceJoin("%s_%s".format(tableName,column))
    var transformJoin = config.transformJoin("%s_%s".format(tableName,column))
    var sourceDate = config.sourceDate("%s_%s".format(tableName,column))
    var transformValidFrom = config.transformValidFrom("%s_%s".format(tableName,column))
    var transformValidTo = config.transformValidTo("%s_%s".format(tableName,column))
    var columnsToDrop =  config.columnsToDrop("%s_%s".format(tableName,column))

    println("sourceJoin = " + sourceJoin +" transformJoin = " + transformJoin + " sourceDate = " + sourceDate +
      " transformValidFrom = " + transformValidFrom + " transformValidTo = " + transformValidTo + " columnsToDrop = " + columnsToDrop)
    transformResult = transformResult.withColumn(transformValidFrom,col(transformValidFrom).cast("date"))
    transformResult = transformResult.withColumn(transformValidTo,col(transformValidTo).cast("date"))
    println("transformResult schema is as below")
    transformResult.printSchema()
    sourceResult = sourceResult.withColumn(sourceDate,col(sourceDate).cast("date"))
    println("sourceResult schema is as below")
    sourceResult.printSchema()
    targetResult = targetResult.withColumn(sourceDate,col(sourceDate).cast("date"))
    println("targetResult schema is as below")
    targetResult.printSchema()
    var transformedDF = sourceResult.join(transformResult,
      sourceResult(sourceJoin) === transformResult(transformJoin) &&
      sourceResult(sourceDate) <= transformResult(transformValidTo) &&
      sourceResult(sourceDate) >= transformResult(transformValidFrom),"left")
    println("transformedDF schema after join is as below")
    transformedDF.printSchema()
    for (column <- columnsToDrop)
      {
        transformedDF = transformedDF.drop(col(column))
      }
    println("transformedDF schema after dropping columns is as below")
    transformedDF.printSchema()
    var nonMatched = validator.checkSimilarity(transformedDF, targetResult)
    if(nonMatched == 0) {
      val testResult :List[String] = List(testcaseDescription, sourceTable, targetTable,sourceSQL,targetSQL,
        totalSourceRecords.toString,totalTargetRecords.toString,sourceResult.count().toString,targetResult.count().toString,nonMatched.toString,"Pass")
      writer.writeToList(testResult)
    }
    else{
      val testResult :List[String] = List(testcaseDescription, sourceTable, targetTable,sourceSQL,targetSQL,
        totalSourceRecords.toString,totalTargetRecords.toString,sourceResult.count().toString,targetResult.count().toString,nonMatched.toString,"Fail")
      writer.writeToList(testResult)
    }
  }

 def checkSimilarity(column: String) ={
   var testcaseDescription = "Verifying transformation of column " + column + " of table " + tableName
   var sourceColumn = config.sourceColumn("%s_%s".format(tableName,column))
   var targetColumn = config.targetColumn("%s_%s".format(tableName,column))
   var targetSQL = config.targetSQL("TRANSFORM_DAY1").format(targetColumn,targetTable,bizDate)
   var sourceSQL = config.sourceSQL("TRANSFORM_DAY1").format(sourceColumn,sourceTable,bizDate)
   if(verificationType == "DAILY") {
     testcaseDescription = testcaseDescription + " for " + bizDate
     targetSQL = config.targetSQL("TRANSFORM_DAILY").format(targetColumn,targetTable,bizDate)
     sourceSQL = config.sourceSQL("TRANSFORM_DAILY").format(sourceColumn,sourceTable,bizDate)
   }
   println("testcaseDescription = " + testcaseDescription +" targetSQL = " + targetSQL +" sourceSQL = " + sourceSQL )
   var targetResult = spark.sql(targetSQL)
   var sourceResult = spark.sql(sourceSQL)
   var nonMatched = validator.checkSimilarity(sourceResult, targetResult)
   if(nonMatched == 0) {
     val testResult :List[String] = List(testcaseDescription, sourceTable, targetTable,sourceSQL,targetSQL,
       totalSourceRecords.toString,totalTargetRecords.toString,sourceResult.count().toString,targetResult.count().toString,nonMatched.toString,"Pass")
     writer.writeToList(testResult)
   }
   else{
     val testResult :List[String] = List(testcaseDescription, sourceTable, targetTable,sourceSQL,targetSQL,
       totalSourceRecords.toString,totalTargetRecords.toString,sourceResult.count().toString,targetResult.count().toString,nonMatched.toString,"Fail")
     writer.writeToList(testResult)
   }
 }

  def checkSum(column: String) = {

    var testcaseDescription = "Verifying sum of records in column " + column + " of table " + tableName
    var targetSQL = config.targetSQL("SUM_DAY1").format(column,targetTable,bizDate)
    var sourceSQL = config.sourceSQL("SUM_DAY1").format(column,sourceTable,bizDate)
    if(verificationType == "DAILY") {
      testcaseDescription = testcaseDescription + " for " + bizDate
      targetSQL = config.targetSQL("SUM_DAILY").format(column,targetTable,bizDate)
      sourceSQL = config.sourceSQL("SUM_DAILY").format(column,sourceTable,bizDate)
    }
    println("testcaseDescription = " + testcaseDescription +" targetSQL = " + targetSQL +" sourceSQL = " + sourceSQL)
    var targetRecord = spark.sql(targetSQL)
    var sourceRecord = spark.sql(sourceSQL)
    var sourceResult = sourceRecord.first().getDouble(0)
    var targetResult =targetRecord.first().getDouble(0)
    var variance : Double = sourceResult - targetResult

    println("sourceResult = " + sourceResult +" targetResult = " + targetResult + " variance = " + variance )
    if(validator.checkSum(sourceResult, targetResult)) {
      val testResult :List[String] = List(testcaseDescription, sourceTable, targetTable,sourceSQL,targetSQL,
        totalSourceRecords.toString,totalTargetRecords.toString,sourceResult.toString,targetResult.toString,variance.toString,"Pass")
      writer.writeToList(testResult)
    }
    else{
      val testResult :List[String] = List(testcaseDescription, sourceTable, targetTable,sourceSQL,targetSQL,
        totalSourceRecords.toString,totalTargetRecords.toString,sourceResult.toString,targetResult.toString,variance.toString,"Fail")
      writer.writeToList(testResult)
    }
  }

  def checkNullRecords(column: String) = {
    var testcaseDescription = "Verifying number of null records in column " + column + " of table " + tableName
    var targetSQL = config.targetSQL("NULL_DAY1").format(targetTable,bizDate,column)
    var sourceSQL = config.sourceSQL("NULL_DAY1").format(sourceTable,bizDate,column)
    if(verificationType == "DAILY") {
      testcaseDescription = testcaseDescription + " for " + bizDate
      targetSQL = config.targetSQL("NULL_DAILY").format(targetTable,bizDate,column)
      sourceSQL = config.sourceSQL("NULL_DAILY").format(sourceTable,bizDate,column)
    }
    println("testcaseDescription = " + testcaseDescription +" targetSQL = " + targetSQL +" sourceSQL = " + sourceSQL)
    var targetRecord = spark.sql(targetSQL)
    var sourceRecord = spark.sql(sourceSQL)
    var sourceResult = sourceRecord.first().getLong(0)
    var targetResult =targetRecord.first().getLong(0)
    var variance : Long = targetResult

    println("sourceResult = " + sourceResult +" targetResult = " + targetResult + " variance = " + variance )
    if(validator.checkForZeroRecords(variance)) {
      val testResult :List[String] = List(testcaseDescription, sourceTable, targetTable,sourceSQL,targetSQL,
        totalSourceRecords.toString,totalTargetRecords.toString,sourceResult.toString,targetResult.toString,variance.toString,"Pass")
      writer.writeToList(testResult)
    }
    else{
      val testResult :List[String] = List(testcaseDescription, sourceTable, targetTable,sourceSQL,targetSQL,
        totalSourceRecords.toString,totalTargetRecords.toString,sourceResult.toString,targetResult.toString,variance.toString,"Fail")
      writer.writeToList(testResult)
    }
  }

  def checkDuplicateRecords() = {

    var testcaseDescription = "Verifying number of duplicate records for table " + tableName
    var targetColumns  = config.targetDuplicateCheckColumns(tableName)
    var sourceColumns  = config.sourceDuplicateCheckColumns(tableName)
    var targetSQL = config.targetSQL("DUPLI_DAY1").format(targetColumns,targetTable,bizDate,targetColumns)
    var sourceSQL = config.sourceSQL("DUPLI_DAY1").format(sourceColumns,sourceTable,bizDate,sourceColumns)
    if(verificationType == "DAILY") {
      testcaseDescription = testcaseDescription + " for " + bizDate
      targetSQL = config.targetSQL("DUPLI_DAILY").format(targetColumns,targetTable,bizDate,targetColumns)
      sourceSQL = config.sourceSQL("DUPLI_DAILY").format(sourceColumns,sourceTable,bizDate,sourceColumns)
    }
    println("testcaseDescription = " + testcaseDescription +" targetSQL = " + targetSQL +" sourceSQL = " + sourceSQL)
    var targetRecord = spark.sql(targetSQL)
    var sourceRecord = spark.sql(sourceSQL)
    var sourceResult = sourceRecord.count()
    var targetResult =targetRecord.count()
    var variance : Long = targetResult

    println("sourceResult = " + sourceResult +" targetResult = " + targetResult + " variance = " + variance )
    if(validator.checkForZeroRecords(variance)) {
      val testResult :List[String] = List(testcaseDescription, sourceTable, targetTable,sourceSQL,targetSQL,
        totalSourceRecords.toString,totalTargetRecords.toString,sourceResult.toString,targetResult.toString,variance.toString,"Pass")
      writer.writeToList(testResult)
    }
    else{
      val testResult :List[String] = List(testcaseDescription, sourceTable, targetTable,sourceSQL,targetSQL,
        totalSourceRecords.toString,totalTargetRecords.toString,sourceResult.toString,targetResult.toString,variance.toString,"Fail")
      writer.writeToList(testResult)
    }
  }

}
