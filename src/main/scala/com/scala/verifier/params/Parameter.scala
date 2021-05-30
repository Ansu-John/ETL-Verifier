package com.scala.verifier.params

import com.scala.verifier.table.Config
import com.scala.verifier.util.{CommonUtil, ReportGeneratorUtil, ValidatorUtil}
import org.apache.spark.sql.SparkSession

import java.time.LocalDate

class Parameter {

  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("ETLVerifier")
      .getOrCreate()
  }
  private val config:Config = new Config()
  private val util:CommonUtil = new CommonUtil(spark)
  private val validator:ValidatorUtil = new ValidatorUtil(spark)
  private val writer:ReportGeneratorUtil = new ReportGeneratorUtil(spark)
  private var bizDate :LocalDate = java.time.LocalDate.now
  private var verificationType :String = "DAILY"
  private var environment :String = "PROD"
  private var mode : String = "BASE"


  def getConfigObj(): Config ={
    return config
  }
  def getCommonObj(): CommonUtil ={
    return util
  }
  def getValidatorObj(): ValidatorUtil ={
    return validator
  }
  def getReportGeneratorObj(): ReportGeneratorUtil ={
    return writer
  }
  def getSparkSession(): SparkSession ={
    return spark
  }
  def setBizDate(date: LocalDate){
    bizDate= date
  }
  def getBizDate(): LocalDate ={
    return bizDate
  }
  def setVerificationType(vtype: String){
    verificationType= vtype
  }
  def getVerificationType(): String ={
    return verificationType
  }
  def setEnvironment(env: String){
    environment= env
  }
  def getEnvironment(): String ={
    return environment
  }
  def setMode(emode: String){
    mode= emode
  }
  def getMode(): String ={
    return mode
  }
}
