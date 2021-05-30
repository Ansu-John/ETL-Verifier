package com.scala.verifier
import com.scala.verifier.params.{Parameter, TableParameters}
import org.apache.spark.sql.SparkSession
import com.scala.verifier.table.Table

object ETLMain {
  var parameter = new Parameter

  def main(args: Array[String]): Unit = {
    for (arg <-  args) {
      println(arg)
      if (arg.toUpperCase == "DAY1")
        parameter.setVerificationType("DAY1")
      if (arg.toUpperCase == "UAT")
        parameter.setEnvironment("UAT")
      if (arg.toUpperCase == "QA")
        parameter.setEnvironment("QA")
      if (arg.toUpperCase == "DETAIL")
        parameter.setMode("DETAIL")
    }
    // set tables to true based on arguments
    println("VerificationType = " + parameter.getVerificationType() + " environment = " + parameter.getEnvironment()
      + " mode = " + parameter.getMode() + " bizdate = " + parameter.getBizDate())
    var config = parameter.getConfigObj()
    var targetClass = config.targetClass
    println("targetClass = " + targetClass.toString())
     for(className <- targetClass.keys) {
    {
      var tableName = className
      var classFullName = "com.scala.verifier.table."+ targetClass(className)
      println("tableName = " + tableName + " classFullName = " + classFullName)
      var tableParam = new TableParameters(parameter, className)
      var tableObj = objectBuilder(classFullName)
      tableObj.setTableParameters(tableParam)
      tableObj.basic_validation()
      if(parameter.getMode() == "DETAIL")
        tableObj.detail_validation()
    }
    parameter.getReportGeneratorObj().writeToConsole()
     }
  }
  def objectBuilder(name : String) : Table = {
    var table:Table = Class.forName(name).newInstance().asInstanceOf[Table]
    println("tableName = " + name + " table = " + table.toString())
    return table
  }
}
