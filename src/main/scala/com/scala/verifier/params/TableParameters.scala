package com.scala.verifier.params

class TableParameters(parameter: Parameter, tableName :String){

  private var targetTable = parameter.getConfigObj().targetTable(tableName)
  private var sourceTable = parameter.getConfigObj().sourceTable(tableName)

  def getParameter(): Parameter ={
    return parameter
  }
  def getTableName(): String ={
    return tableName
  }
  def getTargetTable(): String ={
    return targetTable
  }
  def getSourceTable(): String ={
    return sourceTable
  }

}
