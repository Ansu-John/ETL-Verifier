package com.scala.verifier.table

import com.scala.verifier.params.TableParameters
import com.scala.verifier.util.Verifier

class SampleTable2 extends Table {
  private var tableParameters: TableParameters = null

  def setTableParameters (parameters:TableParameters) = {
    tableParameters = parameters
  }
  def getTableParameters(): TableParameters = {
    return tableParameters
  }
  def basic_validation(): Unit ={
    println("Validating Table 2 ")
    var verifier : Verifier = new Verifier(tableParameters)
    verifier.checkRecordCount()
    verifier.checkNullRecords("PRIMARY1")
    verifier.checkNullRecords("PRIMARY2")
    verifier.checkSum("SUMVALUE1")
    verifier.checkDuplicateRecords()
  }

  def detail_validation() ={
    println("Validating Table 1 ")
    var verifier : Verifier = new Verifier(tableParameters)
    verifier.checkSimilarity("PRIMARY")
  }

}
