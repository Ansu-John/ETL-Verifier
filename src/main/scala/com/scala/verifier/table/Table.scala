package com.scala.verifier.table

import com.scala.verifier.params.TableParameters

trait Table  {
  def setTableParameters (parameters:TableParameters)
  def basic_validation(): Unit
  def detail_validation(): Unit
}
