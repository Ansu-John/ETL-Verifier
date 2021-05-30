package com.scala.verifier.table

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.sql.DataFrame

class Config {

  val targetClass: Map[String,String] = Map(
    "TABLE1" -> "SampleTable1",
    "TABLE2" -> "SampleTable2"
  )
  val targetTable: Map[String,String] = Map(
    "TABLE1" -> "table1Target",
    "TABLE2" -> "table2Target"
  )
  val sourceTable: Map[String,String] = Map(
    "TABLE1" -> "table1Source",
    "TABLE2" -> "table2Source"
  )
  val targetDuplicateCheckColumns: Map[String,String] = Map(
    "TABLE1" -> "PRIMARY1,PRIMARY2,PRIMARY3,BUSINESS_DATE",
    "TABLE2" -> "PRIMARY1,PRIMARY2,BUSINESS_DATE"
    //"TABLE2" -> "CREATION_DATE,BUSINESS_DATE" // verify false case
  )
  val sourceDuplicateCheckColumns: Map[String,String] = Map(
    "TABLE1" -> "PRIMARY1,PRIMARY2,PRIMARY3,BUSINESS_DATE",
    "TABLE2" -> "PRIMARY1,PRIMARY2,BUSINESS_DATE"
  )
  val targetTransformationCheckColumns: Map[String,String] = Map(
    "TABLE1" -> "PRIMARY1,PRIMARY2,PRIMARY3,BUSINESS_DATE",
    "TABLE2" -> "PRIMARY1,PRIMARY2,BUSINESS_DATE"
  )
  val sourceTransformationCheckColumns: Map[String,String] = Map(
    "TABLE1" -> "PRIMARY1,PRIMARY2,PRIMARY3,BUSINESS_DATE",
    "TABLE2" -> "PRIMARY1,PRIMARY2,BUSINESS_DATE"
  )
  val sourceJoin : Map[String,String] = Map(
    "TABLE1_CODEVALUE" -> "CODEVALUE"
  )
  val transformJoin : Map[String,String] = Map(
    "TABLE1_CODEVALUE" -> "CODE"
  )
  val sourceDate : Map[String,String] = Map(
    "TABLE1_CODEVALUE" -> "CREATION_DATE"
  )
  val transformValidFrom : Map[String,String] = Map(
    "TABLE1_CODEVALUE" -> "VALID_FROM"
  )
  val transformValidTo : Map[String,String] = Map(
    "TABLE1_CODEVALUE" -> "VALID_TO"
  )
  val columnsToDrop : Map[String,List[String]] = Map(
    "TABLE1_CODEVALUE" -> List("CODEVALUE","CODE","VALID_FROM","VALID_TO")
  )
  val sourceColumn : Map[String,String] = Map(
    "TABLE1_CODEVALUE" -> "CREATION_DATE,PRIMARY1,PRIMARY2,PRIMARY3,CODEVALUE",
    "TABLE1_PRIMARY" -> "PRIMARY1,PRIMARY2,PRIMARY3,BUSINESS_DATE",
    "TABLE2_PRIMARY" -> "PRIMARY1,PRIMARY2,BUSINESS_DATE"
  )
  val targetColumn : Map[String,String] = Map(
    "TABLE1_CODEVALUE" -> "CREATION_DATE,PRIMARY1,PRIMARY2,PRIMARY3,CODEVALUE ",
    "TABLE1_PRIMARY" -> "PRIMARY1,PRIMARY2,PRIMARY3,BUSINESS_DATE",
    "TABLE2_PRIMARY" -> "PRIMARY1,PRIMARY2,BUSINESS_DATE"
  )
  val sourceSQL: Map[String,String] = Map(
    "COUNT_DAILY" -> "select count(*) from %s where BUSINESS_DATE = '%s'",
    "COUNT_DAY1" -> "select count(*) from %s where BUSINESS_DATE <= '%s'",
    "NULL_DAILY" -> "select count(*) from %s where BUSINESS_DATE = '%s' and %s is null",
    "NULL_DAY1" -> "select count(*) from %s where BUSINESS_DATE <= '%s' and %s is null",
    "SUM_DAILY" -> "select sum(%s) from %s where BUSINESS_DATE = '%s'",
    "SUM_DAY1" -> "select sum(%s) from %s where BUSINESS_DATE <= '%s'",
    "DUPLI_DAILY" -> "select count(*), %s from %s where BUSINESS_DATE = '%s' group by %s having count(*) > 1",
    "DUPLI_DAY1"-> "select count(*), %s from %s where BUSINESS_DATE <= '%s' group by %s having count(*) > 1",
    "FLAG_DAILY" -> "select count(*) from %s where BUSINESS_DATE = '%s' and %s = %s",
    "FLAG_DAY1" -> "select count(*) from %s where BUSINESS_DATE <= '%s' and %s = %s" ,
    "DATE_DAILY" -> "select %s from %s where BUSINESS_DATE = '%s' and %s is not null",
    "DATE_DAY1" -> "select %s from %s where BUSINESS_DATE <= '%s' and %s is not null",
    "TRANSFORM_DAILY" -> "select %s from %s where BUSINESS_DATE = '%s'",
    "TRANSFORM_DAY1" -> "select %s from %s where BUSINESS_DATE <= '%s'",
    "SIMILARITY_DAILY" -> "select %s from %s where BUSINESS_DATE = '%s'",
    "SIMILARITY_DAY1" -> "select %s from %s where BUSINESS_DATE <= '%s'",
    "TRANSCOUNT_DAILY" -> "select count(*) from %s where BUSINESS_DATE = '%s' and %s = %s",
    "TRANSCOUNT_DAY1" -> "select count(*) from %s where BUSINESS_DATE <= '%s' and %s = %s"
  )

  val targetSQL: Map[String,String] = Map(
    "COUNT_DAILY" -> "select count(*) from %s where BUSINESS_DATE = '%s'",
    "COUNT_DAY1" -> "select count(*) from %s where BUSINESS_DATE <= '%s'",
    "NULL_DAILY" -> "select count(*) from %s where BUSINESS_DATE = '%s' and %s is null",
    "NULL_DAY1" -> "select count(*) from %s where BUSINESS_DATE <= '%s' and %s is null",
    "SUM_DAILY" -> "select sum(%s) from %s where BUSINESS_DATE = '%s'",
    "SUM_DAY1" -> "select sum(%s) from %s where BUSINESS_DATE <= '%s'",
    "DUPLI_DAILY" -> "select count(*), %s from %s where BUSINESS_DATE = '%s' group by %s having count(*) > 1",
    "DUPLI_DAY1"-> "select count(*), %s from %s where BUSINESS_DATE <= '%s' group by %s having count(*) > 1",
    "FLAG_DAILY" -> "select sum(%s) from %s where BUSINESS_DATE = '%s'",
    "FLAG_DAY1" -> "select sum(%s) from %s where BUSINESS_DATE <= '%s'" ,
    "DATE_DAILY" -> "select %s from %s where BUSINESS_DATE = '%s' and %s is not null",
    "DATE_DAY1" -> "select %s from %s where BUSINESS_DATE <= '%s' and %s is not null",
    "TRANSFORM_DAILY" -> "select %s from %s where BUSINESS_DATE = '%s'",
    "TRANSFORM_DAY1" -> "select %s from %s where BUSINESS_DATE <= '%s'",
    "SIMILARITY_DAILY" -> "select %s from %s where BUSINESS_DATE = '%s'",
    "SIMILARITY_DAY1" -> "select %s from %s where BUSINESS_DATE <= '%s'",
    "TRANSCOUNT_DAILY" -> "select count(*) from %s where BUSINESS_DATE = '%s' and %s = %s",
    "TRANSCOUNT_DAY1" -> "select count(*) from %s where BUSINESS_DATE <= '%s' and %s = %s",
    "DAY_DAILY" -> "select distinct day(%s) from %s where BUSINESS_DATE = '%s' and %s is not null",
    "DAY_DAY1" -> "select distinct day(%s) from %s where BUSINESS_DATE <= '%s' and %s is not null",
  )

  val transformSQL: Map[String,String] = Map(
    "TABLE1_CODEVALUE" -> "select * from codeTable"
  )
}
