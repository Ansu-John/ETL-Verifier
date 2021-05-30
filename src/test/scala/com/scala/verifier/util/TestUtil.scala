package com.scala.verifier.util

import org.scalatest.FunSpec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

class TestUtil extends FunSpec {

  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("ETLVerifier")
      .getOrCreate()
  }
  describe("#checkCount") {
    it("checkCount method should return boolean values") {
      var util = new com.scala.verifier.util.ValidatorUtil(spark)
      assert(util.checkCount(7, 7) === true)
      assert(util.checkCount(7, 8) === false)
    }
  }

  describe("#checkSum") {
    it("checkSum method should return boolean values") {
      var util = new com.scala.verifier.util.ValidatorUtil(spark)
      assert(util.checkSum(86764127341.9867492, 86764127341.9867492) === true)
      assert(util.checkSum(86764127341.9867492, 86764127341.99) === true)
      assert(util.checkSum(86764127341.4333464, 86764127341.43) === true)
      assert(util.checkSum(86764127341.9867492, 86764127341.98) === false)
      assert(util.checkSum(86764127341.9867492, 648273648273548.786238172) === false)
    }
  }

  describe("#checkStringValues") {
    it("checkStringValues method should return boolean values") {
      var util = new com.scala.verifier.util.ValidatorUtil(spark)
      assert(util.checkStringValues("test","test") === true)
      assert(util.checkStringValues("test","testvalue") === false)
    }
  }

  describe( "#checkSimilarity") {
    it("checkSimilarity method should return mismatch record count") {
      val spark: SparkSession = {
        SparkSession
          .builder()
          .master("local")
          .appName("ETLVerifier")
          .getOrCreate()
      }
      var sourceDf = spark.read.csv("resources/sourceDf.csv")
      var targetDf = spark.read.csv("resources/sourceDf.csv")
      var sourceDf1 = spark.read.csv("resources/sourceDf.csv")
      var targetDf1 = spark.read.csv("resources/targetDf.csv")

    var util = new com.scala.verifier.util.ValidatorUtil(spark)
    assert(util.checkSimilarity(sourceDf,targetDf)  == 0)
    assert(util.checkSimilarity(sourceDf1,targetDf1) != 0)
  }
}
  describe( "#checkForZeroRecords") {
    it("checkForZeroRecords method should return boolean values") {
    var util = new com.scala.verifier.util.ValidatorUtil(spark)
    assert(util.checkForZeroRecords(0) === true )
    assert(util.checkForZeroRecords(15) === false)
      assert(util.checkForZeroRecords(-1) === false)
  }
}

  /*
    describe( "#checkDateFormat") {
      it("checkDateFormat method should return mismatch record count") {
        val spark: SparkSession = {
          SparkSession
            .builder()
            .master("local")
            .appName("ETLVerifier")
            .getOrCreate()
        }
        var dateDf = spark.read.csv("resources/date.csv")
        var incorrectDateDf = spark.read.csv("resources/incorrectDate.csv")

        var util = new com.scala.verifier.util.Util()
        assert(util.checkDateFormat(dateDf)  === 0)
        assert(util.checkDateFormat(incorrectDateDf) === 3)
      }
    }

    describe( "#checkDatetimeFormat") {
      it("checkDatetimeFormat method should return mismatch record count") {
        val spark: SparkSession = {
          SparkSession
            .builder()
            .master("local")
            .appName("ETLVerifier")
            .getOrCreate()
        }
        var dateDf = spark.read.csv("resources/datetime.csv")
        var incorrectDateDf = spark.read.csv("resources/incorrectDatetime.csv")

        var util = new com.scala.verifier.util.Util()
        assert(util.checkDateFormat(dateDf)  === 0)
        assert(util.checkDateFormat(incorrectDateDf) === 2)
      }
    }
    */

}
