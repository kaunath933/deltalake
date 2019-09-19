package com.kaushik.testdeltalake

import com.kaushik.testdeltalake.util.Constants
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class OutPutSourceSpec extends FlatSpec with BeforeAndAfterAll with OutputSource {

  val sparkSession: SparkSession = SparkSession
    .builder
    .appName(Constants.SparkAppName)
    .master(Constants.SparkMaster)
    .getOrCreate()

  override def afterAll(): Unit = {

    sparkSession.close()
    sparkSession.stop()

  }

  "readFromDelta" should "be compatable with expected dataframe" in {

    val actualResult: DataFrame = readFormDelta(sparkSession, "./deltafiles")

    val expectedResult: DataFrame = sparkSession.read.format(Constants.FileFormat).load("./deltafiles")
    assert(actualResult.first().getString(0) == expectedResult.first().getString(0))

  }

}
