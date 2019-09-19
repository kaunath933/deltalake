package com.kaushik.testdeltalake

import com.kaushik.testdeltalake.util.Constants
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class InputSourceSpec extends FlatSpec with BeforeAndAfterAll with InputSource {

  val sparkSession: SparkSession = SparkSession
    .builder
    .appName(Constants.SparkAppName)
    .master(Constants.SparkMaster)
    .getOrCreate()

  override def afterAll(): Unit = {

    sparkSession.close()
    sparkSession.stop()

  }

  "takeInput method" should "give a dataFrame which should not be null" in {

    val actualResult: DataFrame = takeInput()

    assert(actualResult != null)

  }

}
