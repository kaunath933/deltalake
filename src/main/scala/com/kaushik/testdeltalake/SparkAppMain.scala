package com.kaushik.testdeltalake

import com.kaushik.testdeltalake.util.Constants
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * SparkAppMain is the main working unit of project.
 * Which gets a dataFrame from InputSource and persists it on OutputSource .
 */
object SparkAppMain {

  def main(args: Array[String]): Unit = {

    val dataFrame: DataFrame = InputSource.takeInput()

    val sparkSession: SparkSession = SparkSession
      .builder
      .appName(Constants.SparkAppName)
      .master(Constants.SparkMaster)
      .getOrCreate()

    import sparkSession.implicits._

    val dataSet: Dataset[String] =
      dataFrame.selectExpr(Constants.CastToStringValue).as[String]

    val result: DataFrame = dataSet
      .flatMap(value => value.split("\\s+"))
      .groupByKey(x => if (x == "hello") x + " world" else x)
      .count()
      .withColumnRenamed(Constants.ExistingNameOfColumn, Constants.NewNameOfColumn)

    OutputSource.writeToDelta(result, Constants.ParquetFilePath, Constants.CheckPointPath)

    val readFile: DataFrame = OutputSource.readFormDelta(sparkSession, Constants.ParquetFilePath)

    readFile.show()

  }

}
