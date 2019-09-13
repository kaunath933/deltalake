package com.knoldus.testdeltalake

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkApp {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession
      .builder
      .appName(Constants.SparkAppName)
      .master(Constants.SparkMaster)
      .getOrCreate()

    import sparkSession.implicits._

    val dataFrame: DataFrame = sparkSession
      .readStream
      .format(Constants.SparkInputSource)
      .option(Constants.SparkKafkaServerKey, Constants.SparkKafkaServerValue)
      .option(Constants.SparkKafkaTopicKey, Constants.SparkKafkaTopicValue)
      .load()

    val dataSet: Dataset[String] =
      dataFrame.selectExpr(Constants.CastToStringValue).as[String]

    val result: DataFrame = dataSet
      .flatMap(value => value.split("\\s+"))
      .groupByKey(x => if (x == "hello") x + " world" else x)
      .count()
      .withColumnRenamed(Constants.ExistingNameOfColumn, Constants.NewNameOfColumn)

    val a: DataFrame = sparkSession.read.format(Constants.FileFormat).load(Constants.ParquetFilePath)
    a.show()

    result.select(Constants.SelectColumn).writeStream
      .format(Constants.FileFormat)
      .outputMode(Constants.SaveMode)
      .option(Constants.CheckPointLocation, Constants.CheckPointPath)
      .start(Constants.ParquetFilePath)
      .awaitTermination()

  }

}
