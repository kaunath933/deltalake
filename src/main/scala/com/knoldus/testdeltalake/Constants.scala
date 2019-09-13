package com.knoldus.testdeltalake

object Constants {

  val SparkAppName: String = "SparkApp"
  val SparkMaster: String = "local[1]"
  val SparkInputSource: String = "kafka"
  val SparkKafkaServerKey: String = "kafka.bootstrap.servers"
  val SparkKafkaServerValue: String = "localhost:9092"
  val SparkKafkaTopicKey: String = "subscribe"
  val SparkKafkaTopicValue: String = "cake"
  val CastToStringValue: String = "CAST(value AS STRING)"
  val CastToStringKey: String = "CAST(key AS STRING)"
  val ExistingNameOfColumn: String = "count(1)"
  val NewNameOfColumn: String = "Count"
  val SelectColumn: String = "value"
  val ParquetFilePath: String = "./deltafiles"
  val CheckPointLocation: String = "checkpointLocation"
  val CheckPointPath: String = "./checkpointFiles"
  val FileFormat: String = "delta"
  val SaveMode: String = "complete"

}
