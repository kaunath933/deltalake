package com.kaushik.testdeltalake

import com.kaushik.testdeltalake.util.Constants
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * InputFromat is a Trait which contains the spark session and a method to read from Kafka.
 */
trait InputSource {

  val sparkSession: SparkSession

  /**
   * takeInput() will provide us the DataFrame which is being read from Kafka.
   *
   * @return DataFrame for further process on it.
   */
  def takeInput(): DataFrame = sparkSession
    .readStream
    .format(Constants.SparkInputSource)
    .option(Constants.SparkKafkaServerKey, Constants.SparkKafkaServerValue)
    .option(Constants.SparkKafkaTopicKey, Constants.SparkKafkaTopicValue)
    .load()

  //
}

/**
 * InputFormat is a Singleton object to access the methods from InputAdapterService Trait.
 */
object InputSource extends InputSource {

  val sparkSession: SparkSession = SparkSession
    .builder
    .appName(Constants.SparkAppName)
    .master(Constants.SparkMaster)
    .getOrCreate()

}
