package com.kaushik.testdeltalake

import com.kaushik.testdeltalake.util.Constants
import org.apache.spark.sql.{DataFrame, SparkSession}

trait OutputSource {
  /**
   * writeToDelta() writes the data to delta lake
   * the format of the data is in parquet file format
   *
   * @param outputDataFrame is a DataFrame which is used to write to delta table
   * @param filePath        is a path of type String which specifies the where to write the delta table
   */
  def writeToDelta(outputDataFrame: DataFrame, filePath: String, checkPointPath: String) = {

    outputDataFrame.select(Constants.SelectColumn).writeStream
      .format(Constants.FileFormat)
      .outputMode(Constants.SaveMode)
      .option(Constants.CheckPointLocation, checkPointPath)
      .start(filePath)
      .awaitTermination()

  }

  /**
   * readFromDelta() reads the data from a delta lake table and stores in a dataFrame
   *
   * @param fileReadPath is a path of type String which specifies from where we have to read delta table
   * @return DataFrame which can be used to read delta table
   */
  def readFormDelta(sparkSession: SparkSession, fileReadPath: String): DataFrame = {

    val resultDataFrame: DataFrame = sparkSession.read.format(Constants.FileFormat).load(Constants.ParquetFilePath)

    resultDataFrame

  }

}

object OutputSource extends OutputSource
