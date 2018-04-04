/*
 * Copyright 2018 PayPal Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.paypal.gimel.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.paypal.gimel._
import com.paypal.gimel.datastreamfactory._
import com.paypal.gimel.logger.Logger

object SparkStreamingKafkaMessageTesting extends App {

  // Initiate Logger
  val logger = Logger(this.getClass.getName)

  import SparkStreamingKafkaStringMessageUtils._

  var params = resolveRunTimeParameters(args)
  val sourceName = params("source")
  val targetName = params.getOrElse("target", "NA")
  val messageFormat = params("messageFormat")
  // Specify Batch Interval for Streaming
  val batchInterval = params.getOrElse("batchInterval", "10").toInt
  val timeOutSeconds = params.getOrElse("timeOutSeconds", "60").toInt
  // Context
  val sparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()
  val sc = sparkSession.sparkContext
  sc.setLogLevel("ERROR")
  val sqlContext = sparkSession.sqlContext
  import sqlContext.implicits._

  val ssc = new StreamingContext(sc, Seconds(batchInterval.toInt))

  val dataStream = DataStream(sc)
  val dataSet = DataSet(sparkSession)
  // Get Reference to Stream
  val streamingResult: StreamingResult = dataStream.read(sourceName)
  // Clear CheckPoint if necessary
  streamingResult.clearCheckPoint("some message")
  streamingResult.dStream.foreachRDD { rdd =>
    val k: RDD[WrappedData] = rdd
    val count = rdd.count()

    logger.info(s"Count is --> ${count}")
    logger.info(s"Message Type Specified is ${messageFormat}...")
    if (count > 0) {

      val df1 = streamingResult.getAsDF(sqlContext, rdd)
      df1.printSchema()
      df1.show(10)
      val updatedDataFrame: DataFrame = df1
      // updatedDataFrame.show
      val col1 = date_format(from_unixtime(col("logtime").divide(1000)), "yyyyMMdd")
      val dfWithDate = updatedDataFrame.withColumn("dfWithDate", col1)
      val dateList = dfWithDate.select("dfWithDate").distinct().collect.flatMap(_.toSeq)
      val dateListMap = dateList.map { date =>
        (date -> dfWithDate.where($"dfWithDate" <=> date))
      }.toMap

      dateListMap.foreach { case (key, dfes) =>
        val schemaMapping: String = s"""{"appStartTime": {"format": "strict_date_optional_time||epoch_millis", "type": "date" }, "appEndTime": {"format": "strict_date_optional_time||epoch_millis", "type": "date"},"jobStartTime": {"format": "strict_date_optional_time||epoch_millis", "type": "date"}, "jobEndTime": {"format": "strict_date_optional_time||epoch_millis", "type": "date"}, "logtime": { "format": "strict_date_optional_time||epoch_millis", "type": "date"}}"""
        val options: Map[String, String] = Map("gimel.es.index.partition.suffix" -> s"$key", "gimel.es.schema.mapping" -> schemaMapping)
        if (targetName != "NA") {
          logger.info(s"Begin Writing To : ${targetName}")
          val res = dataSet.write(targetName, dfes, options)
        }
      }
    }
    streamingResult.saveCurrentCheckPoint()
  }

  dataStream.streamingContext.start()
  dataStream.streamingContext.awaitTerminationOrTimeout(-1)
  dataStream.streamingContext.stop(false, true)
}

object SparkStreamingKafkaStringMessageUtils {

  val logger = Logger(this.getClass.getName)

  def resolveRunTimeParameters(allParams: Array[String]): Map[String, String] = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)
    var paramsMapBuilder: Map[String, String] = Map()
    logger.info(s"All Params From User --> \n${allParams.mkString("\n")}")
    if (allParams.length == 0) {
      throw new Exception("Args Cannot be Empty")
    }

    for (jobParams <- allParams) {
      for (eachParam <- jobParams.split(" ")) {
        paramsMapBuilder += (eachParam.split("=")(0) -> eachParam.split("=", 2)(1))
      }
    }
    logger.info(s"Resolved Params From Code --> ${paramsMapBuilder}")
    paramsMapBuilder
  }
}
