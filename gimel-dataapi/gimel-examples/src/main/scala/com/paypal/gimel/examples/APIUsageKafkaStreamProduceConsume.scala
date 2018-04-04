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

import scala.language.implicitConversions

import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import com.paypal.gimel._
import com.paypal.gimel.datastreamfactory.StreamingResult
import com.paypal.gimel.logger.Logger

/**
  * Demo's Kafka Producer and Consumer for DataStream
  */
object APIUsageKafkaStreamProduceConsume extends App {

  // Initiate Logger
  val logger = Logger(this.getClass.getName)
  // Specify Batch Interval for Streaming
  val batchInterval = 10
  // Context
  val sparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()
  val sc = sparkSession.sparkContext
  sc.setLogLevel("ERROR")
  val sqlContext = sparkSession.sqlContext

  // Initiate DStream
  val dataStream = DataStream(sc)

  // Option to Run the Code in spark-submit mode,
  // if a table name is passed - it is considered. Else, default of kafka_testing_flights is read
  val datasetName = if (args.isEmpty) {
    "kafka_testing_flights"
  } else {
    args(0)
  }

  // Get Reference to Stream
  val streamingResult: StreamingResult = dataStream.read(datasetName)

  // Clear CheckPoint if necessary
  streamingResult.clearCheckPoint("some message")

  // Helper for Clients
  streamingResult.dStream.foreachRDD { rdd =>

    val count = rdd.count()

    if (count > 0) {

      /**
        * Mandatory | Get Offset for Current Window, so we can checkpoint at the end of this window's operation
        */

      streamingResult.getCurrentCheckPoint(rdd)

      /**
        * Begin | User's Usecases
        */

      // Sample UseCase | Display Count
      logger.debug("count is -->")
      logger.debug(count)

      // Sample UseCase | Get Avro Generic Record
      val rddAvro: RDD[GenericRecord] = streamingResult.convertBytesToAvro(rdd)
      rddAvro.map(x => x.toString)
      logger.debug("sample records from Avro-->")
      rddAvro.map(x => x.toString).take(10).foreach(record => logger.debug(record))

      // Sample UseCase | Convert to DataFrame
      val df: DataFrame = streamingResult.convertAvroToDF(sqlContext, rddAvro)
      logger.debug("sample records -->")
      df.show(5)

      // JSON / String / Bytes (Avro) / Bytes (CDH) --> All can be deserialized into Spark DataFrame via this function
      streamingResult.getAsDF(sqlContext, rdd)


      /**
        * End | User's Usecases
        */

      /**
        * Mandatory | Save Current Window - CheckPoint
        */

      streamingResult.saveCurrentCheckPoint()
    }
  }

  // Start the Context
  dataStream.streamingContext.start()
  dataStream.streamingContext.awaitTermination()

}
