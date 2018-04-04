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

import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.paypal.gimel.{DataSet, DataStream}
import com.paypal.gimel.datastreamfactory.StreamingResult
import com.paypal.gimel.logger.Logger

object GimelDruidRealtimeIngestion {
  val logger = Logger(this.getClass.getName)


  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")
    val sqlContext = sparkSession.sqlContext

    // Create Streaming context

    val ssc = new StreamingContext(sc, Seconds(20))

    val dataStream = DataStream(ssc)

    val streamingResult: StreamingResult =
      dataStream
        .read("pcatalog.kafka_flights_log")

    streamingResult.clearCheckPoint("Clearing Checkpoint.")

    streamingResult.dStream.foreachRDD { rdd =>
      val count = rdd.count()

      if (count > 0) {
        /**
          * Mandatory | Get Offset for Current Window, so we can checkpoint at the end of this window's operation
          */
        streamingResult.getCurrentCheckPoint(rdd)

        logger.info(s"Count for current Checkpoint: $count")
        logger.info(s"Scala Version Used ---> ${scala.util.Properties.versionString}")

        val rddAvro: RDD[GenericRecord] = streamingResult.convertBytesToAvro(rdd)
        rddAvro.map(_.toString)

        val df: DataFrame = streamingResult.convertAvroToDF(sqlContext, rddAvro)

        // Call Druid Connector for realtime ingestion.

        val dataSet = new DataSet(sparkSession)
        val dataSetProps = Map("load_type" -> "realtime")
        dataSet.write("gimel.druid_flights_log", df, dataSetProps)

        streamingResult.saveCurrentCheckPoint()
      }
    }

    dataStream.streamingContext

    // Start the computation
    ssc.start()

    // Wait for the computation to terminate
    ssc.awaitTermination()
  }

}
