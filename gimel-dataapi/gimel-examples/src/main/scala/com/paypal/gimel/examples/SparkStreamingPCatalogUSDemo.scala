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

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

import com.paypal.gimel.{DataSet, DataStream}
import com.paypal.gimel.logger.Logger

object SparkStreamingPCatalogUSDemo {

  // Define Geo Function
  case class Geo(lat: Double, lon: Double)

  val myUDF: UserDefinedFunction = udf((lat: Double, lon: Double) => Geo(lat, lon))

  def main(args: Array[String]) {

    // Creating SparkContext
    val sparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")
    val sqlContext = sparkSession.sqlContext
    val ssc = new StreamingContext(sc, Seconds(10))
    val logger = Logger(this.getClass.getName)

    // Initiating PCatalog DataSet and DataStream
    val dataSet = DataSet(sparkSession)
    val dataStream = DataStream(ssc)

    // Reading from HDFS Dataset
    logger.info("Reading address_geo HDFS Dataset")
    val geoLookUpDF = dataSet.read("pcatalog.address_geo")
    val geoLookUp = geoLookUpDF.withColumn("geo", myUDF(geoLookUpDF("lat"), geoLookUpDF("lon"))).drop("lat").drop("lon")
    geoLookUp.cache()
    logger.info("Read" + geoLookUp.count() + " records")

    // Reading from Kafka DataStream and Loading into Elastic Search Dataset
    val streamingResult = dataStream.read("pcatalog.kafka_transactions")
    streamingResult.clearCheckPoint("OneTimeOnly")
    streamingResult.dStream.foreachRDD { rdd =>
      if (rdd.count() > 0) {
        streamingResult.getCurrentCheckPoint(rdd)
        val txnDF = streamingResult.convertAvroToDF(sqlContext, streamingResult.convertBytesToAvro(rdd))
        val resultSet = txnDF.join(geoLookUp, txnDF("account_number") === geoLookUp("customer_id"))
          .selectExpr("CONCAT(time_created,'000') AS time_created", "geo", "usd_amount")

        dataSet.write("pcatalog.elastic_transactions_dmz", resultSet)
        streamingResult.saveCurrentCheckPoint()
      }
    }

    // Start Streaming
    dataStream.streamingContext.start()
    dataStream.streamingContext.awaitTermination()

    sc.stop()
  }
}
