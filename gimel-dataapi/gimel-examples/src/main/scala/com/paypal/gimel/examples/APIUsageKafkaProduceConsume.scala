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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.paypal.gimel._
import com.paypal.gimel.logger.Logger

/**
  * Demo's Kafka Producer and Consumer for DataSet
  */
object APIUsageKafkaProduceConsume extends App {

  // Option to Run the Code in spark-submit mode,
  // if a table name is passed - it is considered. Else, default of kafka_testing_flights is read
  val datasetName = if (args.isEmpty) {
    "pcatalog.kafka_flights_log"
  } else {
    args(0)
  }
  // Initiate Logger
  val logger = Logger(this.getClass.getName)
  // Specify Batch Interval for Streaming
  val batchInterval = 5
  // Context
  val sparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()
  val sc = sparkSession.sparkContext
  sc.setLogLevel("ERROR")
  val sqlContext = sparkSession.sqlContext
  val ssc = new StreamingContext(sc, Seconds(batchInterval.toInt))

  /**
    * ---------Initiate DataSet-----------
    */
  val dataSet: DataSet = DataSet(sparkSession)


  /**
    * ------------CDH Example ----------------
    */
  val options = "throttle.batch.fetchRowsOnFirstRun=2500000:throttle.batch.batch.parallelsPerPartition=250:throttle.batch.maxRecordsPerPartition=25000000"

  /**
    * ---------Read from Kafka, using the Table Props-----------
    */
  val recsDF = dataSet.read(datasetName, options)
  recsDF.show

  /**
    * ---------Get StateFul Kafka Operator before next read or any step of operation-----------
    */
  val kafkaOperator = dataSet.latestKafkaDataSetReader.get

  /**
    * ---------to clear Checkpoint (Ideally, one would not clear checkpoint in a continuous Batch or Stream in production)-----------
    * This operation Deletes the Zookeeper Node where the checkpoint is being done
    */

  kafkaOperator.clearCheckPoint()

  /**
    * ---------- Ability to check if already checkpointed -------
    * Once checkpoint is done - we set kafkaOperator.alreadyCheckPointed = true
    * This prevents second time checkpointing (for protection)
    * Below will return "true"
    */

  // val isAlreadyCheckPointed = kafkaOperator.alreadyCheckPointed

  /**
    * Second call on CheckPoint function will not perform any save but throw a warning message to user -
    * "Warning --> Already Check-Pointed, Consume Again to Checkpoint !"
    */
  kafkaOperator.saveCheckPoint()

  /**
    * ---------Write to Kafka Some Custom Data (NOT CDH !!)-----------
    */

  // Create Dummy Data Set for Write
  def stringed(n: Int): String = {
    s"""{"age": $n, "name": "MAC-$n", "rev": ${n * 10000}}"""
  }

  val texts: Seq[String] = (1 to 20).map { x => stringed(x) }
  val rdd: RDD[String] = sc.parallelize(texts)
  val df: DataFrame = sqlContext.read.json(rdd)

  // Get a List of Supported Systems for DataSet Operations
  // val systemType = DataSetType

  // DataSet Write API Call
  dataSet.write(datasetName, df)

}
