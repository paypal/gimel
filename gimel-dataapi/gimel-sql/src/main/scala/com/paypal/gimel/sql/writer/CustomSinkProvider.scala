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

package com.paypal.gimel.sql.writer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import com.paypal.gimel.DataSet
import com.paypal.gimel.datastreamfactory.WrappedData
import com.paypal.gimel.kafka.conf.{KafkaClientConfiguration, KafkaConfigs}
import com.paypal.gimel.kafka.utilities.KafkaUtilities.rddToDF
import com.paypal.gimel.sql.GimelQueryProcessor
import com.paypal.gimel.sql.GimelQueryProcessor.queryUtils


class CustomSink(options: Map[String, String]) extends org.apache.spark.sql.execution.streaming.Sink {
  val datasetName = "pcatalog.flights_kafka_json"
  val sparkSession = GimelQueryProcessor.sparkSession
  val sqlContext = GimelQueryProcessor.sparkSession.sqlContext
  val conf = new KafkaClientConfiguration(queryUtils.getOptions(sparkSession)._2)
  val dataset = new DataSet(sparkSession)
  override def addBatch(batchId: Long, data: org.apache.spark.sql.DataFrame): Unit = synchronized {
    val df: DataFrame = data.sparkSession.createDataFrame(data.rdd, data.schema)
    val rDD: RDD[WrappedData] = df.rdd.map(x => WrappedData(x.get(0), x.get(1)))
    val finalDf: DataFrame = rddToDF(sqlContext, conf.kafkaMessageValueType, conf.kafkaKeySerializer
      , conf.kafkaValueSerializer, rDD, "value", conf.avroSchemaString
      , conf.avroSchemaSource, conf.cdhTopicSchemaMetadata, conf.cdhAllSchemaDetails)
    dataset.write(datasetName, finalDf)
  }
}

class CustomSinkProvider extends org.apache.spark.sql.sources.StreamSinkProvider with org.apache.spark.sql.sources.DataSourceRegister {
  def createSink(
                  sqlContext: org.apache.spark.sql.SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: org.apache.spark.sql.streaming.OutputMode): org.apache.spark.sql.execution.streaming.Sink = {
    new CustomSink(parameters)
  }
  def shortName(): String = "custom"
}
