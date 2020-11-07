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

package com.paypal.gimel.kafka2

import scala.language.implicitConversions

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.DataStreamWriter

import com.paypal.gimel.datastreamfactory.{GimelDataStream2, StructuredStreamingResult}
import com.paypal.gimel.kafka2.conf.KafkaClientConfiguration
import com.paypal.gimel.kafka2.reader.KafkaStreamConsumer
import com.paypal.gimel.kafka2.writer.KafkaStreamProducer
import com.paypal.gimel.logger.Logger

class DataStream(sparkSession: SparkSession) extends GimelDataStream2(sparkSession: SparkSession) {

  // GET LOGGER
  val logger = Logger()
  logger.info(s"Initiated --> ${this.getClass.getName}")

  /**
    *
    * Provides dataframe for a given configuration
    *
    * @param dataset      Kafka Dataset Name
    * @param datasetProps Map of K->V kafka Properties
    * @return Returns a DataFrame
    */
  def read(dataset: String, datasetProps: Map[String, Any]): StructuredStreamingResult = {
    if (datasetProps.isEmpty) {
      throw new DataStreamException("Props Map Cannot be empty for KafkaDataSet Read")
    }
    val conf = new KafkaClientConfiguration(datasetProps)
    KafkaStreamConsumer.createStructuredStream(sparkSession, conf, datasetProps)
  }

  /**
    *
    * Writes dataframe to target stream for a given configuration
    *
    * @param dataset      Kafka Dataset Name
    * @param dataFrame    DataFrame to write
    * @param datasetProps Map of K->V kafka Properties
    */
  def write(dataset: String, dataFrame: DataFrame, datasetProps: Map[String, Any]): DataStreamWriter[Row] = {
    if (datasetProps.isEmpty) {
      throw new DataStreamException("Props Map Cannot be empty for KafkaDataSet Write")
    }
    val conf = new KafkaClientConfiguration(datasetProps)
    KafkaStreamProducer.produceStreamToKafka(conf, dataFrame)
  }
}

/**
  * Custom Exception for KafkaDataStream initiation errors
  *
  * @param message Message to Throw
  * @param cause   A Throwable Cause
  */
private class DataStreamException(message: String, cause: Throwable)
  extends RuntimeException(message) {
  if (cause != null) {
    initCause(cause)
  }

  def this(message: String) = this(message, null)
}
