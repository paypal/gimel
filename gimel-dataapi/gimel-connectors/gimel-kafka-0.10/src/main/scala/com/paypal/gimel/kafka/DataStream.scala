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

package com.paypal.gimel.kafka

import scala.language.implicitConversions

import org.apache.spark.streaming.StreamingContext

import com.paypal.gimel.datastreamfactory.{GimelDataStream, StreamingResult}
import com.paypal.gimel.kafka.conf.KafkaClientConfiguration
import com.paypal.gimel.kafka.reader.KafkaStreamConsumer

class DataStream(streamingContext: StreamingContext) extends GimelDataStream(streamingContext: StreamingContext) {

  info(s"Initiated --> ${this.getClass.getName}")

  /**
    * Provides DStream for a given configuration
    *
    * @param dataset      Kafka Topic Name
    * @param datasetProps Map of K->V kafka Properties
    * @return Tuple2 Of -
    *         Dstream[GenericRecord , Its Equivalent JSON String]
    *         A Function That Takes (SQLContext, RDD[GenericRecord]) , and returns a DataFrame
    */
  def read(dataset: String, datasetProps: Map[String, Any]): StreamingResult = {

    if (datasetProps.isEmpty) {
      throw new DataStreamException("Props Map Cannot be emtpy for KafkaDataSet Read")
    }
    val conf = new KafkaClientConfiguration(datasetProps)
    KafkaStreamConsumer.createDStream(streamingContext, conf)
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
