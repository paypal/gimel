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

package com.paypal.gimel.datastreamfactory

import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * A Wrapper for Data Returning to Client
  *
  * @param key   The Key From Kafka Topic
  * @param value The raw message/value from Kafka Topic
  */
case class WrappedData(val key: Any, val value: Any)

/**
  * Wrapper for Streaming Result
  * Provides Capabilitie for
  * -  Get CheckPoint At the Beginning of Current Window
  * -  Save CheckPoint At the End of Current Window
  *
  * @param dStream            Kafka DStream of Tuple( Avro Generic Record ,
  *                           Its Equivalent JSON String)
  * @param convertBytesToAvro A Function that deserializes avro Bytes to Generic Record
  * @param convertAvroToDF    A Function that converts Avro Generic Record Into DataFrame
  * @param getAsDF            Provides a DataFrame ; for usecases where Kafka Topic has
  *                           Message as Human Readable Strings
  * @param saveCheckPoint     A Function that provides capability to SaveCheckPoint Internally
  * @param clearCheckPoint    Deletes the CheckPoint Path from Zookeeper
  */
case class StreamingResult(val dStream: DStream[WrappedData],
                           val convertBytesToAvro: RDD[WrappedData] => RDD[GenericRecord],
                           val convertAvroToDF: (SQLContext, RDD[GenericRecord]) => DataFrame,
                           val getAsDF: (SQLContext, RDD[WrappedData]) => DataFrame,
                           val saveCheckPoint: Array[OffsetRange] => Boolean,
                           val clearCheckPoint: String => Unit
                          ) {
  var thisCheckPoint = Array[OffsetRange]()

  /**
    * Gets the End-Offset of Current Window of RDD and persists it on to thisCheckPoint
    *
    * @param rdd RDD[WrappedData]
    * @return Unit
    */
  def getCurrentCheckPoint(rdd: RDD[WrappedData]): Array[OffsetRange] = {
    thisCheckPoint = CheckPointHolder().getCurrentCheckPoint()
    thisCheckPoint
  }

  /**
    * Saves the checkPoint in thisCheckPoint into the Location
    *
    * @return Unit
    */
  def saveCurrentCheckPoint(): Unit = {
    saveCheckPoint(thisCheckPoint)
  }
}

abstract class GimelDataStream(streamingContext: StreamingContext) {

  /**
    * Provides DStream for a given configuration
    *
    * @param dataset      Kafka Topic Name
    * @param datasetProps Map of K->V kafka Properties
    * @return StreamingResult
    */

  def read(dataset: String, datasetProps: Map[String, Any] = Map()): StreamingResult

}

/**
  * Streaming CheckPoint Holder - SingleTon
  */

object CheckPointHolder {
  @transient private var instance: CheckPointHolder = null

  def apply(): CheckPointHolder = {
    if (instance == null) {
      instance = new CheckPointHolder()
    }
    instance
  }
}


class CheckPointHolder() {

  private var currentCheckPoint: Array[OffsetRange] = Array[OffsetRange]()

  /**
    * Sets the Current CheckPoint
    *
    * @param offsetRanges Array of OffsetRanges
    */
  def setCurentCheckPoint(offsetRanges: Array[OffsetRange]): Unit = {
    currentCheckPoint = offsetRanges
  }

  /**
    * Returns the last saved CheckPoint
    *
    * @return Array of OffsetRanges
    */
  def getCurrentCheckPoint(): Array[OffsetRange] = currentCheckPoint

}
