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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Wrapper for Streaming Result
  * Provides Capabilitie for
  * -  Get CheckPoint At the Beginning of Current Window
  * -  Save CheckPoint At the End of Current Window
  *
  * @param df            A dataframe
  */
case class StructuredStreamingResult(val df: DataFrame,
                                     val saveCheckPoint: Unit,
                                     val clearCheckPoint: String => Unit
                                    ){
  var thisCheckPoint = Array[OffsetRange]()

  /**
    * Gets the End-Offset of Current Window of RDD and persists it on to thisCheckPoint
    *
    * @return Unit
    */
  def getCurrentCheckPoint(): Array[OffsetRange] = {
    thisCheckPoint = StreamCheckPointHolder().getCurrentCheckPoint()
    thisCheckPoint
  }


}

abstract class GimelStructuredDataStream(sparkSession: SparkSession) {

  /**
    * Provides DStream for a given configuration
    *
    * @param dataset      Kafka Topic Name
    * @param datasetProps Map of K->V kafka Properties
    * @return StreamingResult
    */

  def read(dataset: String, datasetProps: Map[String, Any] = Map()): StructuredStreamingResult

}

/**
  * Streaming CheckPoint Holder - SingleTon
  */

object StreamCheckPointHolder {
  @transient private var instance: StreamCheckPointHolder = null

  def apply(): StreamCheckPointHolder = {
    if (instance == null) {
      instance = new StreamCheckPointHolder()
    }
    instance
  }
}


class StreamCheckPointHolder() {

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

