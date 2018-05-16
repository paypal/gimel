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

package com.paypal.gimel.restapi.writer

import scala.language.implicitConversions
import scala.util._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import com.paypal.gimel.common.gimelservices.GimelServiceUtilities
import com.paypal.gimel.restapi.conf.{RestApiClientConfiguration, RestApiConstants}


/**
  * Implements Produce to Rest API Logic Here
  */
object RestApiProducer {

  val logger = com.paypal.gimel.logger.Logger()
  val utils: GimelServiceUtilities = GimelServiceUtilities()

  /**
    * Publish to Any Rest API
    *
    * @param conf RestApiClientConfiguration
    * @param data RDD
    */
  def produce(conf: RestApiClientConfiguration, data: RDD[String]): Unit = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    // Get all members of RestApiClientConfiguration before "foreach" to avoid TaskNotSerializableException on object RestApiClientConfiguration
    val url = conf.resolvedUrl.toString
    val isHttps = conf.httpsFlag
    val writeMode = conf.writeMode
    data.foreach { payload =>
      Try {
        val responseAndPayload: (Int, String) = isHttps match {
          case false =>
            writeMode match {
              case RestApiConstants.writeModePost => utils.post(url, payload)
              case _ => utils.put(url, payload)
            }
          case true =>
            writeMode match {
              case RestApiConstants.writeModePost => utils.httpsPost(url, payload)
              case _ => utils.httpsPut(url, payload)
            }
        }
        responseAndPayload
      }
      match {
        case Success(x) =>
          logger.info(s"Publish to Rest API - Completed ! \n Response --> ${x}")
        case Failure(ex) =>
          logger.info(
            s"""
               |Error while --> ${writeMode}
               |Payload --> ${payload}
               |Response Code & Response Payload --> ${ex.getStackTraceString}
              """.stripMargin)
          throw ex
      }
    }
  }

  /**
    * Publish to Any Rest API
    *
    * @param conf      RestApiClientConfiguration
    * @param dataFrame DataFrame
    */
  def produce(conf: RestApiClientConfiguration, dataFrame: DataFrame): Unit = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val dfToWrite: DataFrame = conf.usePayloadFlag match {
      case false => dataFrame
      case true => dataFrame.select(RestApiConstants.payloadString)
    }
    produce(conf, dfToWrite.toJSON.rdd)

  }
}
