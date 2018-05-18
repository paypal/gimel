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

package com.paypal.gimel.restapi.reader

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import com.paypal.gimel.common.gimelservices.GimelServiceUtilities
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.restapi.conf.RestApiClientConfiguration


/**
  * Implements RestAPI Consumer Here
  */
object RestApiConsumer {

  val logger: Logger = Logger()
  val utils: GimelServiceUtilities = GimelServiceUtilities()

  def consume(sparkSession: SparkSession, conf: RestApiClientConfiguration): DataFrame = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val responsePayload = conf.httpsFlag match {
      case false => utils.get(conf.resolvedUrl.toString)
      case true => utils.httpsGet(conf.resolvedUrl.toString)
    }
    conf.parsePayloadFlag match {
      case false =>
        logger.info("NOT Parsing payload.")
        val rdd: RDD[String] = sparkSession.sparkContext.parallelize(Seq(responsePayload))
        val rowRdd: RDD[Row] = rdd.map(Row(_))
        val field: StructType = StructType(Seq(StructField(conf.payloadFieldName, StringType)))
        sparkSession.sqlContext.createDataFrame(rowRdd, field)
      case true =>
        logger.info("Parsing payload to fields - as requested.")
        val rdd: RDD[String] = sparkSession.sparkContext.parallelize(Seq(responsePayload))
        sparkSession.sqlContext.read.json(rdd)
    }
  }

}
