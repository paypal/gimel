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

import com.paypal.gimel.DataSet

object APIUsageElasticSearchDataSet {

  val sparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()
  val sc = sparkSession.sparkContext
  sc.setLogLevel("ERROR")
  val sqlContext = sparkSession.sqlContext

  val nodes = "elastic_host_ip"

  val dataSet = DataSet(sparkSession)

  /*  Use case for write as JSON for a given rdd  */
  // WriteJSONforRDD

  val json1 = """{"reason2" : "business2", "airport2" : "SFO2"}"""
  val json2 = """{"participants2" : 5, "airport2" : "OTP2"}"""
  var options = Map("pushdown" -> "true",
    "es.nodes" -> "localhost", "es.port" -> "9200",
    "es.index.auto.create" -> "true", "JSON" -> "TRUE")
  val airportsRDD = sc.makeRDD(Seq(json1, json2))


  /*  Use case for Read API into a DF  */
  // ReadEStoDF
  options = Map("gimel.es.index.partitioned" -> "false"
    , "gimel.es.index.partition.delimiter" -> "_"
    , "gimel.es.index.partition" -> "20170602,20170603")


  /*  Use case for write API for a given rdd  */
  // WriteESfromRdd

  val game = Map("name" -> "dheeraj3", "age" -> "28", "gender" -> "male")
  val game1 = Map("name" -> "dheeraj4", "age" -> "28", "gender" -> "male")
  val rdd = sc.makeRDD(Seq(game, game1))
  options = Map("pushdown" -> "true"
    , "es.nodes" -> nodes, "es.port" -> "9200"
    , "es.index.auto.create" -> "true"
    , "gimel.es.index.partitioned" -> "true"
    , "gimel.es.index.partition.delimiter" -> "_"
    , "gimel.es.index.partition" -> "20170603")


  /*  Use case for Read API as JSON into a DF  */
  // ReadasJSONintoDF

  options = Map("pushdown" -> "true"
    , "es.nodes" -> "localhost"
    , "es.port" -> "9200"
    , "es.index.auto.create" -> "true"
    , "JSON" -> "TRUE"
    , "gimel.es.index.partitioned" -> "true"
    , "gimel.es.index.partition.delimiter" -> "_"
    , "gimel.es.index.partition" -> "20170602")

  /*  Use case for Write API From a DF  */
  // WriteESfromDF

  options = Map("gimel.es.index.partition" -> "20170602")
  val json31 = s"""{"name" : "dheeraj11", "age" : "28","gender":"male"}"""
  val json41 = s"""{"name" : "dheeraj12", "age" : "28","gender":"male"}"""
  val rdd11 = sc.parallelize(Seq(json31, json41))
  val df12 = sqlContext.read.json(rdd11)

  /*  Use case for Write API From a DF as JSON  */
  // WriteasJSONfromDF

  options = Map("pushdown" -> "true"
    , "es.nodes" -> nodes
    , "es.port" -> "9200"
    , "es.index.auto.create" -> "true"
    , "JSON" -> "TRUE")
  val json3 = """{"name" : "dheeraj", "age" : 28,","gender":"male"}"""
  val json4 = """{"name" : "baskar", "age" : 16,","gender":"male"}"""

  val rdd12 = sc.parallelize(Seq(json3, json4))
  val df1 = sqlContext.read.json(rdd12)

}
