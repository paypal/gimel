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
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.paypal.gimel.{DataSet}
import com.paypal.gimel.logger.Logger

object APIUsageAcrossDataSets {

  // spark-shell --master yarn-client --driver-memory 4g \
  // --executor-memory 4g --executor-cores 1 --num-executors 2 --jars ~/pcatalog.jar
  // Initiate Logger
  val logger = Logger(this.getClass.getName)
  // Specify Batch Interval for Streaming
  val batchInterval = 5

  val sparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()
  val sc = sparkSession.sparkContext
  sc.setLogLevel("ERROR")
  val sqlContext = sparkSession.sqlContext

  /**
    * --------------------- Context Initiation ---------------------
    */

  val ssc = new StreamingContext(sc, Seconds(batchInterval.toInt))

  /**
    * --------------------- DataSet Initiation ---------------------
    */

  // Initiate Pcatalog DataSet
  // val systemType = DataSetType
  val dataSet: DataSet = DataSet(sparkSession)

  /**
    * --------------------- Begin KAFKA Params ---------------------
    */
  // Create a Schema String for Avro SerDe
  val schema: String =
    """ {
      |   "type" : "record",
      |   "namespace" : "default",
      |   "name" : "flights",
      |   "fields" : [
      |      { "name" : "month" , "type" : "string" },
      |      { "name" : "dayofmonth" , "type" : "string" },
      |      { "name" : "dayofweek" , "type" : "string" },
      |      { "name" : "deptime" , "type" : "string" },
      |      { "name" : "crsdeptime" , "type" : "string" },
      |      { "name" : "arrtime" , "type" : "string" },
      |      { "name" : "crsarrtime" , "type" : "string" },
      |      { "name" : "uniquecarrier" , "type" : "string" },
      |      { "name" : "flightnum" , "type" : "string" },
      |      { "name" : "tailnum" , "type" : "string" },
      |      { "name" : "actualelapsedtime" , "type" : "string" },
      |      { "name" : "crselapsedtime" , "type" : "string" },
      |      { "name" : "airtime" , "type" : "string" },
      |      { "name" : "arrdelay" , "type" : "string" },
      |      { "name" : "depdelay" , "type" : "string" },
      |      { "name" : "origin" , "type" : "string" },
      |      { "name" : "dest" , "type" : "string" },
      |      { "name" : "distance" , "type" : "string" },
      |      { "name" : "taxiin" , "type" : "string" },
      |      { "name" : "taxiout" , "type" : "string" },
      |      { "name" : "cancelled" , "type" : "string" },
      |      { "name" : "cancellationcode" , "type" : "string" },
      |      { "name" : "diverted" , "type" : "string" },
      |      { "name" : "carrierdelay" , "type" : "string" },
      |      { "name" : "weatherdelay" , "type" : "string" },
      |      { "name" : "nasdelay" , "type" : "string" },
      |      { "name" : "securitydelay" , "type" : "string" },
      |      { "name" : "lateaircraftdelay" , "type" : "string" },
      |      { "name" : "year" , "type" : "string" }
      |   ]
      |}
    """.stripMargin
  // Create a Host:Port for Kafka, below works for Kafka installed on local machine
  val hostAndPort = "localhost:6667"
  val topic = "flights_avro_data1"
  // Create Kafka Params for Consumer
  val consumerParamsKafka: Map[String, String] = Map[String, String]("bootstrap.servers" -> hostAndPort,
    "group.id" -> 111.toString, "zookeeper.connection.timeout.ms" -> 10000.toString, "auto.offset.reset" -> "smallest",
    "avro.schema.string" -> schema)
  // Create Kafka Params for Producer
  val producerParamsKafka: Map[String, String] = Map[String, String]("bootstrap.servers" -> hostAndPort,
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
    "avro.schema.string" -> schema)
  // Produce to Kafka


  /**
    * ------------------------- ES Props ------------------------------
    */

  val esOptions: Map[String, String] = Map("pushdown" -> "true", "es.nodes" -> "localhost", "es.port" -> "9200", "es.index.auto.create" -> "true")

  /**
    * ------------------------ HDFS Props ------------------------------
    */

  val wrtoptionsParquet: Map[String, String] = Map("hiveDatabaseName" -> "default", "hdfsPath" -> "hdfs:///tmp/parquet_demo/parquet_out", "inDataFormat" -> "parquet", "compressionCodec" -> "gzip", "columnDelimiter" -> "20")

  /**
    * --------------------- Begin Demo of API Usage ---------------------
    */


  // Read Hive

  val flights_from_hive: DataFrame = dataSet.read("flights_1m")
  flights_from_hive.show()


  // Write Kafka

  dataSet.write(topic, flights_from_hive.limit(10000), producerParamsKafka)

  // Read Kafka

  val flights_from_kafka: DataFrame = dataSet.read(topic, consumerParamsKafka)
  flights_from_kafka.show()

  // write HBase

  dataSet.write("flights_hbase", flights_from_kafka)

  // Read HBase

  val flights_from_hbase: DataFrame = dataSet.read("flights_hbase")
  flights_from_hbase.show()

  // Write ES

  dataSet.write("flights/demo", flights_from_hbase, esOptions)

  // Read ES

  val flights_from_ES: DataFrame = dataSet.read("flights/demo", esOptions)
  flights_from_ES.show()

  // Write HDFS

  dataSet.write("parquet_out", flights_from_ES, wrtoptionsParquet)

  // Read HDFS via Hive

  val flights_parquet_via_hive: DataFrame = dataSet.read("flights_parquet")
  flights_parquet_via_hive.show()

  // Comparison of All Operations --> Expected 999999

  flights_parquet_via_hive.unionAll(flights_from_hive).distinct().count()

}
