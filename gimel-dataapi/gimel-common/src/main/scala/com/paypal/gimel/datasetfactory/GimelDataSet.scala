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

package com.paypal.gimel.datasetfactory

import scala.reflect.runtime.universe._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Interface for the DataSet
  */
abstract class GimelDataSet(sparkSession: SparkSession) {

  /**
    * This will hold all the Catalog Properties of a given dataset
    * Catalog properties are such as -->
    * dataSetName : flights_kafka
    * source type : KAFKA
    * kafka broker : kafka_host:9092
    * kafka topic : flights.flights_log
    * schema string : {"fields" ....}
    * serializer : com.paypal.pcatalog.serde.customerKafka
    */

  /**
    * Function reads the dataset and provides a DataFrame to Client
    *
    * @param dataset Name of the PCatalog Data Set
    * @param dataSetProps
    *                props is the way to set various additional parameters for read and write operations in DataSet class
    *                Example Usecase : to read kafka from-to a certain offset range : One can set something like below -
    *                val props = Map("fromOffset" -> 10, "toOffset" -> 20)
    *                val data = Dataset(sc).read("flights_log", props)
    * @return DataFrame
    */
  def read(dataset: String, dataSetProps: Map[String, Any] = Map.empty): DataFrame

  /**
    * Function writes a given dataframe to the actual Target System (Example Hive : DB.Table | HBASE namespace.Table)
    *
    * @param dataset   Name of the PCatalog Data Set
    * @param dataFrame The Dataframe to write into Target
    * @param dataSetProps
    *                  props is the way to set various additional parameters for read and write operations in DataSet class
    *                  Example Usecase : to write kafka with a specific parallelism : One can set something like below -
    *                  val props = Map("parallelsPerPartition" -> 10)
    *                  Dataset(sc).write(clientDataFrame, props)
    * @return DataFrame
    */
  def write(dataset: String, dataFrame: DataFrame, dataSetProps: Map[String, Any] = Map.empty): DataFrame

  /**
    * Function writes a given dataframe to the actual Target System (Example Hive : DB.Table | HBASE namespace.Table)
    *
    * @param dataset Name of the UDC Data Set
    * @param anyRDD  The RDD[T] to write into Target
    *                Note the RDD has to be typeCast to supported types by the inheriting DataSet Operators
    *                instance#1 : ElasticSearchDataSet may support just RDD[Seq(Map[String, String])], so Elastic Search must implement supported Type checking
    *                instance#2 : Kafka, HDFS, HBASE - Until they support an RDD operation for Any Type T : They throw Unsupporter Operation Exception & Educate Users Clearly !
    * @param dataSetProps
    *                props is the way to set various additional parameters for read and write operations in DataSet class
    *                Example Usecase : to write kafka with a specific parallelism : One can set something like below -
    *                val props = Map("parallelsPerPartition" -> 10)
    *                Dataset(sc).write(clientDataFrame, props)
    * @return RDD[T]
    */
  def write[T: TypeTag](dataset: String, anyRDD: RDD[T], dataSetProps: Map[String, Any]): RDD[T]

  /**
    * Function create the dataset (Currently only in the respective storages and not in the pcatalog/hive meta data)
    *
    * @param dataset Name of the UDC Data Set
    * @param dataSetProps
    *                props is the way to set various additional parameters for read and write operations in DataSet class
    *                    * @return Boolean
    */
  def create(dataset: String, dataSetProps: Map[String, Any] = Map.empty): Unit

  /**
    * Function to drop the dataset (Currently only in the respective storages and not in the pcatalog/hive meta data)
    *
    * @param dataset Name of the UDC Data Set
    * @param dataSetProps
    *                props is the way to set various additional parameters for read and write operations in DataSet class
    *
    * @return Boolean
    */
  def drop(dataset: String, dataSetProps: Map[String, Any] = Map.empty): Unit

  /**
    * Function to purge the records in the dataset
    *
    * @param dataset Name of the UDC Data Set
    * @param dataSetProps
    *                props is the way to set various additional parameters for read and write operations in DataSet class
    *
    * @return Boolean
    */
  def truncate(dataset: String, dataSetProps: Map[String, Any] = Map.empty): Unit

  /**
    * Supported Types of RDD, where RDD[T] is provided by client
    * Override and add supported types in implementations
    */

  val supportedTypesOfRDD: List[String] = List[String]()

  /**
    * Save Checkpoint
    */
  def saveCheckPoint(): Unit

  /**
    * Clear CheckPoint
    *
    */

  def clearCheckPoint(): Unit
}
