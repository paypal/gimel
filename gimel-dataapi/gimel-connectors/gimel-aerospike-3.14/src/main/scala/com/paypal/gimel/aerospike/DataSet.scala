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

package com.paypal.gimel.aerospike

import scala.reflect.runtime.universe._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.paypal.gimel.aerospike.conf.AerospikeClientConfiguration
import com.paypal.gimel.aerospike.reader.AerospikeReader
import com.paypal.gimel.aerospike.writer.AerospikeWriter
import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.logger.Logger

/**
  * Concrete Implementation for Aerospike Dataset
  *
  * @param sparkSession: SparkSession
  */

class DataSet(sparkSession: SparkSession) extends GimelDataSet(sparkSession: SparkSession) {

  // GET LOGGER
  val logger = Logger()
  /**
    * Change this parameter with cluster config
    */
  logger.info(s"Initiated --> ${this.getClass.getName}")
  private var conf: AerospikeClientConfiguration = _

  /**
    *
    * @param dataset Name of the UDC Data Set
    * @param dataSetProps
    *                props is the way to set various additional parameters for read and write operations in DataSet class
    *                Example Usecase : Overwrite aerospike seed host over the one in hive table properties
    *                val props = Map("gimel.aerospike.seed.hosts" -> "host_ip")
    *                val data = Dataset(sc).read("aeropsike_dataset", props)
    * @return DataFrame
    */
  override def read(dataset: String, dataSetProps: Map[String, Any]): DataFrame = {
    if (dataSetProps.isEmpty) {
      throw AerospikeDataSetException("props cannot be empty !")
    }
    conf = new AerospikeClientConfiguration(dataSetProps)
    AerospikeReader.read(sparkSession, dataset, conf)
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataFrame The Dataframe to write into Target
    * @param dataSetProps
    *                  Example Usecase : Specify column name which will be used as aerospike row key
    *                  val props = Map("gimel.aerospike.rowkey" -> id)
    *                  Dataset(sc).write("aerospike_dataset", clientDataFrame,props)
    * @return DataFrame
    */

  override def write(dataset: String, dataFrame: DataFrame, dataSetProps: Map[String, Any]): DataFrame = {
    if (dataSetProps.isEmpty) {
      throw AerospikeDataSetException("props cannot be empty !")
    }
    conf = new AerospikeClientConfiguration(dataSetProps)
    AerospikeWriter.write(dataset, dataFrame, conf)
  }

  // Add Additional Supported types to this list as and when we support other Types of RDD
  // Example to start supporting RDD[String], add to List <  typeOf[Seq[Map[String, String]]].toString)  >
  override val supportedTypesOfRDD: List[String] = List[String]()

  /**
    * Function writes a given dataframe to the actual Target System (Example Hive : DB.Table | HBASE namespace.Table)
    *
    * @param dataset Name of the UDC Data Set
    * @param rdd     The RDD[T] to write into Target
    *                Note the RDD has to be typeCast to supported types by the inheriting DataSet Operators
    *                instance#1 : ElasticSearchDataSet may support just RDD[Seq(Map[String, String])], so Elastic Search must implement supported Type checking
    *                instance#2 : Kafka, HDFS, HBASE, AEROSPIKE - Until they support an RDD operation for Any Type T : They throw Unsupporter Operation Exception & Educate Users Clearly !
    * @param dataSetProps
    *                Example Usecase : Specify column name which will be used as aerospike row key
    *                val props = Map("gimel.aerospike.rowkey" -> id)
    *                Dataset(sc).write("aerospike_dataset", clientDataFrame,props)
    * @return RDD[T]
    */
  def write[T: TypeTag](dataset: String, rdd: RDD[T], dataSetProps: Map[String, Any]): RDD[T] = {

    if (!supportedTypesOfRDD.contains(typeOf[T].toString)) {
      throw new UnsupportedOperationException(s"""Invalid RDD Type. Supported Types : ${supportedTypesOfRDD.mkString(" | ")}""")
    } else {
      // todo Implementation for Write
      rdd
    }
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def create(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet create for aerospike currently not Supported")
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def drop(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet drop for aerospike currently not Supported")
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def truncate(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet truncate for aerospike currently not Supported")
  }

  /**
    * Save Checkpoint
    */
  override def clearCheckPoint(): Unit = {
    logger.info(s"Clear check Point functionality is not available for Aerospike Dataset")
  }

  /**
    * Clear Checkpoint
    */
  override  def saveCheckPoint(): Unit = {
    logger.info(s"Save check Point functionality is not available for Aerospike Dataset")
  }

  case class AerospikeDataSetException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)

}
