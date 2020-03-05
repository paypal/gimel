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

package com.paypal.gimel.elasticsearch

import scala.reflect.runtime.universe._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sql._

import com.paypal.gimel.common.catalog.{DataSetProperties}
import com.paypal.gimel.common.conf.{CatalogProviderConstants, GimelConstants}
import com.paypal.gimel.common.gimelservices.GimelServiceUtilities
import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.elasticsearch.conf.{ElasticSearchConfigs, ElasticSearchConstants}
import com.paypal.gimel.elasticsearch.utilities._
import com.paypal.gimel.logger.Logger

/**
  * Concrete Implementation for ES Dataset
  *
  * @param sparkSession : SparkSession
  */

class DataSet(sparkSession: SparkSession) extends GimelDataSet(sparkSession: SparkSession) {

  // GET LOGGER
  val logger = Logger()
  logger.info(s"Initiated --> ${this.getClass.getName}")
  val sqlContext: SQLContext = sparkSession.sqlContext

  /** Read Implementation for Elastic Search
    *
    * @param dataset Name of the UDC Data Set
    * @param dataSetProps
    *                props is the way to set various additional parameters for read and write operations in DataSet class
    *                Example Usecase : to get 10 factor parallelism (specifically)
    *                val props = Map("coalesceFactor" -> 10)
    *                val data = Dataset(sc).read("flights", props)
    *                data.coalesce(props.get("coalesceFactor"))
    * @return DataFrame
    */
  override def read(dataset: String, dataSetProps: Map[String, Any]): DataFrame = {
    var dataFrames = Seq[DataFrame]()

    /*
     TODO -> This needs to be addressed later by refactoring it to get from a Class Configuration
    */
    val esOptions = ElasticSearchUtilities.addClusterConfig(dataSetProps, dataset, ElasticSearchConstants.esReadFlag)
    val dataSets: Seq[String] = ElasticSearchUtilities.validateAndDeduceDatasets(esOptions, ElasticSearchConstants.esReadFlag)
    esOptions.get("JSON") match {
      case Some("TRUE") =>
        logger.info(s"JSON read API...")
        dataSets.foreach(dataSet => {
          logger.info("Dataset -> " + dataSet)
          val jsonData: RDD[(String, String)] = EsSpark.esJsonRDD(sqlContext.sparkContext, dataSet, esOptions)
          val dataFrame: DataFrame = sqlContext.read.json(jsonData.map { x => x._2 })
          dataFrames :+= dataFrame
        })
      case _ =>
        logger.info(s"Read API...")
        dataSets.foreach(dataSet => {
          logger.info("Dataset -> " + dataSet)
          val dataFrame: DataFrame = sqlContext.esDF(dataSet, esOptions)
          dataFrames :+= dataFrame
        })
    }
    if (dataFrames.nonEmpty) {
      val columns: Seq[Set[String]] = dataFrames.map(dataFrame => dataFrame.columns.toSet)
      val consolidatedColumns: Set[String] = columns.tail.foldLeft(columns.head)((x, y) => x ++ y)
      dataFrames.tail.foldLeft(dataFrames.head)(
        (x, y) =>
          x.select(ElasticSearchUtilities.getUpdatedColumnList(x.columns.toSet, consolidatedColumns): _*)
            .unionAll(
              y.select(ElasticSearchUtilities.getUpdatedColumnList(y.columns.toSet, consolidatedColumns): _*)
            )
      )
      // dataFrames.reduce(_.unionAll(_))
    } else {
      val schema: StructType = StructType(Nil)
      sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], schema)
    }
  }

  /**
    * Write Implementation for Elastic Search
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataFrame The dataframe to write into Target
    * @param dataSetProps
    *                  Example Usecase : we want only 1 executor for ES Writer (specifically)
    *                  val props = Map("coalesceFactor" -> 1)
    *                  Dataset(sc).write(clientDataFrame, props)
    *                  Inside write implementation :: dataFrame.coalesce(props.get("coalesceFactor"))
    * @return DataFrame
    */
  override def write(dataset: String, dataFrame: DataFrame, dataSetProps: Map[String, Any]): DataFrame = {
    val sqlDailyPartitionWrite: Boolean = dataSetProps.getOrElse(ElasticSearchConfigs.esIsDailyIndex, false).toString.toBoolean

    val datasetProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]


    logger.info("outside " + dataSetProps.getOrElse(CatalogProviderConstants.DYNAMIC_DATASET, "outside").toString)
    logger.info("inside " + datasetProps.props.getOrElse(CatalogProviderConstants.DYNAMIC_DATASET, "inside").toString)
    logger.info("sqlDailyPartitionWrite =>" + sqlDailyPartitionWrite)
    if (sqlDailyPartitionWrite) {
      val isCataloguedTable = datasetProps.props.getOrElse(CatalogProviderConstants.DYNAMIC_DATASET, "false").toString
      if(isCataloguedTable.contains("true"))
      {
        val errorMessage =
          s"""
             |Writing to dynamic dataset with partitioned index is not currently supported
             |Solutions for common exceptions are documented here : http://go/gimel/exceptions"
             |""".stripMargin
        throw new Exception(errorMessage)

      }

      val columnRepartitionName = dataSetProps.getOrElse(ElasticSearchConfigs.esPartitionColumn, "").toString
      val esMappingId = dataSetProps.getOrElse(ElasticSearchConfigs.esMappingId, ElasticSearchConstants.esRowHashId).toString
      val hashedDF = dataFrame.withColumn(ElasticSearchConstants.esRowHashId, hash(dataFrame.columns.map(col): _*))
      val repartitionedDF: Dataset[Row] = hashedDF.repartition(col(s"$columnRepartitionName"))
      val columnDistinctValues = repartitionedDF.select(columnRepartitionName).distinct().collect.flatMap(_.toSeq)
      val columDistinctMap: Map[Any, Dataset[Row]] = columnDistinctValues.map {
        date => (date -> repartitionedDF.where(col(s"$columnRepartitionName") <=> date))
      }.toMap
      columDistinctMap.foreach { case (key, df) =>
        val options: Map[String, Any] = dataSetProps ++ Map(
          ElasticSearchConfigs.esPartition -> s"$key",
          ElasticSearchConfigs.esIsDailyIndex -> false,
          ElasticSearchConfigs.esMappingId -> s"$esMappingId"
        )
        this.write(dataset, df, options)
      }
      repartitionedDF
    } else {
      val serviceUtility: GimelServiceUtilities = GimelServiceUtilities()
      val esOptions = ElasticSearchUtilities.addClusterConfig(dataSetProps, dataset, ElasticSearchConstants.esWriteFlag)
      val dataSets: Seq[String] = ElasticSearchUtilities.validateAndDeduceDatasets(esOptions, ElasticSearchConstants.esWriteFlag)
      dataSets.foreach { dataSet => {
        val esHost: String = esOptions(GimelConstants.ES_NODE)
        val port: String = esOptions(GimelConstants.ES_PORT)
        val separator: String = ElasticSearchConstants.slashSeparator
        val esUrl: String = esHost + ElasticSearchConstants.colon + port + separator + dataSet.split(separator)(0)
        logger.info("esUrl is =>" +  esUrl )
        val statusCode: Int = serviceUtility.getStatusCode(esUrl)
        logger.info("statusCode code is " + statusCode)
        statusCode match {
          case GimelConstants.HTTP_SUCCESS_STATUS_CODE =>
            logger.info("dataSet already indexed ->" + dataSet + ". Proceed to Write.")
          case _ =>
            if (esOptions.contains(ElasticSearchConfigs.esMapping)) {
              logger.info("Mapping supplied by the User. Creating index with user specified Mapping.")
              val esMapping: String = esOptions.getOrElse(ElasticSearchConfigs.esMapping, "")
              val esPayload: String = s"""{"mappings":{"${dataSet.split(ElasticSearchConstants.slashSeparator)(1)}":{"properties":$esMapping}}}"""
              val (statusCode: Int, response: String) = serviceUtility.put(esUrl, esPayload)
              if (statusCode != GimelConstants.HTTP_SUCCESS_STATUS_CODE) {
                throw DataSetException(s"Unable to create index with status code -> ${statusCode} and with the following cause -> ${response}. Please try again")
              }
            }
        }
      }
      }
      ElasticSearchUtilities.writeToESForDF(esOptions, dataSets, dataFrame)
    }
  }

  // Add Additional Supported types to this list as and when we support other Types of RDD
  // Example to start supporting RDD[String], add to List <  typeOf[Seq[Map[String, String]]].toString)  >
  override val supportedTypesOfRDD = List(
    typeOf[scala.collection.immutable.Map[java.lang.String, java.lang.String]].toString,
    typeOf[Map[String, String]].toString,
    typeOf[Seq[String]].toString,
    typeOf[String].toString,
    typeOf[java.lang.String].toString
  )

  /**
    * Function writes a given dataframe to the actual Target System (Example Hive : DB.Table | HBASE namespace.Table)
    *
    * @param dataset Name of the UDC Data Set
    * @param rdd     The RDD[T] to write into Target
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
  def write[T: TypeTag](dataset: String, rdd: RDD[T], dataSetProps: Map[String, Any]): RDD[T] = {

    if (!supportedTypesOfRDD.contains(typeOf[T].toString)) {
      throw new Exception(
        s"""
           |Invalid RDD Type. Supported Types : ${
          supportedTypesOfRDD.mkString(" | ")
        }
           |Supplied Type is --> ${
          typeOf[T].toString
        }
           |""".stripMargin
      )
    } else {
      // todo Implementation for Write
      val serviceUtility: GimelServiceUtilities = GimelServiceUtilities()
      val esOptions = ElasticSearchUtilities.addClusterConfig(dataSetProps, dataset, ElasticSearchConstants.esWriteFlag)
      val dataSets: Seq[String] = ElasticSearchUtilities.validateAndDeduceDatasets(esOptions, ElasticSearchConstants.esWriteFlag)
      dataSets.foreach(dataSet => {
        val esHost: String = esOptions(GimelConstants.ES_NODE)
        val port: String = esOptions(GimelConstants.ES_PORT)
        val separator: String = ElasticSearchConstants.slashSeparator
        val esUrl: String = esHost + ElasticSearchConstants.colon + port + separator + dataSet.split(separator)(0)
        val statusCode: Int = serviceUtility.getStatusCode(esUrl)
        statusCode match {
          case GimelConstants.HTTP_SUCCESS_STATUS_CODE =>
            logger.info("dataSet already indexed ->" + dataSet + ". Proceed to Write.")
          case _ =>
            val esMapping: String = esOptions.getOrElse(ElasticSearchConfigs.esMapping, "")
            if (esMapping.length == 0) {
              throw DataSetException("Please supply Mapping for Index creation")
            }
            val esPayload: String = s"""{"mappings":{"${dataSet.split(ElasticSearchConstants.slashSeparator)(1)}":{"properties":${esMapping}}}}"""
            val (statusCode: Int, response: String) = serviceUtility.put(esUrl, esPayload)
            if (statusCode != GimelConstants.HTTP_SUCCESS_STATUS_CODE) {
              throw DataSetException(s"Unable to create index with status code -> ${statusCode} and with the following cause -> ${response}. Please try again")
            }
        }
      })
      ElasticSearchUtilities.writeToESForRDD(rdd, esOptions)
    }
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def create(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet create for elastic search currently not Supported")
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def drop(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet drop for elastic search currently not Supported")
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def truncate(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet truncate for elastic search currently not Supported")
  }

  /**
    * Save Checkpoint
    */
  override def clearCheckPoint(): Unit = {
    logger.info(s"Clear check Point functionality is not available for ElasticSearch Dataset")
  }

  /**
    * Clear Checkpoint
    */
  override  def saveCheckPoint(): Unit = {
    logger.info(s"Save check Point functionality is not available for ElasticSearch Dataset")
  }
}

case class DataSetException(private val message: String = "", private val cause: Throwable = null)
  extends Exception(message) {
  if (cause != null) {
    initCause(cause)
  }

  def this(message: String) = this(message, null)
}
