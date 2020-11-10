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

package com.paypal.gimel.elasticsearch.utilities

import scala.collection.Map
import scala.reflect.runtime.universe._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sql._
import spray.json.JsObject

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.gimelservices.GimelServiceUtilities
import com.paypal.gimel.elasticsearch.DataSetException
import com.paypal.gimel.elasticsearch.conf.{ElasticSearchConfigs, ElasticSearchConstants}
import com.paypal.gimel.logger.Logger

/**
  * Elastic Search Functionalities internal to PCatalog
  */
object ElasticSearchUtilities {

  val logger = Logger()
  logger.info(s"Initiated --> ${this.getClass.getName}")

  /**
    * addClusterConfig - function to pull the cluster configuration either from the hive table or from the user's options
    *
    * @param dataset  - hive table
    * @param propsMap - options coming from the user
    * @return
    */

  def addClusterConfig(propsMap: Map[String, Any], dataset: String, operationFlag: Int): Map[String, String] = {
    logger.info("Pulling hive table properties")

    val dataSetProperties = propsMap(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val tableProperties = dataSetProperties.props

    if (operationFlag == ElasticSearchConstants.esReadFlag) {
      if (propsMap.contains(ElasticSearchConfigs.esDefaultReadForAllPartitions) && propsMap(ElasticSearchConfigs.esDefaultReadForAllPartitions) == ElasticSearchConstants.defaultReadAllFlag &&
        propsMap.contains(ElasticSearchConfigs.esPartition) && propsMap(ElasticSearchConfigs.esPartition) == "*") {
        throw DataSetException("Reading all partitions is not supported. Please set the " + ElasticSearchConfigs.esDefaultReadForAllPartitions + " to true.")
      }


      if (propsMap.contains(ElasticSearchConfigs.esIsPartitioned) && (propsMap(ElasticSearchConfigs.esIsPartitioned) != null)) {
        if (propsMap(ElasticSearchConfigs.esIsPartitioned).toString != tableProperties.getOrElse(ElasticSearchConfigs.esIsPartitioned, ElasticSearchConstants.defaultPartitionsIsEnabled)) {
          throw DataSetException(ElasticSearchConfigs.esIsPartitioned + " value supplied is different when compared to the one in the table.")
        }
      }

      if (propsMap.contains(ElasticSearchConfigs.esDelimiter) && (propsMap(ElasticSearchConfigs.esDelimiter) != null)) {
        if (propsMap(ElasticSearchConfigs.esDelimiter).toString != tableProperties.getOrElse(ElasticSearchConfigs.esDelimiter, ElasticSearchConstants.defaultDelimiter)) {
          throw DataSetException(ElasticSearchConfigs.esDelimiter + " value supplied is different when compared to the one in the table.")
        }
      }
    }

    val finalProps: Map[String, String] = tableProperties ++ Map(
      GimelConstants.ES_NODE -> tableProperties.getOrElse(GimelConstants.ES_NODE, ""),
      GimelConstants.ES_PORT -> tableProperties.getOrElse(GimelConstants.ES_PORT, ""),
      ElasticSearchConfigs.esMappingNames -> tableProperties.getOrElse(ElasticSearchConfigs.esMappingNames, ""),
      ElasticSearchConfigs.esResource -> tableProperties.getOrElse(ElasticSearchConfigs.esResource, ""),
      ElasticSearchConfigs.esIsPartitioned -> tableProperties.getOrElse(ElasticSearchConfigs.esIsPartitioned, "false"),
      ElasticSearchConfigs.esDelimiter -> tableProperties.getOrElse(ElasticSearchConfigs.esDelimiter, "_")
    ) ++ propsMap.map { x => (x._1, x._2.toString) }

    logger.info(s"Resolved ES configuration --> ${finalProps.mkString(",")}")

    finalProps
  }

  /**
    * Construct the payload for posting index to Elastic Search
    *
    * @param dataFrame     The dataframe to write into Target
    * @param dataSet       the elastic search index for which
    * @param schemaPayload the payload which has all the columns in json format.
    *                      eg -> {
    *                      "age": {
    *                      "type": "long"
    *                      },
    *                      "gender": {
    *                      "type": "string"
    *                      },
    *                      "name": {
    *                      "type": "string"
    *                      }
    *                      }
    * @return String -> payload for creating ES index
    */
  def generateESPayload(dataFrame: DataFrame, dataSet: String, schemaPayload: String, esVersion: String): String = {

    if (schemaPayload == null || schemaPayload.length == 0) {
      logger.info("Mapping not supplied by the User. Proceed with default Schema." + esVersion)
      val columnsFromDF: Array[StructField] = dataFrame.schema.fields
      val ESversion2Pattern = s"2.(.*)".r
      val ESversion6Pattern = s"6.(.*)".r
      val columnsArray: String = esVersion.replaceAll(""""""", "") match {
        case ESversion2Pattern(_) =>

          columnsFromDF.foreach(column => {
            logger.info(s""""${column.name} => ${column.dataType.toString}""")})
          columnsFromDF.map(column => {
            s""""${column.name}":{"type":"${ElasticSearchConstants.dataFrameES2Mapping(column.dataType.toString)}"}"""
          }).mkString("{", ",", "}")

        case ESversion6Pattern(_) =>
          columnsFromDF.foreach(column => {
            logger.info(s""""${column.name} => ${column.dataType.toString}""")})
          columnsFromDF.map(column => {
            s""""${column.name}":{"type":"${ElasticSearchConstants.dataFrameES6Mapping(column.dataType.toString)}"}"""
          }).mkString("{", ",", "}")

        case _ => null
      }
      val esPayload: String = s"""{"mappings":{"${dataSet.split(ElasticSearchConstants.slashSeparator)(1)}":{"properties":$columnsArray}}}"""
      logger.info("Payload to ES -> " + esPayload)
      esPayload
    } else {
      logger.info("Mapping supplied by the User. Creating index with user specified Mapping.")
      val esPayload: String = s"""{"mappings":{"${dataSet.split(ElasticSearchConstants.slashSeparator)(1)}":{"properties":$schemaPayload}}}"""
      esPayload
    }
  }

  /**
    * validateAndDeduceDatasets - get all the ES Resources based on the partition values from esOptions
    *
    * @param esOptions     - Map[String, String]
    * @param operationFlag - Int -> tells you whether its a read operation or a write operation
    * @return Seq[String]
    */
  def validateAndDeduceDatasets(esOptions: Map[String, String], operationFlag: Int): Seq[String] = {
    val isPartitioned: Option[String] = esOptions.get(ElasticSearchConfigs.esIsPartitioned)
    var dataSets = Seq[String]()

    // check for partition strategy
    isPartitioned match {

      //  e.g -> "pcatalog.es.index.partitioned"->"true"
      case Some("true") =>

        // check for the partition list. If its empty throw an error
        if (esOptions.get(ElasticSearchConfigs.esPartition) == null) {
          logger.error("Incorrect Usage of API-> Either you have 'gimel.es.index.partition.isEnabled' is set to true  but 'gimel.es.index.partition.suffix' property is not set.So for Partitioned Index please set these two properties" +
            "For Partitioned Index  --> gimel.es.index.partition.isEnabled is set to true and gimel.es.index.partition.suffix is set" +
            "For Non Partitioned Index  --> gimel.es.index.partition.isEnabled is set to false")
          throw DataSetException("Incorrect Usage of API-> Either you have 'gimel.es.index.partition.isEnabled' is set to true  but 'gimel.es.index.partition.suffix' property is not set.So for Partitioned Index please set these two properties" +
            "For Partitioned Index  --> gimel.es.index.partition.isEnabled is set to true and gimel.es.index.partition.suffix is set" +
            "For Non Partitioned Index  --> gimel.es.index.partition.isEnabled is set to false")
        }

        // get the es.Resource name
        val actualResource: String = esOptions(ElasticSearchConfigs.esResource)
        val indexTypeArray: Array[String] = actualResource.split(ElasticSearchConstants.slashSeparator)
        // if the resource doesnt have a '/', throw an error
        if (indexTypeArray.length != 2) {
          logger.error("Invalid Resource Type")
          throw DataSetException("Invalid resource type.")
        }

        // construct the resource name from the partition info
        val indexName: String = indexTypeArray(0)
        val typeName: String = indexTypeArray(1)

        val esHost: String = esOptions(GimelConstants.ES_NODE)
        val port: String = esOptions(GimelConstants.ES_PORT)
        val partitions: String = esOptions.getOrElse(ElasticSearchConfigs.esPartition, getLatestIndexPartition(esHost, port, indexName, esOptions))
        logger.info("The suffix is " + partitions)

        // for regular expression based search
        if (partitions.contains('*')) {
          // for read operation, you proceed computing the dataSets.
          operationFlag match {
            case ElasticSearchConstants.esReadFlag =>
              val esHost: String = esOptions(GimelConstants.ES_NODE)
              val port: String = esOptions(GimelConstants.ES_PORT)
              getAllIndexPartitions(esHost, port, indexName, esOptions).foreach(dataSet => dataSets :+= dataSet)
              logger.info("dataSets reading => " + dataSets.toString())
            case ElasticSearchConstants.esWriteFlag =>
              logger.info("Continue Writing data")
          }
        } else {
          // for multiple partitions without wild card
          // partitions String contains ',' in it. This means user has given the exact matched string which needs to be prepended.
          val partitionsArray: Array[String] = partitions.split(",")
          // for write operation. If the partitionsArray length is > 1, throw an error
          operationFlag match {
            case ElasticSearchConstants.esWriteFlag =>
              if (partitionsArray.length > 1) {
                logger.error("Incorrect Usage of API. Cannot Write to Multiple Partitions -> gimel.es.index.partition.suffix has multiple suffix -->20180603,20180604 ")
                throw DataSetException("Incorrect usage of the API. Cannot write to multiple partitions-> gimel.es.index.partition.suffix has multiple suffix -->20180603,20180604.")
              }
            case ElasticSearchConstants.esReadFlag =>
              logger.info("Continue Reading for data")
          }

          // iteration the partitionsArray to get the actual resource names
          partitionsArray.foreach(partition => {
            val dataSet: String = indexName.concat(esOptions(ElasticSearchConfigs.esDelimiter)).concat(partition).concat(ElasticSearchConstants.slashSeparator).concat(typeName)
            dataSets :+= dataSet
          })
        }

      // e.g -> "pcatalog.es.index.partition.isEnabled"->"false"
      case Some("false") =>
        // If the partition info is supplied, throw an error.
        if (esOptions.contains(ElasticSearchConfigs.esPartition) && esOptions(ElasticSearchConfigs.esPartition) != "*") {
          logger.info("Partition suffix is given,but it is not necessary for Non Partitioned Index --> pcatalog.es.index.partition.isEnabled is set to false,but gimel.es.index.partition.suffix has been set-->Continuing the operation ignoring suffix property")
          //   throw DataSetException("Incorrect usage of the API. Partitions should not be given. The table is a non-partitioned table.")
        }
        dataSets +:= esOptions(ElasticSearchConfigs.esResource)

      // For any other flag for pcatalog.es.index.partitioned, throw an error.
      case Some(_) | None =>
        throw DataSetException("Invalid partition strategy.")
    }
    dataSets
  }

  /**
    * Gets all the partitions and sorts them and slices it into only the daily part as suffix
    * @param esHost - Elastic Host
    * @param port - Port
    * @param indexName - Index Name
    * @param esOptions - Set of Elastic search user options
    * @return - returns the suffix (day portion of the index)
    */
  def getLatestIndexPartition(esHost: String, port: String, indexName: String, esOptions: Map[String, String]): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info("@Begin --> " + MethodName)
    val indexNameWithSuffix = getAllIndexPartitions(esHost, port, indexName, esOptions).sortWith(_ > _).head
    logger.info("top index name =>" + indexNameWithSuffix)
    indexNameWithSuffix.substring(indexNameWithSuffix.lastIndexOf(esOptions(ElasticSearchConfigs.esDelimiter)) + 1, indexNameWithSuffix.size)
  }

  /**
    * Gets all the daily partitions of an Index by using the GimelServiceUtilities REST call
    * @param esHost - Elastic Host
    * @param port - Port
    * @param indexName - Index Name
    * @param esOptions - Set of Elastic search user options
    * @return - It returns all the daily partitions of the given Index.
    */
  def getAllIndexPartitions(esHost: String, port: String, indexName: String, esOptions: Map[String, String]): Seq[String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info("@Begin --> " + MethodName)
    val separator: String = ElasticSearchConstants.slashSeparator


    val esUrl: String = esHost + ElasticSearchConstants.colon + port + separator + "_mapping"
    logger.info("ES URL -> " + esUrl)

    val serviceUtility: GimelServiceUtilities = GimelServiceUtilities()
    val response: JsObject = serviceUtility.getAsObject(esUrl)
    val listAllIndexes = response.fields.map(x => x._1.toString).toSeq
    val listCurrentIndexes = listAllIndexes.filter(x => x.contains(indexName.concat(esOptions(ElasticSearchConfigs.esDelimiter))))
    logger.info(listCurrentIndexes.toString())
    listCurrentIndexes
  }

  /**
    * writeDataset - Writes the dataframe based on the options and index name
    *
    * @param esOptions - Map[String, String]
    * @param dataSets  - Seq[String] -> datasets to write -> Always of size = 1
    * @param dataFrame - DataFrame -> Dataframe to write
    * @return DataFrame
    */
  def writeToESForDF(esOptions: Map[String, String], dataSets: Seq[String], dataFrame: DataFrame): DataFrame = {
    var dataFrames = Seq[DataFrame]()
    esOptions.get("JSON") match {
      case Some("TRUE") =>
        dataSets.foreach(dataSet => {
          logger.info(s"Begin Writing to ES as JSON String....")
          val strRDD: RDD[String] = dataFrame.toJSON.rdd
          EsSpark.saveJsonToEs(strRDD, dataSet, esOptions)
          logger.info(s"Write to ES as JSON String - Success.")
          dataFrames :+= dataFrame
        })
      case _ =>
        dataSets.foreach(dataSet => {
          logger.info(s"Begin Writing DataFrame to ES.... ")
          dataFrame.saveToEs(dataSet, esOptions)
          logger.info(s"Write to ES - Success.")
          dataFrames :+= dataFrame
        })
    }
    dataFrames.reduce(_.unionAll(_))
  }

  /**
    * writeToESForRDD - Writes the RDD based on the options and index name
    *
    * @param esOptions - Map[String, String]
    * @param rdd       - RDD -> RDD to write
    * @return RDD
    */

  def writeToESForRDD[T: TypeTag](rdd: RDD[T], esOptions: Map[String, String]): RDD[T] = {
    val dataSet = esOptions("es.resource")
    esOptions.get("JSON") match {
      case Some("TRUE") =>
        logger.info(s"Begin Writing JSON to ES...")
        EsSpark.saveJsonToEs(rdd, dataSet, esOptions)
        logger.info(s"Write to ES - Success.")
      case _ =>
        logger.info(s"Begin Writing RDD to ES...")
        EsSpark.saveToEs(rdd, dataSet, esOptions)
        logger.info(s"Write to ES - Success.")
    }
    rdd
  }

  /**
    * Get elastic search version
    * Following is the output of version call from ES and we grab the version and return it
    * {"name":"pba.com","cluster_name":"hadoop","version":{"number":"2.2.0","lucene_version":"5.4.1","build_snapshot":false,"build_timestamp":
    * "2016-01-27T13:32:39Z","build_hash":"8ff36d139e16f8720f2947ef62c8167a888992fe"},"tagline":"You Know, for Search"}
    * @param host - Host of Elastic Search cluster
    * @param port - Port of Elastic Search cluster
    * @return - Version number
    */
  def getESVersion(host: String, port: String): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info("@Begin --> " + MethodName)
    val esUrl: String = host +  ElasticSearchConstants.colon + port
    logger.info("ES URL -> " + esUrl)
    val serviceUtility: GimelServiceUtilities = GimelServiceUtilities()
    val response: JsObject = serviceUtility.getAsObject(esUrl)
    response.fields("version").asJsObject.fields("number").toString
  }

  /**
    * getModifiedList - Get modified list of columns by adding Nulls
    *
    * @param currentCols - Set[String]
    * @param allCols     - Set[String]
    * @return List[Column]
    */
  def getUpdatedColumnList(currentCols: Set[String], allCols: Set[String]): List[Column] = {
    val modifiedList: List[Column] = allCols.toList.map {
      case x if currentCols.contains(x) =>
        col(x)
      case x =>
        lit(null).as(x)
    }
    modifiedList
  }
}
