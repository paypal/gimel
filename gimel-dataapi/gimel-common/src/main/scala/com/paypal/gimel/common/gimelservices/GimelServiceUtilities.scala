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

package com.paypal.gimel.common.gimelservices

import java.io.{BufferedReader, DataOutputStream, InputStream, InputStreamReader}
import java.net.URL
import java.util.stream.Collectors
import javax.net.ssl.HttpsURLConnection

import scala.collection.immutable.{Map, Seq}
import scala.io.Source.fromInputStream

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost, HttpPut}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import spray.json.{JsObject, JsValue, _}

import com.paypal.gimel.common.catalog.{DataSetProperties, Field}
import com.paypal.gimel.common.conf.CatalogProviderConstants
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.conf.PCatalogPayloadConstants
import com.paypal.gimel.common.gimelservices.payload.{StorageTypeAttributeKey, _}
import com.paypal.gimel.common.gimelservices.payload.GimelJsonProtocol._
import com.paypal.gimel.logger.Logger

object GimelServiceUtilities {

  def apply(): GimelServiceUtilities = new GimelServiceUtilities()

  def apply(params: Map[String, String]): GimelServiceUtilities = new GimelServiceUtilities(params)

}

class GimelServiceUtilities(userProps: Map[String, String] = Map[String, String]()) extends Logger {

  // Initiate Logger
  // Initiate Sevices Properties
  private val serviceProperties: GimelServicesProperties = GimelServicesProperties(userProps)
  // Import the custom implementation of JSON Protocols


  /**
    * Makes a HTTPS GET call to the URL and returns the output along with status code.
    *
    * @param url
    * @return (ResponseBody, Https Status Code)
    */
  def httpsGet(url: String): String = {
    info(s"Get Request -> $url")
    try {
      val urlObject: URL = new URL(url)
      val conn: HttpsURLConnection = urlObject.openConnection().asInstanceOf[HttpsURLConnection]
      val resStream: InputStream = conn.getInputStream()
      val response: String = fromInputStream(resStream).getLines().mkString("\n")
      response
    } catch {
      case e: Throwable =>
        error(e.getStackTraceString)
        e.printStackTrace()
        throw e
    }
  }

  /**
    * Common Function to Get Response from a URL
    *
    * @param url Service URI
    * @return Response as String
    */
  def get(url: String): String = {
    // info(s"url is --> $url")
    var response = ""
    try {
      val client = new DefaultHttpClient()
      val requesting: HttpGet = new HttpGet(url)
      val httpResponse: CloseableHttpResponse = client.execute(requesting)
      val resStream: InputStream = httpResponse.getEntity.getContent
      response = fromInputStream(resStream).getLines().mkString("\n")
      // debug(s"Response is --> $response")
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
    response
  }

  /**
    * Common Function to Get Status Code from a URL
    *
    * @param url Service URI
    * @return Response as Status Code
    */
  def getStatusCode(url: String): Int = {
    try {
      val client = new DefaultHttpClient()
      val requesting: HttpGet = new HttpGet(url)
      val httpResponse: CloseableHttpResponse = client.execute(requesting)
      val status: Int = httpResponse.getStatusLine.getStatusCode
      status
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }


  /**
    * Gets the Response as a JsObject
    *
    * @param url Service URI
    * @return Response as JsObject
    */
  def getAsObject(url: String): JsObject = {
    get(url).parseJson.convertTo[JsObject]
  }

  /**
    * Gets the Response as a Seq[JsObject]
    *
    * @param url Service URI
    * @return Response as Seq[JsObject]
    */
  def getAsObjectList(url: String): Seq[JsObject] = {
    get(url).parseJson.convertTo[Seq[JsValue]].map(_.asJsObject)
  }

  /**
    * Gets the Cluster Details for a Given Cluster Name
    *
    * @param name Name of Cluster -- Sample : test_cluster
    * @return ClusterInfo
    */
  def getClusterInfo(name: String): ClusterInfo = {
    val responseObject: JsObject = getAsObject(s"${serviceProperties.urlClusterByName}/$name")
    responseObject.convertTo[ClusterInfo]
  }

  /**
    * Gets the Cluster Details for a Given Cluster Name
    *
    * @param clusterId Id of Cluster -- Sample : 4
    * @return ClusterInfo
    */
  def getClusterInfo(clusterId: Int): ClusterInfo = {
    val responseObject: JsObject = getAsObject(s"${serviceProperties.urlClusterById}/$clusterId")
    responseObject.convertTo[ClusterInfo]
  }

  /**
    * Gets the Details of Every cluster in PCatalog Registry
    *
    * @return Seq[ClusterInfo]
    */
  def getClusters(): Seq[ClusterInfo] = {
    val responseObject: Seq[JsObject] = getAsObjectList(s"${serviceProperties.urlClusters}")
    responseObject.map(_.convertTo[ClusterInfo])
  }

  /**
    * Gets the Change Log Details for a Given Cluster Id
    *
    * @param clusterId Cluster Id
    * @return Seq[ObjectSchemaChangeLog]
    */
  def getChangeLogsByCluster(clusterId: Int): Seq[ObjectSchemaChangeLog] = {
    val responseObject = getAsObjectList(s"${serviceProperties.urlDataSetChangeLog}/$clusterId")
    responseObject.map(_.convertTo[ObjectSchemaChangeLog])
  }

  /**
    * Gets the Details of StorageSystem for a given ID
    *
    * @param storageSystemId Storage System ID
    * @return StorageSystem
    */
  def getStorageSystem(storageSystemId: Int): StorageSystem = {
    val responseObject: JsObject = getAsObject(s"${serviceProperties.urlStorageSystemById}/$storageSystemId")
    responseObject.convertTo[StorageSystem]
  }

  /**
    *
    * @param storageTypeId
    * @return StorageType
    */
  def getStorageType(storageTypeId: Int): StorageType = {
    val responseObject: JsObject = getAsObject(s"${serviceProperties.urlStorageTypeById}/$storageTypeId")
    responseObject.convertTo[StorageType]
  }

  /**
    * Gets all the Storage Systems
    *
    * @return Seq[StorageSystem]
    */
  def getStorageSystems(): Seq[StorageSystem] = {
    val responseObject: Seq[JsObject] = getAsObjectList(s"${serviceProperties.urlStorageSystems}")
    responseObject.map(_.convertTo[StorageSystem])
  }

  /**
    * Get all StorageSystemContainers
    *
    * @return Seq[StorageSystemContainer]
    */
  def getStorageSystemContainers(clusterId: Int): Seq[StorageSystemContainer] = {
    val responseObject: Seq[JsObject] = getAsObjectList(s"${serviceProperties.urlStorageSystemContainers}/${clusterId}")
    responseObject.map(_.convertTo[StorageSystemContainer])
  }

  /**
    * Gets the Storage Systems that corresponds to a given Name
    *
    * @param storageSystemName StorageSystemName
    * @return Option[StorageSystem]
    */
  def getStorageSystem(storageSystemName: String): Option[StorageSystem] = {
    getStorageSystems().find(storageSystem => storageSystem.storageSystemName == storageSystemName)
  }

  /**
    * Get Storage Type Name based on storage system name (e.g. test_cluster:Hive -> Hive)
    *
    * @param storageSystemName
    * @return String
    */
  def getStorageTypeName(storageSystemName: String): String = {
    getStorageSystem(storageSystemName).get.storageType.storageTypeName
  }

  /**
    * Gets ObjectSchemas for a Given Storage System ID
    *
    * @param storageSystemId Storage System Id
    * @return Seq[ObjectSchema]
    */
  def getObjectSchemasByStorageSystem(storageSystemId: Int): Seq[AutoRegisterObject] = {
    val responseObject: Seq[JsObject] =
      getAsObjectList(s"${serviceProperties.urlObjectSchemaByStorageSystemId}/$storageSystemId")
    val objectSchemas = responseObject.map(_.convertTo[AutoRegisterObject])
    objectSchemas
  }

  def getUserByName(userName: String): User = {
    val responseObject: JsObject = getAsObject(s"${serviceProperties.urlUserByName}/$userName")
    responseObject.convertTo[User]
    // User()
  }

  /**
    * Gets ObjectSchemas which are not registered
    *
    * @return Seq[ObjectSchema]
    */
  def getUnRegisteredObjects(): Seq[AutoRegisterObject] = {
    val responseObject: Seq[JsObject] = getAsObjectList(s"${serviceProperties.urlObjectSchemaByStorageSystemId}")
    val objectSchemas = responseObject.map(_.convertTo[AutoRegisterObject])
    objectSchemas
  }

  /**
    * Gets ObjectSchemas for a Given Storage System ID
    *
    * @param storageTypeId        Storage Type Id
    * @param isStorageSystemLevel Storage System Level flag
    * @return Seq[StorageTypeAttributeKey]
    */
  def getAttributeKeysByStorageType(storageTypeId: Int, isStorageSystemLevel: String): Seq[StorageTypeAttributeKey] = {
    val responseObject: Seq[JsObject] =
      getAsObjectList(s"${serviceProperties.urlStorageTypeAttributeKeys}/$storageTypeId/$isStorageSystemLevel")
    val storageTypeAttributes = responseObject.map(_.convertTo[StorageTypeAttributeKey])
    storageTypeAttributes
  }

  def getObjectsBySystemContainerAndObject(storageSystemId: Int, containerName: String, objectName: String): Seq[AutoRegisterObject] = {
    val responseObject: Seq[JsObject] = getAsObjectList(s"${serviceProperties.urlObjectSchemaBySystemContainerObject}/$storageSystemId/$containerName/$objectName")
    val objectSchemas = responseObject.map(_.convertTo[AutoRegisterObject])
    objectSchemas
  }

  /**
    * Gets ObjectSchemas for a Given Storage System ID
    *
    * @param storageSystemName Storage System Name
    * @return Seq[ObjectSchema]
    */
  def getObjectSchemasByStorageSystem(storageSystemName: String): Seq[AutoRegisterObject] = {
    val storageSystemId = getStorageSystem(storageSystemName).get.storageSystemId
    getObjectSchemasByStorageSystem(storageSystemId)
  }

  /**
    * Gets ContainerObjects
    *
    * @param storageSystemId Storage System Id
    * @return Seq[ContainerObject]
    */
  def getContainerObjects(storageSystemId: Int): Seq[ContainerObject] = {
    val objectSchemas: Seq[AutoRegisterObject] = getObjectSchemasByStorageSystem(storageSystemId)
    objectSchemas.map(objectSchema => ContainerObject(objectSchema.containerName, objectSchema.objectName, objectSchema.storageSystemId, objectSchema.objectSchema, objectSchema.objectAttributes, objectSchema.isActiveYN))
  }

  /**
    * Gets all the Objects in a Given Container
    *
    * @param containerName ContainerName
    * @return
    */
  def getObjectsInContainer(storageSystemId: Int, containerName: String): Seq[ContainerObject] = {
    getContainerObjects(storageSystemId).filter(containerObjects => containerObjects.containerName == containerName)
  }

  /**
    * Get Dataset Name for given storage name,container,object
    *
    * @param  storageName   String
    * @param  containerName String
    * @param  objectName    String
    *
    */
  def getDatasetNameForObject(storageName: String, containerName: String, objectName: String): String = {
    val responseObject: Seq[JsObject] =
      getAsObjectList(s"${serviceProperties.urlObjectSchema}/$storageName/$containerName/$objectName")
    if (responseObject.isEmpty) {
      "No Dataset"
    } else {
      val responseObjectUpdated = responseObject.head
      val dataset = responseObjectUpdated.getFields("storageDatabaseName", "storageDataSetName")
      val datasetName = dataset.mkString(".").filterNot(_ == '"')
      datasetName
    }
  }

  /**
    * Returns True if an object is found for given search parameters
    *
    * @param storageSystemId Storage System ID
    * @param containerName   Container Name
    * @param objectName      Object Name
    * @return True if found , false if not found
    */
  def isContainerObjectExists(storageSystemId: Int, containerName: String, objectName: String): Boolean = {
    getContainerObjects(storageSystemId).exists { containerObject =>
      (containerObject.containerName == containerName
        & containerObject.objectName == objectName
        & containerObject.storageSystemId == storageSystemId)
    }
  }

  /**
    * Returns True if an object is found for given search parameters
    *
    * @param storageSystemName Storage System Name
    * @param containerName     Container Name
    * @param objectName        Object Name
    * @return True if found , false if not found
    */
  def isContainerObjectExists(storageSystemName: String, containerName: String, objectName: String): Boolean = {
    val storageSystem: Option[StorageSystem] = getStorageSystem(storageSystemName)
    storageSystem match {
      case None =>
        false
      case Some(x) =>
        isContainerObjectExists(x.storageSystemId, containerName, objectName)
    }
  }

  /**
    * Gets the DataSet PayLoad by DataSet Name
    *
    * @param dataset Name of DataSet
    * @return JSON Payload
    */
  def getDataSetByName(dataset: String): JsObject = {
    val response: String = get(s"${serviceProperties.urlDataSetByName}/${dataset}")
    val responseJs = response.parseJson.asJsObject
    responseJs
  }

  /**
    * Get DataSet By Name via PCatalog Services
    * Return a DataSetProperties Object
    *
    * @param dataset
    * @return DataSetProperties
    */
  def getDataSetProperties(dataset: String): DataSetProperties = {

    val dataSetByNameJs = getDataSetByName(dataset)

    val systemAttributes = dataSetByNameJs
      .fields(PCatalogPayloadConstants.SYSTEM_ATTRIBUTES_KEY)
      .toString().parseJson
      .convertTo[Seq[JsValue]]
      .map(_.asJsObject)

    val storageSystemProps = systemAttributes.map { x =>
      x.fields(PCatalogPayloadConstants.SYSTEM_ATTRIBUTE_NAME).toString() ->
        x.fields(PCatalogPayloadConstants.SYSTEM_ATTRIBUTE_VALUE).toString()
    }.toMap


    val objectAttributes = dataSetByNameJs
      .fields(PCatalogPayloadConstants.OBJECT_ATTRIBUTES_KEY)
      .toString().parseJson
      .convertTo[Seq[JsValue]]
      .map(_.asJsObject)

    val dataSetProps = objectAttributes.map { x =>
      x.fields(PCatalogPayloadConstants.SYSTEM_ATTRIBUTE_NAME).toString() ->
        x.fields(PCatalogPayloadConstants.OBJECT_ATTRIBUTE_VALUE).toString()
    }.toMap


    // @TODO Not the best logic here : we are doing string replacement of culling '"'
    // @TODO The namespace must be populated according to the storage system type.

    val allProps: Map[String, String] = {
      storageSystemProps ++ dataSetProps
    }.map {
      x => x._1.replace("\"", "") -> x._2.replace("\"", "")
    } ++ Map[String, String](
      CatalogProviderConstants.PROPS_NAMESPACE -> GimelConstants.PCATALOG_STRING,
      CatalogProviderConstants.DATASET_PROPS_DATASET -> dataset
    )

    val storageSystemType = allProps.getOrElse(GimelConstants.STORAGE_TYPE, GimelConstants.NONE_STRING)

    val objectSchema = dataSetByNameJs
      .fields(PCatalogPayloadConstants.OBJECT_SCHEMA)
      .toString().parseJson
      .convertTo[Seq[JsValue]]
      .map(_.asJsObject)

    val fields = objectSchema.map { x =>
      Field(x.fields(PCatalogPayloadConstants.COLUMN_NAME).toString,
        x.fields(PCatalogPayloadConstants.COLUMN_TYPE).toString,
        x.fields.getOrElse(GimelConstants.NULL_STRING, "true").toString.toBoolean
      )
    }.toArray

    //  @TODO Fill this in once Metastore provided partitions as part of Payload
    val partitionFields = Array[Field]()

    val dataSetProperties = DataSetProperties(storageSystemType, fields, partitionFields, allProps)

    dataSetProperties
  }

  /**
    * Makes a HTTPS POST call to the URL and returns the output along with status code.
    *
    * @param url
    * @param data
    * @return (ResponseBody, Https Status Code)
    */
  def httpsPost(url: String, data: String = ""): (Int, String) = {
    info(s"Post request -> $url and data -> ${data}")
    try {
      val urlObject: URL = new URL(url)
      val conn: HttpsURLConnection = urlObject.openConnection().asInstanceOf[HttpsURLConnection]
      conn.setRequestProperty("Content-type", "application/json")
      conn.setRequestMethod("POST")
      conn.setDoOutput(true)

      val wr: DataOutputStream = new DataOutputStream(conn.getOutputStream())
      wr.writeBytes(data)
      wr.close()

      val in: BufferedReader = new BufferedReader(new InputStreamReader(conn.getInputStream()))
      val response = in.lines.collect(Collectors.toList[String]).toArray().mkString("")
      in.close()

      info(s"Post response is: $response")
      (conn.getResponseCode, response)
    } catch {
      case e: Throwable =>
        error(e.getStackTraceString)
        e.printStackTrace()
        throw e
    }

  }

  /**
    * Post Implementation
    *
    * @param url     URI
    * @param payload Payload as String (JSON Formatted)
    * @return Status Code and response in a tuple
    */
  def post(url: String, payload: String = ""): (Int, String) = {

    val client = new DefaultHttpClient()

    try {
      val post = new HttpPost(url)
      post.setHeader("Content-type", "application/json")
      post.setEntity(new StringEntity(payload))
      val httpResponse = client.execute(post)
      val resStream = httpResponse.getEntity.getContent
      val response = fromInputStream(resStream).getLines().mkString("")
      debug(s"Post Response --> $response")
      val status: Int = httpResponse.getStatusLine.getStatusCode
      if (status != GimelConstants.HTTP_SUCCESS_STATUS_CODE) {
        error(s"Unable to post to web service $url. Response code is $status")
      } else {
        info(s"Success. Response --> $status")
      }
      (status, response)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

  /**
    * Put Implementation
    *
    * @param url     URI
    * @param payload Payload as String (JSON Formatted)
    * @return Status Code and response in a tuple
    */
  def put(url: String, payload: String = ""): (Int, String) = {
    val client = new DefaultHttpClient()
    try {
      val put = new HttpPut(url)
      put.setHeader("Content-type", "application/json")
      put.setEntity(new StringEntity(payload))
      val httpResponse = client.execute(put)
      val resStream = httpResponse.getEntity.getContent
      val response = fromInputStream(resStream).getLines().mkString("")
      debug(s"put Response --> $response")
      val status: Int = httpResponse.getStatusLine.getStatusCode
      if (status != 200) {
        error(s"Unable to put to web service $url. Response code is $status")
      } else {
        info(s"Success. Response --> $status")
      }
      (status, response)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

  /**
    * Posts the Deployment Status of a Given Change Log
    *
    * @param deploymentStatus ObjectSchemaChangeDeploymentStatus
    */
  def postObjectDeploymentStatus(deploymentStatus: ObjectSchemaChangeDeploymentStatus): (Int, String) = {

    val url = s"${serviceProperties.urlDataSetDeploymentStatus}/${deploymentStatus.changeLogId}"
    post(url, deploymentStatus.toJson.compactPrint)
  }

  /**
    * Posts the FAILURE status of a Given Change Log
    *
    * @param deploymentStatus ObjectSchemaChangeDeploymentStatus
    */
  def postObjectFailureDeploymentStatus(deploymentStatus: ObjectSchemaChangeDeploymentStatus): (Int, String) = {

    val url = s"${serviceProperties.urlDataSetFailureDeploymentStatus}/${deploymentStatus.changeLogId}"
    post(url, deploymentStatus.toJson.compactPrint)
  }

  /**
    * Posts the Object Schema for an Object from Any Container on Any Cluster
    *
    * @param objectSchema ObjectSchema
    */
  def postObjectSchema(objectSchema: ObjectSchemaMapUpload): (Int, String) = {

    val url = s"${serviceProperties.urlObjectSchema}"
    post(url, objectSchema.toJson.compactPrint)
  }

  /**
    * Puts the Object Schema for an Object from Any Container on Any Cluster
    *
    * @param objectSchema ObjectSchema
    */
  def putObjectSchema(objectSchema: RawObjectSchemaMap): (Int, String) = {

    val url = s"${serviceProperties.urlObjectSchema}"
    put(url, objectSchema.toJson.compactPrint)
  }

  /**
    * Deactivate the object and the associated datasets
    *
    * @param objectId Int
    */
  def deactivateObject(objectId: Int): (Int, String) = {
    val url = s"${serviceProperties.urlDeactivateObject}/${objectId}"
    put(url)
  }

  /**
    * Posts the dataset for registration
    *
    * @param dataSet Dataset
    */
  def registerDataset(dataSet: Dataset): (Int, String) = {

    val url = s"${serviceProperties.urlDataSetPost}"
    post(url, dataSet.toJson.compactPrint)
  }

  def apiUsage: String =
    """
      |import com.paypal.pcatalog.pcatalogservices.PCatalogServiceUtilities
      |import com.paypal.pcatalog.pcatalogservices.payload._
      |
      |val params = Map("clusterName" -> "test_cluster_1,test_cluster_2")
      |lazy val serviceUtils: PCatalogServiceUtilities = PCatalogServiceUtilities()
      |lazy val clusters: Array[String] = params.getOrElse("clusterName", "test_cluster_1").split(",")
      |lazy val clustersInfo: Seq[ClusterInfo] = clusters.map(serviceUtils.getClusterInfo(_)).toSeq
      |lazy val storageSystems: Seq[StorageSystem] = serviceUtils.getStorageSystems()
      |
      |lazy val existingContainerObjects: Seq[ContainerObject] = storageSystems.map { eachStorageSystem =>
      |  serviceUtils.getContainerObjects(eachStorageSystem.storageSystemId)
      |}.reduce((l, c) => l ++ c)
      |
      |lazy val objectsInAContainer = serviceUtils.getContainerObjects(56)
      |lazy val objectSchemasInAStorageSystem = serviceUtils.getObjectSchemasByStorageSystem(56)
      |lazy val objectSchemaChangeLostsToDeployInACluster = serviceUtils.getChangeLogsByCluster(8)
      |
      |val success = "success"
      |serviceUtils.postObjectSchema(ObjectSchemaInfo("tmp1","default",1,"create table....",56,"drampally"))
    """.stripMargin

}

