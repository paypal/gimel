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
import com.paypal.gimel.common.conf
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

class GimelServiceUtilities(userProps: Map[String, String] = Map[String, String]()) {

  // Initiate Logger
  private val logger = Logger(this.getClass.getName)
  // Initiate Sevices Properties
  private var serviceProperties: GimelServicesProperties = GimelServicesProperties(userProps)
  // Import the custom implementation of JSON Protocols

  /**
    * Override the properties from user properties
    * @param props - Set of incoming properties
    */
  def customize(props: Map[String, String]): Unit = {
    serviceProperties = GimelServicesProperties(userProps ++ props)
  }

  /**
    * Makes a HTTPS GET call to the URL and returns the output along with status code.
    *
    * @param url
    * @return (ResponseBody, Https Status Code)
    */
  def httpsGet(url: String): String = {
    logger.info(s"Get Request -> $url")
    try {
      val urlObject: URL = new URL(url)
      val conn: HttpsURLConnection = urlObject.openConnection().asInstanceOf[HttpsURLConnection]
      val resStream: InputStream = conn.getInputStream()
      val response: String = fromInputStream(resStream).getLines().mkString("\n")
      response
    } catch {
      case e: Throwable =>
        logger.error(e.getStackTraceString)
        e.printStackTrace()
        throw e
    }
  }

  /**
    * Makes a HTTPS PUT call to the URL and returns the output along with status code.
    *
    * @param url
    * @param data
    * @return (ResponseBody, Https Status Code)
    */
  def httpsPut(url: String, data: String = ""): (Int, String) = {
    logger.info(s"PUT request -> $url and data -> ${data}")
    try {
      val urlObject: URL = new URL(url)
      val conn: HttpsURLConnection = urlObject.openConnection().asInstanceOf[HttpsURLConnection]
      conn.setRequestProperty("Content-type", "application/json")
      conn.setRequestMethod("PUT")
      conn.setDoOutput(true)

      val wr: DataOutputStream = new DataOutputStream(conn.getOutputStream())
      wr.writeBytes(data)
      wr.close()

      val in: BufferedReader = new BufferedReader(new InputStreamReader(conn.getInputStream()))
      val response = in.lines.collect(Collectors.toList[String]).toArray().mkString("")
      in.close()

      logger.info(s"PUT response is: $response")
      (conn.getResponseCode, response)
    } catch {
      case e: Throwable =>
        logger.error(e.getStackTraceString)
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
    // logger.info(s"url is --> $url")
    var response = ""
    try {
      val client = new DefaultHttpClient()
      val requesting: HttpGet = new HttpGet(url)
      val httpResponse: CloseableHttpResponse = client.execute(requesting)
      val resStream: InputStream = httpResponse.getEntity.getContent
      response = fromInputStream(resStream).getLines().mkString("\n")
      // logger.debug(s"Response is --> $response")
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

    val output = get(url).parseJson.convertTo[Seq[JsValue]].map(_.asJsObject)
    if (output == null) {
      Seq.empty[JsObject]
    } else {
      output
    }

  }

  /**
    * Gets the Cluster Details for a Given Cluster Name
    *
    * @param name Name of Cluster -- Sample : cluster1
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
    * Get Storage Type Name based on storage system name (e.g. cluster1:Hive -> Hive)
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
    val responseObject: Seq[JsObject] = getAsObjectList(s"${serviceProperties.urlObjectSchemaByStorageSystemId}/$storageSystemId")
    val objectSchemas = responseObject.map(_.convertTo[AutoRegisterObject])
    objectSchemas
  }


  /**
    * Gets ObjectSchemas for a Given Storage System ID
    *
    * @param storageSystemId Storage System Id
    * @return Seq[ObjectSchema]
    */
  def getPagedObjectSchemasByStorageSystem(storageSystemId: Int, page: Int, size: Int): PagedAutoRegisterObject = {
    val responseObject: JsObject = getAsObject(s"${serviceProperties.urlPagedObjectSchemaByStorageSystemId}/$storageSystemId?page=${page}&size=${size}")
    responseObject.convertTo[PagedAutoRegisterObject]
  }


  def getUserByName(userName: String): User = {
    val responseObject: JsObject = getAsObject(s"${serviceProperties.urlUserByName}/$userName")
    responseObject.convertTo[User]
    // User()
  }

  /**
    * Gets ObjectSchemas for a Given Storage System ID
    *
    * @param storageSystemId Storage System Id
    * @return Seq[ObjectSchema]
    */
  def getPagedUnRegisteredByStorageSystem(storageSystemId: Int, page: Int, size: Int): PagedAutoRegisterObject = {
    val responseObject: JsObject = getAsObject(s"${serviceProperties.urlUnregisteredObjectSchemaByStorageSystemId}/${storageSystemId}?page=${page}&size=${size}")
    responseObject.convertTo[PagedAutoRegisterObject]
  }

  /**
    * Gets Unregistered Objects for specified storageSystemId
    *
    * @return Seq[AutoRegisterObject]
    */
  def getUnRegisteredObjects(storageSystemId: Int): Seq[AutoRegisterObject] = {
    logger.debug(s"storageSystemId:${storageSystemId}")

    var autoRegisterObjects = Seq.empty[AutoRegisterObject]
    var lastPage: Boolean = false
    var page = 0
    val size = 1000
    do {
      logger.info(s"Selecting ${page} 'th page for Datastore ID = ${storageSystemId}")
      val pagedAutoRegisterObjects: PagedAutoRegisterObject = getPagedUnRegisteredByStorageSystem(storageSystemId, page, size)
      val tempAutoRegisterObjects = pagedAutoRegisterObjects.content
      tempAutoRegisterObjects.foreach(tempAutoRegisterObject => {
        autoRegisterObjects = autoRegisterObjects :+ tempAutoRegisterObject
      })
      page = page + 1
      lastPage = pagedAutoRegisterObjects.last
    }
    while (!lastPage)
    autoRegisterObjects
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

  /**
    *
    * @param storageSystemId
    * @param containerName
    * @param objectName
    * @return
    */
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
    logger.debug(s"storageSystemId:${storageSystemId}")

    var containerObjects = Seq.empty[ContainerObject]
    var lastPage: Boolean = false
    var page = 0
    val size = 1000
    do {
      logger.info(s"Selecting ${page} 'th page for Datastore ID = ${storageSystemId}")
      val objectSchemas: PagedAutoRegisterObject = getPagedObjectSchemasByStorageSystem(storageSystemId, page, size)
      val autoRegisterObjects = objectSchemas.content
      autoRegisterObjects.foreach(objectSchema => {
        val containerObject = ContainerObject(objectSchema.objectId, objectSchema.containerName, objectSchema.objectName, objectSchema.storageSystemId, objectSchema.objectSchema, objectSchema.objectAttributes, objectSchema.isActiveYN, objectSchema.createdUserOnStore, objectSchema.createdTimestampOnStore)
        containerObjects = containerObjects :+ containerObject
      })
      page = page + 1
      lastPage = objectSchemas.last
    }
    while (!lastPage)
    containerObjects
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
    * Gets the DataSet PayLoad by DataSet Name
    *
    * @param systemStorage Name of DataSet
    * @return JSON Payload
    */
  def getSystemAttributesByName(systemStorage: String): Seq[JsObject] = {
    val responseObject: Seq[JsObject] = getAsObjectList(s"${serviceProperties.urlStorageSystemAttributesByName}/${systemStorage}")
    responseObject
  }

  /**
    * Gets the Druid Object List
    *
    * @param url Service URI
    * @return JSON Payload
    */
  def getDruidDataStoreList(url: String): List[String] = {

    val responseObject = get(url)
    if (responseObject == null) {
      List.empty[String]
    } else {
      val output: String = responseObject.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\"", "")
      output.split(",").toList
    }
  }

  /**
    * Gets the details of All Druid Datastores
    *
    * @return Seq[String]
    */
  def getDruidDataStores(): Seq[String] = {

    val responseObject: Seq[String] = getDruidDataStoreList(s"${serviceProperties.urlDruidDataSource}")
    responseObject
  }

  /**
    *
    * @param url
    * @return DruidObject
    */
  def getDruidObject(url: String): DruidObject = {

    val responseObject: String = get(url)
    responseObject.parseJson.convertTo[DruidObject]
  }

  /**
    *
    * @param druidDataSource
    * @return druidObject
    */
  def getDruidObjectForDataSource(druidDataSource: String): DruidObject = {

    val responseObject: DruidObject = getDruidObject(s"${serviceProperties.urlDruidDataSource}/${druidDataSource}?${serviceProperties.apiDruidFull}")
    responseObject
  }

  /**
    * Check whether a data set exists in the catalog
    *
    * @param dataset Name of DataSet
    * @return Boolean
    */
  def checkIfDataSetExists(dataset: String): Boolean = {
    val dataSetByNameJs = getDataSetByName(dataset)
    if (dataSetByNameJs.fields(PCatalogPayloadConstants.SYSTEM_ATTRIBUTES_KEY) == JsNull) {
      false
    }
    else {
      true
    }
  }

  /**
    * creates the object properties for a given storage types [HIVE, TERADATA, ....]
    *
    * @param storageTypeName - could be HIVE, TERADATA
    * @param dataset         Name of DataSet
    * @return Boolean
    */
  def getObjectPropertiesForSystem(storageTypeName: String, dataset: String): scala.collection.mutable.Map[String, String] = {

    var objProps = scala.collection.mutable.Map[String, String]()
    storageTypeName.toUpperCase() match {
      case "HIVE" => {
        val dbTable = dataset.split('.').tail.mkString(".").split('.').tail.mkString(".")
        val Array(db, table) = dbTable.split('.')
        objProps += ("gimel.hive.db.name" -> db)
        objProps += ("gimel.hive.table.name" -> table)
      }
      case "TERADATA" => {
        val dbTable = dataset.split('.').tail.mkString(".").split('.').tail.mkString(".")
        objProps += ("gimel.jdbc.input.table.name" -> dbTable)
      }
    }
    objProps
  }

  /**
    * gets the system properties for a given storage type and storage system
    *
    * @param dataset Name of DataSet
    * @return Boolean
    */
  def getDynamicDataSetProperties(dataset: String, options: Map[String, Any]): DataSetProperties = {

    val storageType = dataset.split('.').head
    val sotrageTypeSyatem = storageType + "." + dataset.split('.').tail.mkString(".").split('.').head
    val sysAttrjs: Seq[JsObject] = getSystemAttributesByName(sotrageTypeSyatem)
    val storageSystemProps = sysAttrjs.map { x =>
      x.fields(PCatalogPayloadConstants.SYSTEM_ATTRIBUTE_NAME).toString() ->
        x.fields(PCatalogPayloadConstants.SYSTEM_ATTRIBUTE_VALUE).toString()
    }.toMap
    val storageSystemID = sysAttrjs.head.fields("storageSystemID").toString().toInt
    val storage = getStorageSystem(storageSystemID)
    val storageTypeName = storage.storageType.storageTypeName
    val objProps: scala.collection.mutable.Map[String, String] = getObjectPropertiesForSystem(storageTypeName, dataset)
    val allProps: Map[String, String] = {
      storageSystemProps ++ objProps
    }.map {
      x => x._1.replace("\"", "") -> x._2.replace("\"", "")
    } ++ Map[String, String](
      CatalogProviderConstants.PROPS_NAMESPACE -> GimelConstants.PCATALOG_STRING,
      CatalogProviderConstants.DATASET_PROPS_DATASET -> dataset,
      CatalogProviderConstants.DYNAMIC_DATASET -> "true"
    )
    val storageSystemType = allProps.getOrElse(GimelConstants.STORAGE_TYPE, GimelConstants.NONE_STRING)
    //  Since it is a dynamic data set, if the user provides PARTITIONED clause for DDL api (CREATE API) we will set up
    //  the partition field here by getting it from the options which will be passed by GimelQueryProcessor
    val partitionFields: Array[Field] = options.get(GimelConstants.HIVE_DDL_PARTITIONS_STR) match {
      case None => Array[Field]()
      case _ => options.get(GimelConstants.HIVE_DDL_PARTITIONS_STR).get.asInstanceOf[Array[Field]]
    }
    val fields = Array[Field]()
    val dataSetProperties = DataSetProperties(storageSystemType, fields, partitionFields, allProps)
    dataSetProperties
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


    val systemAttributes: Seq[JsObject] = dataSetByNameJs
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
      CatalogProviderConstants.DATASET_PROPS_DATASET -> dataset,
      CatalogProviderConstants.DYNAMIC_DATASET -> "false"
    )

    val storageSystemType = allProps.getOrElse(GimelConstants.STORAGE_TYPE, GimelConstants.NONE_STRING)

    val objectSchema = dataSetByNameJs
      .fields(PCatalogPayloadConstants.OBJECT_SCHEMA)
      .toString().parseJson
      .convertTo[Seq[JsValue]]
      .map(_.asJsObject)

    val allFields = objectSchema.map { x =>
      Field(x.fields(PCatalogPayloadConstants.COLUMN_NAME).toString.replace("\"", ""),
        x.fields(PCatalogPayloadConstants.COLUMN_TYPE).toString.replace("\"", ""),
        x.fields.getOrElse(GimelConstants.NULL_STRING, "true").toString.toBoolean,
        x.fields.getOrElse(PCatalogPayloadConstants.COLUMN_PARTITION_STATUS, false).toString.toBoolean,
        x.fields.getOrElse(PCatalogPayloadConstants.COLUMN_INDEX, "0").toString.toInt
      )
    }.toArray

    val fields = allFields.filter(field => ! field.partitionStatus)
    val partitionFields = allFields.filter(field => field.partitionStatus)

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
    logger.info(s"Post request -> $url and data -> ${data}")
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

      logger.info(s"Post response is: $response")
      (conn.getResponseCode, response)
    } catch {
      case e: Throwable =>
        logger.error(e.getStackTraceString)
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
      logger.debug(s"Post Response --> $response")
      val status: Int = httpResponse.getStatusLine.getStatusCode
      if (status != GimelConstants.HTTP_SUCCESS_STATUS_CODE) {
        logger.error(s"Unable to post to web service $url. Response code is $status")
      } else {
        logger.info(s"Success. Response Posting --> $status")
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
      logger.debug(s"put Response --> $response")
      val status: Int = httpResponse.getStatusLine.getStatusCode
      if (status != 200) {
        logger.error(s"Unable to put to web service $url. Response code is $status")
      } else {
        logger.info(s"Success. Response Putting--> $status")
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
      |val params = Map("clusterName" -> "cluster1,cluster2")
      |lazy val serviceUtils: PCatalogServiceUtilities = PCatalogServiceUtilities()
      |lazy val clusters: Array[String] = params.getOrElse("clusterName", "cluster1").split(",")
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

