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

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost, HttpPut}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.params.CoreConnectionPNames
import spray.json._

import com.paypal.gimel.common.catalog.{DataSetProperties, Field}
import com.paypal.gimel.common.conf.{CatalogProviderConstants, GimelConstants, PCatalogPayloadConstants}
import com.paypal.gimel.common.gimelservices.payload._
import com.paypal.gimel.common.gimelservices.payload.GimelJsonProtocol._
import com.paypal.gimel.common.utilities.GenericUtils
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
  // Check for gracefully exit
  private val exitCondition = userProps.getOrElse(GimelConstants.EXIT_CONDITION, GimelConstants.FALSE).toBoolean
  private var headerMap = Map.empty[String, String]

  /**
    * Get the map with header values to be passed to UDC API
    *
    * @param props : Map of dataset properties
    */
  def getUDCHeader(props: Map[String, String]): Map[String, String] = {
    val userName = System.getenv(GimelConstants.USER)
    val hostName = System.getenv(GimelConstants.HOST_NAME)
    val udcAppName = props.getOrElse(GimelConstants.UDC_APP_NAME_KEY, GimelConstants.UDC_GIMEL_DEFAULT_APP_NAME)
    val headerMap = Map(GimelConstants.APP_NAME -> serviceProperties.appName,
      GimelConstants.USER_NAME -> userName,
      GimelConstants.HOST_NAME -> hostName,
      GimelConstants.APP_ID -> serviceProperties.appId,
      GimelConstants.APP_TAG -> serviceProperties.appTag.replaceAllLiterally("/", "_"),
      GimelConstants.UDC_APP_NAME_HEADER_KEY -> udcAppName)
    val udcAuthEnabled = GenericUtils.getValueAny(props, GimelConstants.UDC_AUTH_ENABLED, "true").toBoolean
    if (udcAuthEnabled) {
      try {
        val udcAuthToken = getUDCAuthToken(props)
        logger.info("Initial Header Map excluding " + GimelConstants.UDC_AUTH_HEADER_KEY + " -> " + headerMap)
        headerMap ++ Map(GimelConstants.UDC_AUTH_HEADER_KEY -> udcAuthToken)
      } catch {
        case e: Throwable =>
          val msg = "Failed to get key from KeyMaker. Reason -> " + e
          e.printStackTrace()
          val newHeaderMap = headerMap ++ Map(GimelConstants.UDC_AUTH_HEADER_KEY -> "",
            GimelConstants.UDC_COMMENT_HEADER_KEY -> msg)
          logger.info("Failed to get UDC Auth Token. New Header Map " + " -> " + newHeaderMap)
          newHeaderMap
      }
    } else {
      headerMap
    }
  }

  /**
   * Get the UDC auth token from auth provider class
   *
   * @param props : Map of dataset properties
   */
  def getUDCAuthToken(props: Map[String, String]) : String = {
    // Loading the custom auth provider at runtime
    val authLoaderClassName = GenericUtils.getValueAny(props, GimelConstants.UDC_AUTH_PROVIDER_CLASS, "")
    if (authLoaderClassName.isEmpty) {
      throw new IllegalArgumentException(s"You need to set the property ${GimelConstants.UDC_AUTH_PROVIDER_CLASS} " +
        s"with ${GimelConstants.UDC_AUTH_ENABLED} = true, " +
        s"Please set ${GimelConstants.UDC_AUTH_ENABLED} to false in order to turn off udc auth.")
    }
    val authLoader = Class.forName(authLoaderClassName).newInstance.asInstanceOf[com.paypal.gimel.common.security.AuthProvider]
    authLoader.getCredentials(props ++ Map(GimelConstants.GIMEL_AUTH_REQUEST_TYPE -> GimelConstants.UDC_AUTH_REQUEST_TYPE))
  }

  /**
    * Override the properties from user properties
    *
    * @param props - Set of incoming properties
    */
  def customize(props: Map[String, String]): Unit = {
    serviceProperties = GimelServicesProperties(userProps ++ props)
    logger.info("Updating Header Map with new props.")
    headerMap = getUDCHeader(userProps ++ props)
  }

  /**
    * Makes a HTTPS GET call to the URL and returns the output along with status code.
    *
    * @param url
    * @return (ResponseBody, Https Status Code)
    */
  def httpsGet(url: String): String = {
    // logger.info(s"Get Request -> $url")
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
        if (exitCondition) {
          System.exit(1)
        }
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
    // logger.info(s"PUT request -> $url and data -> ${data}")
    var response = ""
    var responseCode = 0
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
      response = in.lines.collect(Collectors.toList[String]).toArray().mkString("")
      responseCode = conn.getResponseCode
      in.close()

    } catch {
      case e: Throwable =>
        logger.error(e.getStackTraceString)
        e.printStackTrace()
        if (exitCondition) {
          System.exit(1)
        }
        else {
          throw e
        }

    }
    (responseCode, response)
  }

  implicit class MutableInt(var value: Int) {
    def inc(): Unit = {
      value += 1
    }
  }


  def increment(s: MutableInt): Boolean = {
    s.inc()
    return true
  }

  /**
    * Common Function to Get Response from a URL with retry
    *
    * @param url Service URI
    * @return Response as String
    */
  def get(url: String, counter: MutableInt): String = {
    var response = ""
    try {
      val client = new DefaultHttpClient()
      val httpParams = client.getParams
      httpParams.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, GimelConstants.CONNECTION_TIMEOUT * 1000)
      httpParams.setParameter(CoreConnectionPNames.SO_TIMEOUT, GimelConstants.CONNECTION_TIMEOUT * 1000)
      val requesting: HttpGet = new HttpGet(url)
      headerMap.foreach(x => requesting.addHeader(x._1, x._2))
      val httpResponse: CloseableHttpResponse = client.execute(requesting)
      val resStream: InputStream = httpResponse.getEntity.getContent
      response = fromInputStream(resStream).getLines().mkString("\n")
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        if (exitCondition) {
          if (counter.value == 3) {
            logger.error("Max retries done ")
            System.exit(1)
          } else {
            logger.info(s"Retrying Url = ${url}")
            increment(counter)
            get(url, counter)
          }

        }
        else {
          throw ex
        }
    }
    response
  }

  /**
    * Common Function to Get Response from a URL without retry
    *
    * @param url Service URI
    * @return Response as String
    */
  def get(url: String): String = {
    var response = ""
    try {
      val client = new DefaultHttpClient()
      val httpParams = client.getParams
      httpParams.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, GimelConstants.CONNECTION_TIMEOUT * 1000)
      httpParams.setParameter(CoreConnectionPNames.SO_TIMEOUT, GimelConstants.CONNECTION_TIMEOUT * 1000)
      val requesting: HttpGet = new HttpGet(url)
      headerMap.foreach(x => requesting.addHeader(x._1, x._2))
      val httpResponse: CloseableHttpResponse = client.execute(requesting)
      val resStream: InputStream = httpResponse.getEntity.getContent
      response = fromInputStream(resStream).getLines().mkString("\n")
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        if (exitCondition) {
          System.exit(1)
        }
        else {
          throw ex
        }
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
    var status = 0
    try {
      val client = new DefaultHttpClient()
      val httpParams = client.getParams
      httpParams.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, GimelConstants.CONNECTION_TIMEOUT * 1000)
      httpParams.setParameter(CoreConnectionPNames.SO_TIMEOUT, GimelConstants.CONNECTION_TIMEOUT * 1000)
      val requesting: HttpGet = new HttpGet(url)
      headerMap.foreach(x => requesting.addHeader(x._1, x._2))
      val httpResponse: CloseableHttpResponse = client.execute(requesting)
      status = httpResponse.getStatusLine.getStatusCode
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        if (exitCondition) {
          System.exit(1)
        }
        else {
          throw ex
        }
    }
    status
  }


  /**
    * Gets the Response as a JsObject
    *
    * @param url Service URI
    * @return Response as JsObject
    */
  def getAsObject(url: String): JsObject = {
    val counter: MutableInt = 0
    get(url, counter).parseJson.convertTo[JsObject]
  }

  /**
    * Gets the Response as a Seq[JsObject]
    *
    * @param url Service URI
    * @return Response as Seq[JsObject]
    */
  def getAsObjectList(url: String): Seq[JsObject] = {
    val counter: MutableInt = 0
    val output = get(url, counter).parseJson.convertTo[Seq[JsValue]].map(_.asJsObject)
    if (output == null) {
      Seq.empty[JsObject]
    } else {
      output
    }

  }

  /**
    * Gets the Cluster Details for a Given Cluster Name
    *
    * @param name Name of Cluster -- Sample : horton
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
    * Gets the Details of Every cluster in UDC Registry
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
    *
    * @param storageTypeName
    * @return
    */
  def getStorageSystemsByStorageType(storageTypeName: String): Seq[StorageSystem] = {
    logger.info(s"Getting all storage systems of storageType : ${storageTypeName}")
    val storageSystems = getStorageSystems()
    storageSystems.filter(storageSystem => {
      storageSystem.storageType.storageTypeName.equalsIgnoreCase(storageTypeName)
    })
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
    * Get Storage Type Name based on storage system name (e.g. Horton:Hive -> Hive)
    *
    * @param storageSystemName
    * @return String
    */
  def getStorageTypeName(storageSystemName: String): String = {
    getStorageSystem(storageSystemName).get.storageType.storageTypeName
  }

  /**
    * Get all storage types available
    */
  def getStorageTypes(): Seq[StorageType] = {
    val responseObjects: Seq[JsObject] = getAsObjectList(s"${serviceProperties.urlStorageTypes}")
    val storageTypes: Seq[StorageType] = responseObjects.map(_.convertTo[StorageType])
    storageTypes
  }

  /**
    * Get the storage type object based on t
    *
    * @param storageTypeName
    * @return
    */
  def getStorageType(storageTypeName: String): Seq[StorageType] = {
    getStorageTypes().filter(storageType => storageType.storageTypeName.equalsIgnoreCase(storageTypeName))
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

  /**
    *
    * @param userName
    * @return
    */
  def getUserByName(userName: String): User = {
    val responseObject: JsObject = getAsObject(s"${serviceProperties.urlUserByName}/$userName")
    responseObject.convertTo[User]
    // User()
  }

  /**
    *
    * @param zone
    * @return
    */
  def getZoneByName(zone: String): Zone = {
    val responseObject: JsObject = getAsObject(s"${serviceProperties.urlByZoneName}/${zone}")
    responseObject.convertTo[Zone]
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
        val containerObject = ContainerObject(objectSchema.objectId, objectSchema.containerName, objectSchema.objectName, objectSchema.storageSystemId, objectSchema.objectSchema, objectSchema.objectAttributes, objectSchema.isActiveYN, objectSchema.createdUserOnStore, objectSchema.createdTimestampOnStore, objectSchema.isSelfDiscovered)
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
    val counter: MutableInt = 0
    val response: String = get(s"${serviceProperties.urlDataSetByName}/${dataset}", counter)
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
    val responseObject: Seq[JsObject] =
      getAsObjectList(s"${serviceProperties.urlStorageSystemAttributesByName}/${systemStorage}")
    responseObject
  }

  /**
    * Utility for getting the attributes map of the storage system
    *
    * @param storageSystemType -> Name of the storage system like `Teradata.CLUSTER_NAME`
    * @return -> Attributes Map
    */
  def getSystemAttributesMapByName(storageSystemType: String): Map[String, String] = {
    try {
      val systemAttributes: Seq[JsObject] = getSystemAttributesByName(storageSystemType)
      getAttributesMap(systemAttributes) +
        (PCatalogPayloadConstants.STORAGE_SYSTEM_ID -> systemAttributes.head.fields("storageSystemID").toString())
    } catch {
      case e: Throwable =>
        logger.info(s"Unable to get System attributes for storageSystemType: $storageSystemType")
        throw e
    }
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
        val containerTable = dataset.split('.').tail.mkString(".").split('.').tail.mkString(".")
        val Array(container, table) = containerTable.split('.')
        objProps += ("gimel.hive.db.name" -> container)
        objProps += ("gimel.hive.table.name" -> table)
      }
      case "TERADATA" | "MYSQL" => {
        val containerTable = dataset.split('.').tail.mkString(".").split('.').tail.mkString(".")
        objProps += ("gimel.jdbc.input.table.name" -> containerTable)
      }
      case "ELASTIC" => {
        val containerTable = dataset.split('.').tail.mkString(".").split('.').tail.mkString(".")
        val Array(container, table) = containerTable.split('.')
        objProps += ("es.resource" -> (container + "/" + table))
        objProps += ("es.index.auto.create" -> "true")
      }
      case "HBASE" => {
        val containerTable = dataset.split('.').tail.mkString(".").split('.').tail.mkString(".")
        val Array(container, table) = containerTable.split('.')
        objProps += ("gimel.hbase.namespace.name" -> container)
        objProps += ("gimel.hbase.table.name" -> table)
      }
      case _ => {
        val errorMessage =
          s"""
             |[The dataset ${dataset} does not exist. Please check if the dataset name is correct.
             |It may not exist in UDC (if you've set gimel.catalog.provider=UDC)
             |Solutions for common exceptions are documented here : http://go/gimel/exceptions"
             |""".stripMargin
        throw new Exception(errorMessage)
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
    val storageSystemTypeName: String = storageType + "." + dataset.split('.').tail.mkString(".").split('.').head
    val storageSystemProps = getSystemAttributesMapByName(storageSystemTypeName)
    val storageSystemID = storageSystemProps(PCatalogPayloadConstants.STORAGE_SYSTEM_ID).toInt
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
    * Get DataSet By Name via UDC Services
    * Return a DataSetProperties Object
    *
    * @param datasetName
    * @return DataSetProperties
    */
  def getDataSetProperties(datasetName: String): DataSetProperties = {
    getDataSetProperties(datasetName, getDataSetByName(datasetName))
  }

  def getDataSetProperties(datasetName: String, dataSetByNameJs: JsObject): DataSetProperties = {
    val systemAttributes: Seq[JsObject] = dataSetByNameJs
      .fields(PCatalogPayloadConstants.SYSTEM_ATTRIBUTES_KEY)
      .toString().parseJson
      .convertTo[Seq[JsValue]]
      .map(_.asJsObject)

    val storageSystemProps = getAttributesMap(systemAttributes)

    val objectAttributes = dataSetByNameJs
      .fields(PCatalogPayloadConstants.OBJECT_ATTRIBUTES_KEY)
      .toString().parseJson
      .convertTo[Seq[JsValue]]
      .map(_.asJsObject)

    val customAttributes = dataSetByNameJs
      .fields(PCatalogPayloadConstants.CUSTOM_ATTRIBUTES_KEY)
      .toString().parseJson
      .convertTo[Seq[JsValue]]
      .map(_.asJsObject)

    val dataSetProps = objectAttributes.map { x =>
      x.fields(PCatalogPayloadConstants.SYSTEM_ATTRIBUTE_NAME).toString() ->
        x.fields(PCatalogPayloadConstants.OBJECT_ATTRIBUTE_VALUE).toString()
    }.toMap

    val customProps = customAttributes.map { x =>
      x.fields(PCatalogPayloadConstants.OBJECT_ATTRIBUTE_KEY).toString() ->
        x.fields(PCatalogPayloadConstants.OBJECT_ATTRIBUTE_VALUE).toString()
    }.toMap


    // @TODO Not the best logic here : we are doing string replacement of culling '"'
    // @TODO The namespace must be populated according to the storage system type.

    val allProps: Map[String, String] = {
      storageSystemProps ++ dataSetProps ++ customProps
      }.map {
      x =>
        if (x._1.contains("gimel.es.schema.mapping")) {
          x._1.replace("\"", "") -> StringEscapeUtils.unescapeJava(x._2).stripPrefix("\"").stripSuffix("\"")
        } else {
          x._1.replace("\"", "") -> x._2.replace("\"", "")
        }
    } ++ Map[String, String](
      CatalogProviderConstants.PROPS_NAMESPACE -> GimelConstants.PCATALOG_STRING,
      CatalogProviderConstants.DATASET_PROPS_DATASET -> datasetName,
      CatalogProviderConstants.DYNAMIC_DATASET -> "false"
    )

    val storageSystemType = allProps.getOrElse(GimelConstants.STORAGE_TYPE, GimelConstants.NONE_STRING)

    val objectSchema = dataSetByNameJs
      .fields(PCatalogPayloadConstants.OBJECT_SCHEMA)
      .toString().parseJson
      .convertTo[Seq[JsValue]]
      .map(_.asJsObject)

    val allFields: Array[Field] = objectSchema.map { x =>
      Field(x.fields(PCatalogPayloadConstants.COLUMN_NAME).toString.replace("\"", ""),
        x.fields(PCatalogPayloadConstants.COLUMN_TYPE).toString.replace("\"", ""),
        x.fields.getOrElse(GimelConstants.NULL_STRING, "true").toString.toBoolean,
        x.fields.getOrElse(PCatalogPayloadConstants.COLUMN_PARTITION_STATUS, false).toString.toBoolean,
        x.fields.getOrElse(PCatalogPayloadConstants.COLUMN_INDEX, "0").toString.toInt)
    }.toArray

    val fields: Array[Field] = allFields.filter(field => !field.partitionStatus)
    val partitionFields = allFields.filter(field => field.partitionStatus)

    DataSetProperties(storageSystemType, fields, partitionFields, allProps)
  }

  private def getAttributesMap(systemAttributes: Seq[JsObject]): Map[String, String] = {
    systemAttributes.map { x =>
      x.fields(PCatalogPayloadConstants.SYSTEM_ATTRIBUTE_NAME).asInstanceOf[JsString].value ->
        x.fields(PCatalogPayloadConstants.SYSTEM_ATTRIBUTE_VALUE).asInstanceOf[JsString].value
    }.toMap
  }

  /**
    * Makes a HTTPS POST call to the URL and returns the output along with status code.
    *
    * @param url
    * @param data
    * @return (ResponseBody, Https Status Code)
    */
  def httpsPost(url: String, data: String = ""): (Int, String) = {
    // logger.info(s"Post request -> $url and data -> ${data}")
    var responseCode = 0
    var response = ""
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
      response = in.lines.collect(Collectors.toList[String]).toArray().mkString("")
      responseCode = conn.getResponseCode
      in.close()
    } catch {
      case e: Throwable =>
        logger.error(e.getStackTraceString)
        e.printStackTrace()
        if (exitCondition) {
          System.exit(1)
        }
        else {
          throw e
        }
    }
    (responseCode, response)
  }


  /**
    * Post Implementation
    *
    * @param url     URI to post with retry
    * @param payload Payload as String (JSON Formatted)
    * @return Status Code and response in a tuple
    */
  def postWithRetry(url: String, payload: String = "", counter: MutableInt): (Int, String) = {

    val client = new DefaultHttpClient()
    var status: Int = 0
    var response = ""
    try {
      val post = new HttpPost(url)
      post.addHeader("Content-type", "application/json")
      headerMap.foreach(x => post.addHeader(x._1, x._2))
      post.setEntity(new StringEntity(payload))
      val httpResponse = client.execute(post)
      val resStream = httpResponse.getEntity.getContent
      response = fromInputStream(resStream).getLines().mkString("")
      status = httpResponse.getStatusLine.getStatusCode
      if (status >= GimelConstants.HTTP_SUCCESS_RESPONSE_CODE) {
        logger.error(s"Unable to post to web service $url. Response code is $status")
        logger.error(s"The request to UDC is -> $payload")
        logger.error(s"The response from UDC is -> $response")
        if (exitCondition) {
          System.exit(1)
        }
      } else {
        logger.info(s"Success. Response Posting --> $status")
      }

    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        if (exitCondition) {
          if (counter.value == 3) {
            logger.error("Max retries done ")
            System.exit(1)
          } else {
            logger.info(s"Retrying Url = ${url}")
            increment(counter)
            postWithRetry(url, payload, counter)
          }
        }
        else {
          throw ex
        }
    }
    (status, response)
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
    var status: Int = 0
    var response = ""
    try {
      val post = new HttpPost(url)
      post.addHeader("Content-type", "application/json")
      headerMap.foreach(x => post.addHeader(x._1, x._2))
      post.setEntity(new StringEntity(payload))
      val httpResponse = client.execute(post)
      val resStream = httpResponse.getEntity.getContent
      response = fromInputStream(resStream).getLines().mkString("")
      status = httpResponse.getStatusLine.getStatusCode
      if (status >= GimelConstants.HTTP_SUCCESS_RESPONSE_CODE) {
        logger.error(s"Unable to post to web service $url. Response code is $status")
        logger.error(s"The request to UDC is -> $payload")
        logger.error(s"The response from UDC is -> $response")
        if (exitCondition) {
          System.exit(1)
        }
      } else {
        logger.info(s"Success. Response Posting --> $status")
      }

    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        if (exitCondition) {
          System.exit(1)
        }
        else {
          throw ex
        }
    }
    (status, response)
  }


  /**
    * Put Implementation with retry
    *
    * @param url     URI
    * @param payload Payload as String (JSON Formatted)
    * @return Status Code and response in a tuple
    */
  def putWithRetry(url: String, payload: String = "", counter: MutableInt): (Int, String) = {
    val client = new DefaultHttpClient()
    var status: Int = 0
    var response = ""
    try {
      val put = new HttpPut(url)
      put.addHeader("Content-type", "application/json")
      headerMap.foreach(x => put.addHeader(x._1, x._2))
      put.setEntity(new StringEntity(payload))
      val httpResponse = client.execute(put)
      val resStream = httpResponse.getEntity.getContent
      response = fromInputStream(resStream).getLines().mkString("")
      status = httpResponse.getStatusLine.getStatusCode
      if (status >= GimelConstants.HTTP_SUCCESS_RESPONSE_CODE) {
        logger.error(s"Unable to put to web service $url. Response code is $status")
        logger.error(s"The request to UDC is -> $payload")
        logger.error(s"The response from UDC is -> $response")
        if (exitCondition) {
          System.exit(1)
        }
      } else {
        logger.info(s"Success. Response Putting--> $status")
      }

    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        if (exitCondition) {
          if (counter.value == 3) {
            logger.error("Max retries done ")
            System.exit(1)
          } else {
            logger.info(s"Retrying Url = ${url}")
            increment(counter)
            putWithRetry(url, payload, counter)
          }
        } else {
          throw ex
        }
    }
    (status, response)
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
    var status: Int = 0
    var response = ""
    try {
      val put = new HttpPut(url)
      put.addHeader("Content-type", "application/json")
      headerMap.foreach(x => put.addHeader(x._1, x._2))
      put.setEntity(new StringEntity(payload))
      val httpResponse = client.execute(put)
      val resStream = httpResponse.getEntity.getContent
      response = fromInputStream(resStream).getLines().mkString("")
      status = httpResponse.getStatusLine.getStatusCode
      if (status >= GimelConstants.HTTP_SUCCESS_RESPONSE_CODE) {
        logger.error(s"Unable to put to web service $url. Response code is $status")
        logger.error(s"The request to UDC is -> $payload")
        logger.error(s"The response from UDC is -> $response")
        if (exitCondition) {
          System.exit(1)
        }
      } else {
        logger.info(s"Success. Response Putting--> $status")
      }

    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        if (exitCondition) {
          System.exit(1)
        } else {
          throw ex
        }
    }
    (status, response)
  }

  /**
    * Posts the Deployment Status of a Given Change Log
    *
    * @param deploymentStatus ObjectSchemaChangeDeploymentStatus
    */
  def postObjectDeploymentStatus(deploymentStatus: ObjectSchemaChangeDeploymentStatus): (Int, String) = {

    val counter: MutableInt = 0
    val url = s"${serviceProperties.urlDataSetDeploymentStatus}/${deploymentStatus.changeLogId}"
    postWithRetry(url, deploymentStatus.toJson.compactPrint, counter)
  }

  /**
    * Posts the FAILURE status of a Given Change Log
    *
    * @param deploymentStatus ObjectSchemaChangeDeploymentStatus
    */
  def postObjectFailureDeploymentStatus(deploymentStatus: ObjectSchemaChangeDeploymentStatus): (Int, String) = {

    val counter: MutableInt = 0
    val url = s"${serviceProperties.urlDataSetFailureDeploymentStatus}/${deploymentStatus.changeLogId}"
    postWithRetry(url, deploymentStatus.toJson.compactPrint, counter)
  }

  /**
    * Posts the Object Schema for an Object from Any Container on Any Cluster
    *
    * @param objectSchema ObjectSchema
    */
  def postObjectSchema(objectSchema: ObjectSchemaMapUpload): (Int, String) = {
    val counter: MutableInt = 0
    val url = s"${serviceProperties.urlObjectSchema}"
    postWithRetry(url, objectSchema.toJson.compactPrint, counter)
  }

  /**
    *
    * @param storageSystem
    */
  def postStorageSystem(storageSystem: StorageSystemCreatePostPayLoad): (Int, String) = {
    val counter: MutableInt = 0
    val url = s"${serviceProperties.urlStorageSystemPost}"
    postWithRetry(url, storageSystem.toJson.compactPrint, counter)
  }

  /**
    *
    * @param storageSystem
    * @return
    */
  def postAndGetStorageSystem(storageSystem: StorageSystemCreatePostPayLoad): StorageSystem = {
    val (status, response) = postStorageSystem(storageSystem)
    if (status <= 300) {
      logger.info(s"Created storageSystem for ${storageSystem.storageSystemName}: ${response}")
      val storageSystemRetrieved = getStorageSystem(storageSystem.storageSystemName)
      storageSystemRetrieved.get
    }
    else {
      println(s"Cannot create a storageSystem for ${storageSystem.storageSystemName}: ${response}")
      throw new Exception("Cannot create a storageSystem for ${storageSystemName}: ${response}")
    }
  }

  /**
    * Puts the Object Schema for an Object from Any Container on Any Cluster
    *
    * @param objectSchema ObjectSchema
    */
  def putObjectSchema(objectSchema: RawObjectSchemaMap): (Int, String) = {

    val counter: MutableInt = 0
    val url = s"${serviceProperties.urlObjectSchema}"
    putWithRetry(url, objectSchema.toJson.compactPrint, counter)
  }

  /**
    * Deactivate the object and the associated datasets
    *
    * @param objectId Int
    */
  def deactivateObject(objectId: Int): (Int, String) = {
    val counter: MutableInt = 0
    val url = s"${serviceProperties.urlDeactivateObject}/${objectId}"
    putWithRetry(url, "", counter)
  }

  /**
    * Posts the dataset for registration
    *
    * @param dataSet Dataset
    */
  def registerDataset(dataSet: Dataset): (Int, String) = {
    val counter: MutableInt = 0
    val url = s"${serviceProperties.urlDataSetPost}"
    postWithRetry(url, dataSet.toJson.compactPrint, counter)
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
