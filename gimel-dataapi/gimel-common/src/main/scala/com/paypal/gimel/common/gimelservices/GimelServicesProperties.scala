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

import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.paypal.gimel.common.conf.PCatalogUrlConfigs
import com.paypal.gimel.logger.Logger

class GimelServicesProperties(userProps: Map[String, String] = Map[String, String]()) {

  // Get Logger
  val logger = Logger()
  // Get Properties
  val props: mutable.Map[String, String] = getProps()
  // Rest Services Method, Host & Port
  val restMethod: String = userProps.getOrElse(PCatalogUrlConfigs.API_PROTOCOL, props(PCatalogUrlConfigs.API_PROTOCOL))
  val restHost: String = userProps.getOrElse(PCatalogUrlConfigs.API_HOST, props(PCatalogUrlConfigs.API_HOST))
  val restPort: String = userProps.getOrElse(PCatalogUrlConfigs.API_PORT, props(PCatalogUrlConfigs.API_PORT))
  // Rest APIs
  val apiClusterByName: String = userProps.getOrElse(PCatalogUrlConfigs.CLUSTER_BY_NAME, props(PCatalogUrlConfigs.CLUSTER_BY_NAME))
  val apiClusterById: String = userProps.getOrElse(PCatalogUrlConfigs.CLUSTER_BY_ID, props(PCatalogUrlConfigs.CLUSTER_BY_ID))
  val apiClusters: String = userProps.getOrElse(PCatalogUrlConfigs.API_CLUSTERS, props(PCatalogUrlConfigs.API_CLUSTERS))
  val apiObjectSchema: String = userProps.getOrElse(PCatalogUrlConfigs.API_OBJECT_SCHEMA, props(PCatalogUrlConfigs.API_OBJECT_SCHEMA))
  val apiDeactivateObjectSchema: String = userProps.getOrElse(PCatalogUrlConfigs.DEACTIVATE_OBJECT_SCHEMA, props(PCatalogUrlConfigs.DEACTIVATE_OBJECT_SCHEMA))
  val apiObjectSchemaByStorageSystemId: String = userProps.getOrElse(PCatalogUrlConfigs.OBJECT_SCHEMA_BY_SYSTEM_ID, props(PCatalogUrlConfigs.OBJECT_SCHEMA_BY_SYSTEM_ID))
  val apiPagedObjectSchemaByStorageSystemId: String = userProps.getOrElse(PCatalogUrlConfigs.PAGED_OBJECT_SCHEMA_BY_SYSTEM_ID, props(PCatalogUrlConfigs.PAGED_OBJECT_SCHEMA_BY_SYSTEM_ID))
  val apiUnregisteredObjectSchemaByStorageSystemId: String = userProps.getOrElse(PCatalogUrlConfigs.UNREGISTERED_OBJECT_SCHEMA_BY_SYSTEM_ID, props(PCatalogUrlConfigs.UNREGISTERED_OBJECT_SCHEMA_BY_SYSTEM_ID))
  val apiObjectSchemaByStorageSystemIdContainerObject: String = userProps.getOrElse(PCatalogUrlConfigs.OBJECT_SCHEMA_BY_SYSTEM_CONTAINER_OBJECT, props(PCatalogUrlConfigs.OBJECT_SCHEMA_BY_SYSTEM_CONTAINER_OBJECT))
  val apiDataSetRegister: String = userProps.getOrElse(PCatalogUrlConfigs.REGISTER_DATASET, props(PCatalogUrlConfigs.REGISTER_DATASET))
  val apiDataSetChangeLog: String = userProps.getOrElse(PCatalogUrlConfigs.CHANGE_LOG_DATASET, props(PCatalogUrlConfigs.CHANGE_LOG_DATASET))
  val apiDataSetDeploymentStatus: String = userProps.getOrElse(PCatalogUrlConfigs.DATASET_DEPLOYMENT_STATUS_FOR_SUCESS, props(PCatalogUrlConfigs.DATASET_DEPLOYMENT_STATUS_FOR_SUCESS))
  val apiDataSetFailureDeploymentStatus: String = userProps.getOrElse(PCatalogUrlConfigs.DATASET_DEPLOYMENT_STATUS_FOR_FAILURE, props(PCatalogUrlConfigs.DATASET_DEPLOYMENT_STATUS_FOR_FAILURE))
  val apiStorageSystems: String = userProps.getOrElse(PCatalogUrlConfigs.API_STORAGE_SYSTEMS, props(PCatalogUrlConfigs.API_STORAGE_SYSTEMS))
  val apiStorageSystemById: String = userProps.getOrElse(PCatalogUrlConfigs.SYSTEM_BY_ID, props(PCatalogUrlConfigs.SYSTEM_BY_ID))
  val apiStorageSystemAttributesByName: String = userProps.getOrElse(PCatalogUrlConfigs.SYSTEM_ATTRIBUTES_BY_NAME, props(PCatalogUrlConfigs.SYSTEM_ATTRIBUTES_BY_NAME))
  val apiStorageTypeById: String = userProps.getOrElse(PCatalogUrlConfigs.TYPE_BY_ID, props(PCatalogUrlConfigs.TYPE_BY_ID))
  val apiObjectSchemaContainers: String = userProps.getOrElse(PCatalogUrlConfigs.CONTAINERS_BY_OBJECT_SCHEMA, props(PCatalogUrlConfigs.CONTAINERS_BY_OBJECT_SCHEMA))
  val apiStorageTypeAttributeKeys: String = userProps.getOrElse(PCatalogUrlConfigs.ATTRIBUTE_KEYS_BY_TYPE_ID, props(PCatalogUrlConfigs.ATTRIBUTE_KEYS_BY_TYPE_ID))
  val apiUserByName: String = userProps.getOrElse(PCatalogUrlConfigs.USER_BY_NAME, props(PCatalogUrlConfigs.USER_BY_NAME))
  val apiDatasetPost: String = userProps.getOrElse(PCatalogUrlConfigs.REGISTER_DATASET, props(PCatalogUrlConfigs.REGISTER_DATASET))
  val apiStorageSystemContainers: String = userProps.getOrElse(PCatalogUrlConfigs.STORAGE_SYSTEM_CONTAINERS, props(PCatalogUrlConfigs.STORAGE_SYSTEM_CONTAINERS))
  val apiDatasetByName: String = userProps.getOrElse(PCatalogUrlConfigs.DATASET_BY_NAME, props(PCatalogUrlConfigs.DATASET_BY_NAME))

  // Rest URLs
  val baseUrl = s"$restMethod://$restHost:$restPort"
  val urlClusterByName = s"$baseUrl$apiClusterByName"
  val urlClusterById = s"$baseUrl$apiClusterById"
  val urlClusters = s"$baseUrl$apiClusters"
  val urlObjectSchema = s"$baseUrl$apiObjectSchema"
  val urlDeactivateObject = s"$baseUrl$apiDeactivateObjectSchema"
  val urlObjectSchemaByStorageSystemId = s"$baseUrl$apiObjectSchemaByStorageSystemId"
  val urlPagedObjectSchemaByStorageSystemId = s"$baseUrl$apiPagedObjectSchemaByStorageSystemId"
  val urlUnregisteredObjectSchemaByStorageSystemId = s"$baseUrl$apiUnregisteredObjectSchemaByStorageSystemId"
  val urlObjectSchemaBySystemContainerObject = s"$baseUrl$apiObjectSchemaByStorageSystemIdContainerObject"
  val urlDataSetRegister = s"$baseUrl$apiDataSetRegister"
  val urlDataSetChangeLog = s"$baseUrl$apiDataSetChangeLog"
  val urlDataSetDeploymentStatus = s"$baseUrl$apiDataSetDeploymentStatus"
  val urlDataSetFailureDeploymentStatus = s"$baseUrl$apiDataSetFailureDeploymentStatus"
  val urlStorageSystems = s"$baseUrl$apiStorageSystems"
  val urlStorageSystemById = s"$baseUrl$apiStorageSystemById"
  val urlStorageSystemAttributesByName = s"$baseUrl$apiStorageSystemAttributesByName"
  val urlStorageTypeById = s"$baseUrl$apiStorageTypeById"
  val urlObjectSchemaContainers = s"$baseUrl$apiObjectSchemaContainers"
  val urlStorageTypeAttributeKeys = s"$baseUrl$apiStorageTypeAttributeKeys"
  val urlUserByName = s"$baseUrl$apiUserByName"
  val urlDataSetPost = s"$baseUrl$apiDatasetPost"
  val urlStorageSystemContainers = s"$baseUrl$apiStorageSystemContainers"
  val urlDataSetByName = s"$baseUrl$apiDatasetByName"

  // Druid URLs
  val restDruidMethod: String = userProps.getOrElse(PCatalogUrlConfigs.REST_DRUID_PROTOCOL, props(PCatalogUrlConfigs.REST_DRUID_PROTOCOL))
  val restDruidHost: String = userProps.getOrElse(PCatalogUrlConfigs.REST_DRUID_HOST, props(PCatalogUrlConfigs.REST_DRUID_HOST))
  val restDruidPort: String = userProps.getOrElse(PCatalogUrlConfigs.REST_DRUID_PORT, props(PCatalogUrlConfigs.REST_DRUID_PORT))
  val baseDruidUrl = s"$restDruidMethod://$restDruidHost:$restDruidPort"
  val apiDruidDataSource: String = userProps.getOrElse(PCatalogUrlConfigs.REST_DRUID_DATASOURCES, props(PCatalogUrlConfigs.REST_DRUID_DATASOURCES))
  val urlDruidDataSource = s"$baseDruidUrl$apiDruidDataSource"
  val apiDruidFull: String = userProps.getOrElse(PCatalogUrlConfigs.REST_DRUID_FULL, props(PCatalogUrlConfigs.REST_DRUID_FULL))

  /**
    * Returns Properties from the resources file
    *
    * @return mutable.Map[String, String]
    */
  private def getProps(): mutable.Map[String, String] = {
    val props: Properties = new Properties()
    val configStream = this.getClass.getResourceAsStream("/pcatalogservices.properties")
    props.load(configStream)
    configStream.close
    val finalProps: mutable.Map[String, String] = mutable.Map(props.asScala.toSeq: _*)
    finalProps
  }
}


object GimelServicesProperties {

  /**
    * If nothing is supplied from User ; Load all props from file in resources folder
    *
    * @return PCatalogProperties
    */
  def apply(): GimelServicesProperties = new GimelServicesProperties()

  /**
    * Use the properties supplied by user & load the defaults from resources where-ever applicable
    *
    * @param params User Supplied properties as a KV Pair
    * @return PCatalogProperties
    */
  def apply(params: Map[String, String]): GimelServicesProperties = new GimelServicesProperties(params)

}
