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

package com.paypal.gimel.common.conf

// keys related to web services
// IMPORTANT: Keys changed here must be changed in udcservices.properties as well
object PCatalogUrlConfigs {
  val API_PROTOCOL: String = "rest.service.method"
  val API_HOST: String = "rest.service.host"
  val API_PORT: String = "rest.service.port"
  val CLUSTER_BY_NAME: String = "api.cluster.by.name"
  val CLUSTER_BY_ID: String = "api.cluster.by.id"
  val API_CLUSTERS: String = "api.clusters"
  val API_OBJECT_SCHEMA: String = "api.object.schema"
  val DEACTIVATE_OBJECT_SCHEMA: String = "api.object.schema.deactivate"
  val OBJECT_SCHEMA_BY_SYSTEM_ID: String = "api.object.schemas.by.storage.system.id"
  val PAGED_OBJECT_SCHEMA_BY_SYSTEM_ID: String = "api.paged.object.schemas.by.storage.system.id"
  val UNREGISTERED_OBJECT_SCHEMA_BY_SYSTEM_ID: String = "api.unregistered.schemas.by.storage.system.id"
  val OBJECT_SCHEMA_BY_SYSTEM_CONTAINER_OBJECT: String = "api.schema.system.container.object"
  val REGISTER_DATASET: String = "api.dataset.register"
  val CHANGE_LOG_DATASET: String = "api.dataset.change.log"
  val DATASET_DEPLOYMENT_STATUS_FOR_SUCESS: String = "api.dataset.deployment.status"
  val DATASET_DEPLOYMENT_STATUS_FOR_FAILURE: String = "api.dataset.deployment.status.failure"
  val API_STORAGE_SYSTEMS: String = "api.storage.systems"
  val SYSTEM_BY_ID: String = "api.storage.system.by.id"
  val SYSTEM_ATTRIBUTES_BY_NAME: String = "api.storage.system.attributes.by.name"
  val TYPE_BY_ID: String = "api.storage.type.by.id"
  val CONTAINERS_BY_OBJECT_SCHEMA: String = "api.object.schema.containers"
  val ATTRIBUTE_KEYS_BY_TYPE_ID: String = "api.storage.type.by.attribute.key"
  val USER_BY_NAME: String = "api.user.by.name"
  val STORAGE_SYSTEM_CONTAINERS = "api.storage.system.containers"
  val DATASET_BY_NAME: String = "api.dataset.by.name"
  val STORAGE_TYPES = "api.storage.types"
  val ZONE_BY_NAME = "api.zone.name"
  val POST_STORAGE_SYSTEM = "api.post.storage.system"

  val REST_DRUID_PROTOCOL: String = "rest.druid.service.method"
  val REST_DRUID_HOST: String = "rest.druid.service.host"
  val REST_DRUID_PORT: String = "rest.druid.service.port"
  val REST_DRUID_DATASOURCES: String = "api.druid.datasources"
  val REST_DRUID_FULL: String = "api.druid.full"
}
