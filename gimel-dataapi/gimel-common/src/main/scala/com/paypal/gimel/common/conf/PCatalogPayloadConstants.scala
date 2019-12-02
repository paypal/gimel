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

// PCatalog Services - PayLoad Keys
object PCatalogPayloadConstants {
  val SYSTEM_ATTRIBUTES_KEY: String = "systemAttributes"
  val SYSTEM_ATTRIBUTE_NAME: String = "storageDsAttributeKeyName"
  val SYSTEM_ATTRIBUTE_VALUE: String = "storageSystemAttributeValue"
  val STORAGE_SYSTEM_ID: String = "storageSystemID"
  val OBJECT_ATTRIBUTES_KEY: String = "objectAttributes"
  val CUSTOM_ATTRIBUTES_KEY: String = "customAttributes"
  val OBJECT_ATTRIBUTE_KEY: String = "objectAttributeKey"
  val OBJECT_ATTRIBUTE_VALUE: String = "objectAttributeValue"
  val OBJECT_SCHEMA: String = "objectSchema"
  val COLUMN_NAME: String = "columnName"
  val COLUMN_TYPE: String = "columnType"
  val COLUMN_INDEX: String = "columnIndex"
  val COLUMN_PARTITION_STATUS: String = "partitionStatus"
  val ACTIVE_FLAG: String = "Y"
  val INACTIVE_FLAG: String = "N"
}

