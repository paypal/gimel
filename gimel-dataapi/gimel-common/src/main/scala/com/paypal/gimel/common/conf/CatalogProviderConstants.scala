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

// Catalog Provider
object CatalogProviderConstants {

  val HIVE_PROVIDER: String = "HIVE"
  val PCATALOG_PROVIDER: String = "PCATALOG"
  val USER_PROVIDER: String = "USER"
  val UDC_PROVIDER: String = "UDC"
  val PRIMARY_CATALOG_PROVIDER: String = UDC_PROVIDER
  val SECONDARY_CATALOG_PROVIDER: String = HIVE_PROVIDER
  val PROPS_LOCATION: String = "location"
  val PROPS_NAMESPACE: String = "nameSpace"
  val DATASET_PROPS_DATASET: String = "datasetName"
  val DYNAMIC_DATASET: String = "dynamicDataSet"
}
