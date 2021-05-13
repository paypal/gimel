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

package com.paypal.gimel.bigquery.conf

object BigQueryConfigs {
  val bigQueryTable: String = "gimel.bigquery.table"
  val bigQueryRefreshToken: String = "gimel.bigquery.refresh.token"
  val bigQueryComputeProject: String = "gimel.bigquery.compute.project"
  val bigQueryAuthProviderClass: String = "gimel.bigquery.auth.provider.class"
  val bigQueryAuthProviderToBeLoaded: String = "gimel.auth.provider.is.load"
  val bigQueryKmsClientIdName: String = "gimel.bigquery.kms.client.id.name"
  val bigQuerykmsClientSecretName: String = "gimel.bigquery.kms.secret.id.name"
  val bigQuerykmsDecrKeyName: String = "gimel.bigquery.kms.decr.key.name"
}

