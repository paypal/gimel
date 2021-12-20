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

object BigQueryConstants {
  val bigQuery: String = "bigquery"
  val table: String = "table"
  val saveMode: String = "saveMode"
  val saveModeErrorIfExists: String = "ErrorIfExists"
  val saveModeAppend: String = "Append"
  val saveModeOverwrite: String = "Overwrite"
  val saveModeIgnore: String = "Ignore"
  val bigQueryDocUrl: String = "Please refer docs for Big Query specific params in spark [https://github.com/GoogleCloudDataproc/spark-bigquery-connector]"
  val notebooksClientId: String = "notebooks_client_id"
  val notebooksClientSecret: String = "notebooks_client_secret"
  val parentProject: String = "parentProject"
  val bigQueryTable: String = "bigQueryTable"

}
