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

package com.paypal.gimel.s3.conf

object S3Constants {
  val delimiter: String = "delimiter"
  val inferschema: String = "inferSchema"
  val header: String = "header"
  val userStrategy: String = "user"
  val fileStrategy: String = "file"
  val batchStrategy: String = "batch"
  val localCredentialsFile: String = "local"
  val HDFSCredentialsFile: String = "hdfs"
  val credentialLess: String = ""
  val csvFileFormat = "csv"
  val parquetFileFormat = "parquet"
  val jsonFileformat = "json"
  val textFileFormat = "text"
  val binaryFileFormat = "binary"
  val s3aImpl = "org.apache.hadoop.fs.s3a.S3AFileSystem"
  val appendSaveMode = "append"
  val overwriteSaveMode = "overwrite"
  val ignoreSaveMode = "ignore"
}
