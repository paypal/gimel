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

object S3Configs {

  val awsServicesEnableV4: String = "com.amazonaws.services.s3.enableV4"
  val accessId: String = "fs.s3a.access.key"
  val secretKey: String = "fs.s3a.secret.key"
  val pathStyleAccess: String = "fs.s3a.path.style.access"
  val endPoint: String = "fs.s3a.endpoint"
  val s3aClientImpl: String = "fs.s3a.impl"
  val sslEnabled: String = "fs.s3a.connection.ssl.enabled"
  val objectFormat: String = "gimel.s3.object.format"
  val objectPath: String = "gimel.s3.object.location"
  val delimiter: String = "gimel.s3.file.delimiter"
  val inferSchema: String = "gimel.s3.file.inferSchema"
  val header: String = "gimel.s3.file.header"
  val credentialsStrategy: String = "gimel.s3.credentials.strategy"
  val credentialsFilePath: String = "gimel.s3.credentials.file.path"
  val credentialsFileSource: String = "gimel.s3.credentials.file.source"
  val saveMode = "gimel.s3.save.mode"
}
