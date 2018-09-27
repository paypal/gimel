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

package com.paypal.gimel.sftp.conf

object SFTPConfigs {

  val sftpHost: String = "gimel.sftp.host"
  val sftpUserName: String = "gimel.sftp.username"
  val password: String = "gimel.sftp.password"
  val fileType: String = "gimel.sftp.filetype"
  val sftpClass: String = "com.springml.spark.sftp"
  val filePath: String = "gimel.sftp.file.location"
  val delimiter: String = "gimel.sftp.file.delimiter"
  val inferSchema: String = "gimel.sftp.file.inferSchema"
  val passwordStrategy: String = "gimel.sftp.file.password.strategy"
  val passwordFilePath: String = "gimel.sftp.file.password.path"
  val passwordFileSource: String = "gimel.sftp.file.password.source"

}

