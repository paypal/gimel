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

package com.paypal.gimel.s3.utilities

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.security.FileHandler
import com.paypal.gimel.common.storageadmin.HDFSAdminClient

object S3Utilities {
    def getCredentialsFromHDFS(filePath: String): (String, String) = {
      FileHandler.checkIfFileAccessibleByOthers(filePath, GimelConstants.HADDOP_FILE_SYSTEM, true)
      val fileContent = HDFSAdminClient.readHDFSFile(filePath).trim
      (fileContent.split('\n')(0), fileContent.split('\n')(1))
    }

    def getCredentialsFromLocal(filePath: String): (String, String) = {
      FileHandler.checkIfFileAccessibleByOthers(filePath, GimelConstants.LOCAL_FILE_SYSTEM, true)
      val fileContent = scala.io.Source.fromFile(filePath).getLines.mkString("\n")
      (fileContent.split('\n')(0), fileContent.split('\n')(1))
    }
}
