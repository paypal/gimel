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

package com.paypal.gimel.sftp.utilities

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.security._
import com.paypal.gimel.common.storageadmin.HDFSAdminClient


object SFTPUtilities {

  /**
    * It reads the password from HDFS file through HDFS admin client utility
    * Also if the file is not "700", where the file has some read/write/execute permissions for group and others, we will raise a warning to the user
    * @param filePath - The path of the HDFS ile that has the password
    * @return - returns the password
    */
  def getPasswordFromHDFS(filePath: String): String = {
    FileHandler.checkIfFileAccessibleByOthers(filePath, GimelConstants.HADDOP_FILE_SYSTEM, true)
    HDFSAdminClient.readHDFSFile(filePath).trim
  }

  /**
    * It reads the password from Local file system
    * Also if the file is not "700", where the file has some read/write/execute permissions for group and others, we will raise a warning to the user
    * @param filePath - - The path of the local file that has the password
    * @return - returns the password
    */
  def getPasswordFromLocal(filePath: String): String = {
    FileHandler.checkIfFileAccessibleByOthers(filePath, GimelConstants.LOCAL_FILE_SYSTEM, true)
    scala.io.Source.fromFile(filePath).getLines.mkString
  }
}
