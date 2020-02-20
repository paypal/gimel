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

package com.paypal.gimel.common.security

import java.nio.file._

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{FileSystem, Path}

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.logger.Logger

object FileHandler {

  val logger = Logger(this.getClass)

  /**
    * For both Local and HDFS file system, this function checks whether the file is protected with "700" mode.
    * If the file HAS one of READ, WRITE, EXECUTE permissions for either GROUP or OTHERS, we will post a warning to the user to protect the file
    * @param filePath - The path of the file
    * @param source - Tells whether it is a local or hadoop file system
    */
  def warnIfFileAccessibleByOthers(filePath: String, source: String): Unit = {
    source.toLowerCase() match {
      case GimelConstants.HADDOP_FILE_SYSTEM =>
        val conf = new org.apache.hadoop.conf.Configuration()
        val fs = FileSystem.get(conf)
        val hdfsPath = new Path(filePath)
        if (fs.exists(hdfsPath)) {
          val permission = fs.getFileStatus(hdfsPath).getPermission.toString
          if (permission.substring(3, permission.length) != "------") {
            val message = s"FILE IS NOT PROTECTED. PLEASE PROTECT THE FILE WITH PROPER PERMISSIONS (700) : ${filePath}"
            logger.warning(message)

          }
        }
      case GimelConstants.LOCAL_FILE_SYSTEM =>
        val path = Paths.get(filePath)
        if (Files.exists(path)) {
          val p = Files.getPosixFilePermissions(path)
          if (p.asScala.exists(x => x.toString.startsWith("OTHER") || x.toString.startsWith("GROUP"))) {
            val message = s"FILE IS NOT PROTECTED. PLEASE PROTECT THE FILE WITH PROPER PERMISSIONS (700) : ${filePath}"
            logger.warning(message)
          }
        }

    }
  }

}
