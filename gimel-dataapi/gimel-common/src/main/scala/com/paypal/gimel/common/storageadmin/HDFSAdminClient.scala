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

package com.paypal.gimel.common.storageadmin

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.apache.hadoop.security.UserGroupInformation

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.logger.Logger

/**
  * Provides HDFS Read And Write operations
  */

object HDFSAdminClient {

  val logger = Logger()

  /**
    * Deletes HDFS Path
    *
    * @param path      Fully Qualified Path of the File to Write data into
    * @param recursive true - delete path recursively, false - delete just the provided path
    */
  def deletePath(path: String, recursive: Boolean): AnyVal = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    try {
      val conf = new org.apache.hadoop.conf.Configuration()
      val fs = FileSystem.get(conf)
      val hdfsPath = new Path(path)
      if (fs.exists(hdfsPath)) {
        logger.info(s"Path exists, deleting < ${path} > with recursive? < ${recursive.toString} >")
        fs.delete(hdfsPath, recursive)
      }
      else logger.warning(s"Path does NOT exists. Nothing to Delete.")
    }
    catch {
      case ex: Throwable => {
        ex.printStackTrace()
        throw ex
      }
    }
  }

  /**
    * Writes a String Content to HDFS Path
    *
    * @param path    Fully Qualified Path of the File to Write data into
    * @param toWrite Content to Write
    */
  def writeHDFSFile(path: String, toWrite: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(s" @Begin -->  + $MethodName")

    try {
      if (toWrite.isEmpty) {
        throw new Exception(s"Provided Content to Write into $path is empty!")
      } else {
        val conf = new org.apache.hadoop.conf.Configuration()
        val fs = FileSystem.get(conf)
        val hdfsPath = new Path(path)
        if (fs.exists(hdfsPath)) throw new Exception(s"File $path already Exists !")
        val writer = fs.create(hdfsPath)
        writer.writeBytes(toWrite)
        writer.close()
        logger.info(s"Written to File $path  |  Content $toWrite")
      }
    } catch {
      case ex: Throwable =>
        throw ex
    }
  }

  /**
   * hdfsFolderExists checks whether the HDFS folder exists or not.
   *
   * @param incomingFolder - HDFS folder name
   * @return returns a boolean based on whether the hdfs folder exists or not
   */
  def hdfsFolderExists(incomingFolder: String): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(s" @Begin -->  + $MethodName")

    val conf = new org.apache.hadoop.conf.Configuration()
    val fs = FileSystem.get(new URI(incomingFolder), conf)
    val hdfsPath = new Path(incomingFolder)
    if (fs.exists(hdfsPath)) {
      true
    }
    else {
      false
    }
  }

  /**
    * Reads the Contents of an HDFS File
    *
    * @param path Fully Qualified Path of the File to Read
    * @return Content of the File
    */
  def readHDFSFile(path: String): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(s" @Begin -->  + $MethodName")
    try {
      val conf = new org.apache.hadoop.conf.Configuration()
      val fs = FileSystem.get(conf)
      val hdfsPath = new Path(path)
      if (fs.exists(hdfsPath)) {
        val input = fs.open(hdfsPath)
        val br = new BufferedReader(new InputStreamReader(input))
        var fileContent: String = ""
        var line: String = br.readLine()
        logger.info(s"Reading HDFS file content from $path")
        while (line != null) {
          fileContent = fileContent + line + "\n"
          line = br.readLine()
        }
        fs.close()
        fileContent
      } else {
        throw new Exception(s"File Does not Exists $path")
      }
    } catch {
      case ex: Throwable =>
        throw ex
    }
  }

  /**
    * Reads the Contents of an HDFS File from standalone java application
    *
    * @param path Fully Qualified Path of the File to Read
    * @return Content of the File
    */
  def standaloneHDFSRead(path: String, principal: String, keyTabPath: String): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(s" @Begin -->  + $MethodName")
    try {
      val conf = new org.apache.hadoop.conf.Configuration()
      val hdfsPath = new Path(path)
      val hadoop_conf_dir = System.getenv("HADOOP_CONF_DIR")
      conf.addResource(new Path(hadoop_conf_dir + "/" + "core-site.xml"))
      conf.addResource(new Path(hadoop_conf_dir + "/" + "hdfs-site.xml"))
      conf.set(GimelConstants.HDFS_IMPL, GimelConstants.DISTRIBUTED_FS)
      conf.set(GimelConstants.FILE_IMPL, GimelConstants.LOCAL_FS)
      conf.set(GimelConstants.SECURITY_AUTH, "kerberos")
      UserGroupInformation.setConfiguration(conf)
      val fs = FileSystem.get(conf)
      val input: FSDataInputStream = fs.open(hdfsPath)
      val br = new BufferedReader(new InputStreamReader(input))
      var fileContent: String = ""
      var line: String = br.readLine()
      logger.info(s"Reading HDFS file content from $path")
      while (line != null) {
        fileContent = fileContent + line + "\n"
        line = br.readLine()
      }
      fs.close()
      fileContent
      // }
      //    else {
      //        throw new Exception(s"File Does not Exists $path")
      //      }
    }
    catch {
      case ex: Throwable =>
        throw ex
    }
  }
}
