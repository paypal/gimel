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

package com.paypal.gimel.kafka.utilities

import scala.language.implicitConversions

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.kafka010.OffsetRange

import com.paypal.gimel.common.storageadmin.HDFSAdminClient._
import com.paypal.gimel.kafka.utilities.ImplicitKafkaConverters._

/**
  * Provides Implicit, Convenience Functions for Developers to Do CheckPointing Operations
  */
object ImplicitHDFSCheckPointers {

  val logger = com.paypal.gimel.logger.Logger()

  /**
    * @param offsetRangesAndCheckPointDirectory A Tuple of (Array[OffsetRange], checkPointDirectory)
    */
  implicit class CheckPointers(offsetRangesAndCheckPointDirectory: (Array[OffsetRange], String)) {
    /**
      * CheckPoints a Tuple of (Array[OffsetRange], checkPointDirectory)
      *
      * @example (Array(OffsetRange("l1", 11, 1, 1)), "${USER_DEFINED_CHECKPOINT_PATH}").saveCheckPoint
      * @return true if Success
      *
      */
    def saveCheckPoint: Boolean = {
      def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

      logger.info(" @Begin --> " + MethodName)

      val latestFile = "/latest"
      val checkPointDir = offsetRangesAndCheckPointDirectory._2
      val checkPointFile = checkPointDir + latestFile
      val contentToWrite = offsetRangesAndCheckPointDirectory._1.toStringOfKafkaOffsetRanges
      try {
        val conf = new org.apache.hadoop.conf.Configuration()
        val fs = FileSystem.get(conf)
        val latestHDFSPath = new Path(checkPointFile)
        if (!fs.exists(latestHDFSPath)) {
          writeHDFSFile(checkPointFile, contentToWrite)
        } else {
          val timeStamp = System.currentTimeMillis
          val toRenameLatestPath = checkPointDir + s"/$timeStamp"
          val toRenameLatestPathHDFS = new Path(toRenameLatestPath)
          fs.rename(latestHDFSPath, toRenameLatestPathHDFS)
          writeHDFSFile(checkPointFile, contentToWrite)
        }
      } catch {
        case ex: Throwable =>
          throw ex
      }
      true
    }
  }


  /**
    * @param checkPointDirectoryPath A Tuple of (Array[OffsetRange], checkPointDirectory)
    */
  implicit class CheckPointFetcher(checkPointDirectoryPath: String) {
    /**
      * Fetches CheckPoints as An Array[OffsetRange]
      *
      * @example ("USER_DEFINED_CHECKPOINT_PATH").fetchCheckPoint
      * @return Some(Array[OffsetRange])
      *
      */
    def fetchCheckPoint: Option[Array[OffsetRange]] = {
      def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

      logger.info(" @Begin --> " + MethodName)

      if (checkPointDirectoryPath.isEmpty) throw new HDFSCheckPointerException("Expected CheckPoint Directory, but got Empty String !")
      val latestFile = "/latest"
      val checkPointDir = checkPointDirectoryPath
      val checkPointFile = checkPointDir + latestFile
      val conf = new org.apache.hadoop.conf.Configuration()
      val fs = FileSystem.get(conf)
      val latestHDFSPath = new Path(checkPointFile)
      if (fs.exists(latestHDFSPath)) {
        val checkPointString = readHDFSFile(checkPointDirectoryPath + "/latest")
        println("inside fetchCheckPoint ->" + checkPointString)
        Some(checkPointString.split('|').map(x => CheckPointString(x)).toKafkaOffsetRanges)
      } else {
        None
      }
    }
  }

}

/**
  * Custom Exception
  *
  * @param message Message to Throw
  * @param cause   A Throwable Cause
  */
private class HDFSCheckPointerException(message: String, cause: Throwable)
  extends RuntimeException(message) {
  if (cause != null) {
    initCause(cause)
  }

  def this(message: String) = this(message, null)
}
