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

package com.paypal.gimel.testsuite.storages

import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.paypal.gimel.DataSet
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.testsuite.utilities.{GimelTestSuiteProperties, HiveJDBCUtilsForTestSuite}

abstract class StorageValidation(val dataset: DataSet, val sparkSession: SparkSession, val gimelProps: GimelTestSuiteProperties) {

  /**
    * Holder for All Stats Collected in the Validation
    */
  var stats: Map[String, String] = Map()

  /**
    * Holder for All DDLs Collected in the Validation, though the Stats Map can be used for same purpose - this additional object provides some distinction
    */
  var ddls: Map[String, String] = Map()

  /**
    * Hive Jars Needed for Hive JDBC Utils
    */
  val hiveJarsForDDL: String = gimelProps.gimelHiveJarsToAdd
  /**
    * Hive JDBC Utils to create JDBC Connectivity and deploy DDL's
    */
  val hiveJDBCUtils = HiveJDBCUtilsForTestSuite(gimelProps, gimelProps.cluster)

  /**
    * Assign the DataSetName while implementing
    */
  val dataSetName: String

  // Get Logger
  val logger = Logger()

  /**
    * Extend & Implement all the bootStrap Functionalities
    *
    * @return A Tuple of (DDL , STATS)
    */
  def bootStrap(): (Map[String, String], Map[String, String]) = (ddls, stats)

  /**
    * Extend & Implement all the cleanUp Functionalities
    *
    * @return A Tuple of (DDL , STATS)
    */
  def cleanUp(): (Map[String, String], Map[String, String]) = (ddls, stats)

  /**
    * Main Place to implement Validation Steps
    *
    * @param testData DataFrame (Optional)
    * @return @return A Tuple of (DDL , STATS, Optional[DataFrame])
    */
  def validateAPI(testData: Option[DataFrame] = None): (Map[String, String], Map[String, String], Option[DataFrame]) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val storage = this.getClass.getName.replace(".", "_")
    val tag = s"$MethodName-$storage"
    try {
      val testData = prepareSmokeTestData(gimelProps.smokeTestSampleRowsCount.toInt)
      val dataSet = dataSetName
      logger.info(s"$tag | Begin Write to $dataSet...")
      dataset.write(dataSet, testData)
      logger.info(s"$tag | Write Success.")
      logger.info(s"$tag | Read from $dataSet...")
      val readDF = dataset.read(dataSet)
      val count = readDF.count()
      logger.info(s"$tag | Read Count $count...")
      logger.info(s"$tag | Sample 10 Rows -->")
      readDF.show(10)
      compareDataFrames(testData, readDF)
      stats += (s"$tag" -> s"Success @ ${Calendar.getInstance.getTime}")
    } catch {
      case ex: Throwable =>
        stats += (s"$tag" -> s"Failure @ ${Calendar.getInstance.getTime}")
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
    (ddls, stats, testData)
  }

  /**
    * execute will be the Main Function Called from the Test Suit
    *
    * @return A Tuple of (DDL , STATS)
    */
  def execute(): (Map[String, String], Map[String, String]) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    bootStrap()
    validateAPI(None)
    cleanUp()
    (ddls, stats)
  }

  /**
    * Prepare Dummy/Sample Data for SmokeTest
    *
    * @param numberOfRows Total Number of Sample Rows to Prep
    * @return DataFrame
    */
  def prepareSmokeTestData(numberOfRows: Int = 1000): DataFrame = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      def stringed(n: Int) = s"""{"id": $n, "name": "MAC-$n", "rev": ${n * 10000}}"""

      val texts: Seq[String] = (1 to numberOfRows).map { x => stringed(x) }
      val rdd: RDD[String] = sparkSession.sparkContext.parallelize(texts)
      val df: DataFrame = sparkSession.read.json(rdd)

      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      df
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

  /**
    * One Place to Handle Exception
    *
    * @param ex      Throwable Exception
    * @param message A Custom Message
    */
  def handleException(ex: Throwable, message: String = ""): Nothing = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    logger.error(s"Exception Handler Called with error --> ")
    ex.printStackTrace()
    if ((message contains "cleanUpES")
      || (message contains "cleanUpESHive")
      || (message contains "cleanUpHBase")
      || (message contains "cleanUpHive")
      || (message contains "cleanUpKafka")
      || (message contains "cleanUpKafkaHive")
      || (message contains "validateAPI")
      || (message contains "cleanUpTeradataHive")
    ) {
      // Do nothing
    } else {
      cleanUp()
    }
    throw new Exception(s"An Error Occurred <$message> \n${ex.getMessage}")
  }

  /**
    * Compares Two DataFrames & Fails if the data dont Match
    *
    * @param left  DataFrame
    * @param right DataFrame
    * @param tag   Some Identifier from the Caller
    */
  def compareDataFrames(left: DataFrame, right: DataFrame, tag: String = "Not Passed"): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val testDataColumns = left.columns
    val writeMinusRead = left.selectExpr(testDataColumns: _*).except(right.selectExpr(testDataColumns: _*))
    val readMinusWrite = right.selectExpr(testDataColumns: _*).except(left.selectExpr(testDataColumns: _*))
    val writeMinusReadCount = writeMinusRead.count()
    val readMinusWriteCount = readMinusWrite.count()
    val diffCount = writeMinusReadCount + readMinusWriteCount
    val diffMsg =
      s"""
         |writeMinusReadCount --> $writeMinusReadCount
         |readMinusWriteCount --> $readMinusWriteCount
         |""".stripMargin
    logger.info(s"Differences --> $diffMsg")
    if (diffCount != 0) {
      val ex = new Exception(diffMsg)
      logger.info("writeMinusRead --> \n")
      writeMinusRead.show
      logger.info("readMinusWrite --> \n")
      readMinusWrite.show
      handleException(ex, s"Some Error While Executing Method $MethodName")
    }
    stats += (s"$tag" -> s"Success @ ${Calendar.getInstance.getTime}")
  }


  /**
    * Deploy DDLS
    *
    * @param executeDDL ddl string
    *
    */
  def deployDDL(executeDDL: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    hiveJDBCUtils.withStatement { statement =>
      hiveJarsForDDL.split(",").foreach { jarsToAdd =>
        statement.execute(s"ADD JAR $jarsToAdd")
      }
      statement.execute(executeDDL)
    }

  }

}
