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

package com.paypal.gimel.hbase.utilities

import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import com.paypal.gimel.hbase.conf.HbaseConfigs
import com.paypal.gimel.logger.Logger

/**
  * HBASE implementations internal to Gimel
  */
object HBaseUtilities {

  def apply(sparkSession: SparkSession): HBaseUtilities = new HBaseUtilities(sparkSession)

}

class HBaseUtilities(sparkSession: SparkSession) {
  val logger = Logger()
  val columnFamilyNamePattern = "(.+):(.+)".r
  lazy val hbaseScanner = HBaseScanner()

  /**
    *
    * @param dataDrame DataFrame to cast all columns to string format.
    * @return Dataframe with all string data.
    */
  def castAllColsToString(dataDrame: DataFrame): DataFrame = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    logger.info("Casting All Columns as String")
    val k = dataDrame.schema.fieldNames.foldRight(dataDrame) {
      (column: String, df: DataFrame) => df.withColumn(column, df(column).cast(StringType))
    }
    logger.info("Coalescing All Columns with Null Values to Empty String")
    val returningDF = k.schema.fieldNames.foldRight(k) {
      (fieldName: String, df: DataFrame) => df.withColumn(fieldName, coalesce(df(fieldName), lit("")))
    }
    logger.info("Done with Column Coalese operation")
    returningDF
  }

  /**
    * This function scans the sample records from hbase table if column mapping parameter is empty
    *
    * @param namespace String HBase Namespace Name
    * @param tableName String HBase Table Name
    * @param tableColumnMapping String (:key,cf1:c1,cf1:c2,cf2:c3)
    * @return
    */

  def getColumnMappingForColumnFamily(namespace: String, tableName: String, tableColumnMapping: String, maxRecords: Int, maxColumns: Int): Map[String, Array[String]] = {
    val schema = getColumnMappingForColumnFamily(tableColumnMapping)
    if (schema.isEmpty) {
      logger.info("Column family to column mapping is not present or is in wrong format, scanning the sample records.")
      val schemaFromSampleRecords = hbaseScanner.getSchema(namespace, tableName, maxRecords, maxColumns)
      if (schemaFromSampleRecords.isEmpty) {
        throw new IllegalStateException("No columns found while scanning. May be the table is empty.")
      }
      schemaFromSampleRecords
    } else {
      schema
    }
  }

  /**
    * This function performs Table Column Mapping
    *
    * @param tableColumnMapping String (:key,cf1:c1,cf1:c2,cf2:c3)
    * @return
    */

  def getColumnMappingForColumnFamily(tableColumnMapping: String): Map[String, Array[String]] = {
    // to remove the exact location of :Key
    val indexOfKey: Int = tableColumnMapping.split(",").indexOf(":key")
    val updateMapping = if (indexOfKey != -1) {
      val mappingBuffer = tableColumnMapping.split(",").toBuffer
      mappingBuffer.remove(indexOfKey)
      mappingBuffer.toArray.mkString(",")
    } else {
      tableColumnMapping
    }

    try {
      // checking if CF Mapping matches the pattern
      val columnMapping = updateMapping.split(",").flatMap {
        case columnFamilyNamePattern(cf, cname) => Some((StringEscapeUtils.escapeJava(cf), StringEscapeUtils.escapeJava(cname)))
        case _ => throw new IllegalArgumentException(
          s"""
             |Column family to column mapping pattern is not correct -> ${tableColumnMapping}
             |Please check the property ${HbaseConfigs.hbaseColumnMappingKey}, it should be in format -> cf1:c1,cf1:c2,cf2:c3
             |""".stripMargin)
      }.groupBy(_._1).map { case (k, v) => (k, v.map(_._2)) }
      columnMapping
    } catch {
      case ex: IllegalArgumentException =>
        logger.warning(ex.getMessage)
        Map.empty[String, Array[String]]
    }
  }
}
