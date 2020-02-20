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

package com.paypal.gimel.jdbc.utilities

import scala.collection.immutable.Map

import org.apache.spark.sql.types.StructField

import com.paypal.gimel.common.conf.GimelConstants

object JDBCUtilityFunctions {
  /**
    * prepareCreateStatement - From the column details passed in datasetproperties from GimelQueryProcesser,
    * create statement is constructed. If user passes primary index , set or multi set table, those will be added in the create statement
    * @param dbtable - Table Name
    * @param dataSetProps - Data set properties
    * @return - the created prepared statement for creating the table
    */
  def prepareCreateStatement(sql: String, dbtable: String, dataSetProps: Map[String, Any]): String = {
    // Here we remove the SELECT portion and have only the CREATE portion of the DDL supplied so that we can use that to create the table
    val sqlParts = sql.split(" ")
    val lenPartKeys = sqlParts.length
    val index = sqlParts.indexWhere(_.toUpperCase() == "SELECT")
    val createOnly: String = sqlParts.slice(0, index - 1).mkString(" ")

    // Here we remove the PCATALOG prefix => we replace pcatalog.storagetype.storagesystem.DB.Table with DB.Table
    val createParts = createOnly.split(" ")
    val pcatSQL = createParts.map(element => {
      if (element.toLowerCase().contains(GimelConstants.PCATALOG_STRING)) {
        // we replace pcatalog.storagetype.storagesystem.DB.Table with DB.Table
        element.split('.').tail.mkString(".").split('.').tail.mkString(".").split('.').tail.mkString(".")
      }
      else {
        element
      }
    }
    ).mkString(" ")

    val sparkSchema = dataSetProps(GimelConstants.TABLE_FILEDS).asInstanceOf[Array[StructField]]
    // From the dataframe schema, translate them into Teradata data types
    val gimelSchema: Array[com.paypal.gimel.common.catalog.Field] = sparkSchema.map(x => {
      com.paypal.gimel.common.catalog.Field(x.name, SparkToJavaConverter.getTeradataDataType(x.dataType), x.nullable)
    })
    val colList: Array[String] = gimelSchema.map(x => (x.fieldName + " " + (x.fieldType) + ","))
    val conCatColumns = colList.mkString("").dropRight(1)
    val colQulifier = s"""(${conCatColumns})"""

    // Here we inject back the columns with data types back in the SQL statemnt
    val newSqlParts = pcatSQL.split(" ")
    val PCATindex = newSqlParts.indexWhere(_.toUpperCase().contains("TABLE"))
    val catPrefix = newSqlParts.slice(0, PCATindex + 2).mkString(" ")
    val catSuffix = newSqlParts.slice(PCATindex + 2, newSqlParts.length).mkString(" ")
    val fullStatement = s"""${catPrefix} ${colQulifier} ${catSuffix}"""
    fullStatement.trim()
  }


}
