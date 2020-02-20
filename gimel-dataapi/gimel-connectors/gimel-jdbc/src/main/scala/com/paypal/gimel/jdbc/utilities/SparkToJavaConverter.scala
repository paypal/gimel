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

import org.apache.spark.sql.types.{DataType}

import com.paypal.gimel.jdbc.exception._

object SparkToJavaConverter {
  /**
    * @param schemaObject type of dataframe column type
    * @return java sql type
    *         (Reference : https://docs.oracle.com/javase/8/docs/api/java/sql/Types.html)
    */
  def getSQLType(schemaObject: org.apache.spark.sql.types.DataType): Int = {

    val typeName: String = schemaObject.typeName.toUpperCase

    // regex to find the decimal datatype
    val decimalRegex =
      """DECIMAL\(.*,.*\)""".r

    // println(s"Typename: ${typeName}")

    typeName match {
      case "STRING" =>
        java.sql.Types.VARCHAR
      case "SHORT" =>
        java.sql.Types.SMALLINT
      case "INTEGER" =>
        java.sql.Types.INTEGER
      case "LONG" =>
        java.sql.Types.BIGINT
      case "DOUBLE" =>
        java.sql.Types.DOUBLE
      case "FLOAT" =>
        java.sql.Types.FLOAT
      case "TIMESTAMP" =>
        java.sql.Types.TIMESTAMP
      case "DATE" =>
        java.sql.Types.DATE
      case "BYTE" =>
        java.sql.Types.TINYINT
      case decimalRegex() =>
        java.sql.Types.DECIMAL
      case "BOOLEAN" =>
        java.sql.Types.BOOLEAN
      case _ =>
        throw new JDBCDataTypeException(s"Data Type mismatch/Not Supported for $typeName. Please verify the data types of dataframe and target table.")
    }
  }

  /**
    * This method gives the teradata data type given the spark sql data type
    *
    * @param sparkDataType spark dataframe column datatype type
    * @return corresponding data type string
    */
  def getTeradataDataType(sparkDataType: org.apache.spark.sql.types.DataType): String = {

    val typeName: String = sparkDataType.typeName.toUpperCase

    // regex to find the decimal datatype
    val decimalRegex =
      """DECIMAL\(.*,.*\)""".r

    typeName match {
      case "STRING" =>
        "VARCHAR(1000)"
      case "SHORT" =>
        "SMALLINT"
      case "INTEGER" =>
        "INT"
      case "LONG" =>
        "BIGINT"
      case "DOUBLE" =>
        "FLOAT"
      case "FLOAT" =>
        "FLOAT"
      case "TIMESTAMP" =>
        "TIMESTAMP"
      case "DATE" =>
        "DATE"
      case "BYTE" =>
        "BYTEINT"
      case decimalRegex() => {
        val deci1: Int = typeName.substring(typeName.indexOf("(") + 1, typeName.indexOf(",")).toInt
        val deci2: Int = typeName.substring(typeName.indexOf(",") + 1, typeName.indexOf(")")).toInt
        s"DECIMAL($deci1, $deci2)"
      }
      case "BOOLEAN" =>
        "BOOLEAN"
      case _ =>
        throw new JDBCDataTypeException("Data Type mismatch/Not Supported. Please verify the data types of dataframe")
    }
  }

  /**
    * This method gives the spark sql  data type for a given tera data type
    *
    * @param teraDataType spark dataframe column datatype type
    * @return corresponding data type string
    */
  def getSparkTypeFromTeradataDataType(teraDataType: String): DataType = {

    if (teraDataType.contains("CHAR")) {
      org.apache.spark.sql.types.StringType
    }
    else if (teraDataType == "INTEGER") {
      org.apache.spark.sql.types.IntegerType
    }
    else if (teraDataType.contains("TIMESTAMP")) {
      org.apache.spark.sql.types.TimestampType
    }
    else if (teraDataType.contains("DECIMAL")) {
      val deci1: Int = teraDataType.substring(teraDataType.indexOf("(") + 1, teraDataType.indexOf(",")).toInt
      val deci2: Int = teraDataType.substring(teraDataType.indexOf(",") + 1, teraDataType.indexOf(")")).toInt
      org.apache.spark.sql.types.DecimalType(deci1, deci2)
    }
    else if (teraDataType.contains("DATE")) {
      org.apache.spark.sql.types.DateType
    }
    else if (teraDataType.contains("SMALLINT")) {
      org.apache.spark.sql.types.ShortType
    }
    else if (teraDataType.contains("BIGINT")) {
      org.apache.spark.sql.types.LongType
    }
    else if (teraDataType.contains("FLOAT")) {
      org.apache.spark.sql.types.FloatType
    }
    else if (teraDataType.contains("DOUBLE")) {
      org.apache.spark.sql.types.DoubleType
    }
    else if (teraDataType.contains("BYTE")) {
      org.apache.spark.sql.types.ByteType
    }
    else {
      throw new JDBCDataTypeException("Data Type mismatch/Not Supported. Please verify the data types of dataframe")
    }
  }
}
