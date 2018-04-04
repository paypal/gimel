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

object SparkToJavaConverter {
  /**
    * @param schemaObject type of dataframe column type
    * @return java sql type
    *         (Reference : https://docs.oracle.com/javase/8/docs/api/java/sql/Types.html)
    */
  def getSQLType(schemaObject: Any): Int = {
    schemaObject match {
      case org.apache.spark.sql.types.StringType =>
        java.sql.Types.VARCHAR
      case org.apache.spark.sql.types.ShortType =>
        java.sql.Types.SMALLINT
      case org.apache.spark.sql.types.IntegerType =>
        java.sql.Types.INTEGER
      case org.apache.spark.sql.types.LongType =>
        java.sql.Types.BIGINT
      case org.apache.spark.sql.types.DoubleType =>
        java.sql.Types.DOUBLE
      case org.apache.spark.sql.types.FloatType =>
        java.sql.Types.FLOAT
      case org.apache.spark.sql.types.TimestampType =>
        java.sql.Types.TIMESTAMP
      case org.apache.spark.sql.types.DateType =>
        java.sql.Types.DATE
      case org.apache.spark.sql.types.ByteType =>
        java.sql.Types.BINARY
      case org.apache.spark.sql.types.DecimalType =>
        java.sql.Types.DECIMAL
      case _ =>
        throw new JDBCDataTypeException("Data Type mismatch/Not Supported. Please verify the data types of dataframe and target table.")
    }
  }

  /**
    * This method gives the teradata data type given the spark sql data type
    *
    * @param sparkDataType spark dataframe column datatype type
    * @return corresponding data type string
    */
  def getTeradataDataType(sparkDataType: Any): String = {
    sparkDataType match {
      case org.apache.spark.sql.types.StringType =>
        "varchar(1000)"
      case org.apache.spark.sql.types.ShortType =>
        "smallint"
      case org.apache.spark.sql.types.IntegerType =>
        "int"
      case org.apache.spark.sql.types.LongType =>
        "bigint"
      case org.apache.spark.sql.types.DoubleType =>
        "float"
      case org.apache.spark.sql.types.FloatType =>
        "float"
      case org.apache.spark.sql.types.TimestampType =>
        "timestamp"
      case org.apache.spark.sql.types.DateType =>
        "date"
      case org.apache.spark.sql.types.ByteType =>
        "byteint"
      case org.apache.spark.sql.types.DecimalType =>
        "decimal"
      case _ =>
        throw new JDBCDataTypeException("Data Type mismatch/Not Supported. Please verify the data types of dataframe")
    }
  }
}


/**
  * Custom Exception for JDBCDataType related errors
  *
  * @param message Message to Throw
  * @param cause   A Throwable Cause
  */
private class JDBCDataTypeException(message: String, cause: Throwable)
  extends RuntimeException(message) {
  if (cause != null) {
    initCause(cause)
  }

  def this(message: String) = this(message, null)
}
