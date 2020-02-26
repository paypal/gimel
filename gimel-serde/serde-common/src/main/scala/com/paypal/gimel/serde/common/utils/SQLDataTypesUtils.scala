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

package com.paypal.gimel.serde.common.utils

import scala.collection.immutable.Map

import FieldsJsonProtocol._
import org.apache.spark.sql.types._
import spray.json._

object SQLDataTypesUtils {

  private val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r

  /**
    * Converts string representation of a field type to its DataType for given fields
    *
    * @param fieldsToTypeNameMap Map of Field Name to Field Type Name
    * @return Map of FieldName to Spark SQL DataType
    */
  def getFieldNameSQLDataTypes(fieldsToTypeNameMap: Map[String, String]): Map[String, DataType] = {
    fieldsToTypeNameMap.map { each =>
      (each._1, typeNameToSQLType(each._2))
    }
  }

  /**
    * Converts string representation of a field typeName to its DataType
    */
  val nonDecimalTypeNameToType: Map[String, DataType] = {
    Seq(NullType, DateType, TimestampType, BinaryType, IntegerType, BooleanType, LongType,
      DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType )
      .map(t => t.typeName -> t).toMap
  }

  /**
    * Converts a string representation returned by simpleString of a field type to its DataType
    */
  val nonDecimalSimpleStringToType: Map[String, DataType] = {
    Seq(NullType, DateType, TimestampType, BinaryType, IntegerType, BooleanType, LongType,
      DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType)
      .map(t => t.simpleString -> t).toMap
  }

  /**
    * Converts string representation returned by toString method of a field type to its DataType
    */
  val nonDecimalStringToType: Map[String, DataType] = {
    Seq(NullType, DateType, TimestampType, BinaryType, IntegerType, BooleanType, LongType,
      DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType)
      .map(t => t.toString -> t).toMap
  }

  /**
    * Converts sql representation of a field type to its DataType
    */
  val nonDecimalSQLToType: Map[String, DataType] = {
    Seq(NullType, DateType, TimestampType, BinaryType, IntegerType, BooleanType, LongType,
      DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType)
      .map(t => t.sql.toLowerCase -> t).toMap
  }

  /**
    * Given the string representation of a type, return its DataType
    *
    * @param name type name
    * @return Spark SQL DataType
    */
  def typeNameToSQLType(name: String): DataType = {
    name.toLowerCase() match {
      case "decimal" => DecimalType.USER_DEFAULT
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case other => nonDecimalStringToType.getOrElse(
        other, nonDecimalTypeNameToType.getOrElse(
          other, nonDecimalSQLToType.getOrElse(
            other, nonDecimalSimpleStringToType.getOrElse(
              other, throw new IllegalArgumentException(
                s"Failed to convert the DataType string '$name' to a SQL data type.")))))
    }
  }

  /**
    * Gets schema StructType from given list
    *
    * @param fieldsBindToJSONString Array of Fields specified by users as json
    * @return StructType
    */
  def getSchemaFromBindToFieldsJson(fieldsBindToJSONString: String): StructType = {
    val fieldsBindTo = fieldsBindToJSONString.parseJson.convertTo[Array[Field]]
    val fieldNamesAndTypes = fieldsBindTo.map(x => (x.fieldName, x.fieldType)).toMap
    val fieldsToSQLDataTypeMap = getFieldNameSQLDataTypes(fieldNamesAndTypes)
    val schemaRDD: StructType = StructType(fieldNamesAndTypes.map(x =>
      StructField(x._1, fieldsToSQLDataTypeMap.getOrElse(x._1, StringType), true)).toArray )
    schemaRDD
  }
}
