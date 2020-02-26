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

package com.paypal.gimel.common.utilities

import scala.collection.immutable.Map
import scala.util.matching.Regex

import org.apache.spark.sql.types._

import com.paypal.gimel.logger.Logger

object SQLDataTypesUtils {

  private val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
  private val logger: Logger = Logger(this.getClass.getName)

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
    * @param name : type name
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
                s"Failed to convert the DataType string '$name' to a data type.")))))
    }
  }

  private[utilities] val WHERE: String = "where"
  private val GROUP_BY: String = "group" // removing by to avoid space related matche error
  private val ORDER_BY: String = "order" // removing by to avoid space related match error
  private val SAMPLE: String = "sample"
  private val LIMIT: String = "limit"
  private[utilities] val WHERE_PATTERN: Regex = s"\\b$WHERE\\b".r
  private[utilities] val GROUP_BY_PATTERN: Regex = s"\\b$GROUP_BY\\b".r
  private[utilities] val ORDER_BY_PATTERN: Regex = s"\\b$ORDER_BY\\b".r
  private[utilities] val LIMIT_PATTERN: Regex = s"\\b$LIMIT\\b".r
  private[utilities] val SAMPLE_PATTERN: Regex = s"\\b$SAMPLE\\b".r

  /**
    * It places the created partition sequence on appropriate position on the incoming sql
    *
    * @param incomingSql       -> SQL received from driver
    * @param conditionSequence -> Partition sequence generated in accordance with the user configured partition columns
    * @return
    */
  def mergeConditionClause(
                            incomingSql: String,
                            conditionSequence: String
                          ): String = {
    if (Option(incomingSql).isEmpty || trim(incomingSql).isEmpty) {
      throw new IllegalStateException(
        "Incoming SQL is empty, cannot proceed further!"
      )
    }
    val incomingSqlLowerCase = incomingSql.toLowerCase()
    SQLTagUtils.tagByWhereClause(incomingSqlLowerCase) match {
      case WhereWithGroupBy =>
        s"${trim(incomingSql.substring(0, incomingSqlLowerCase.lastIndexOf(GROUP_BY)))} " +
          s"and ${trim(conditionSequence)} " +
          s"${incomingSql.substring(incomingSqlLowerCase.lastIndexOf(GROUP_BY))}"
      case WhereWithOrderBy =>
        s"${trim(incomingSql.substring(0, incomingSqlLowerCase.lastIndexOf(ORDER_BY)))} " +
          s"and ${trim(conditionSequence)} " +
          s"${incomingSql.substring(incomingSqlLowerCase.lastIndexOf(ORDER_BY))}"
      case WhereWithLimit =>
        s"${trim(incomingSql.substring(0, getLimitLastIndexOf(incomingSqlLowerCase)))} " +
          s"and ${trim(conditionSequence)} " +
          s"${incomingSql.substring(getLimitLastIndexOf(incomingSqlLowerCase))}"
      case Where =>
        s"${trim(incomingSql)} and $conditionSequence"
      case GroupBy =>
        s"${trim(incomingSql.substring(0, incomingSqlLowerCase.lastIndexOf(GROUP_BY)))} " +
          s"$WHERE ${trim(conditionSequence)} " +
          s"${incomingSql.substring(incomingSqlLowerCase.lastIndexOf(GROUP_BY))}"
      case OrderBy =>
        s"${trim(incomingSql.substring(0, incomingSqlLowerCase.lastIndexOf(ORDER_BY)))} " +
          s"$WHERE ${trim(conditionSequence)} " +
          s"${incomingSql.substring(incomingSqlLowerCase.lastIndexOf(ORDER_BY))}"
      case Limit =>
        s"${trim(incomingSql.substring(0, getLimitLastIndexOf(incomingSqlLowerCase)))} " +
          s"$WHERE ${trim(conditionSequence)} " +
          s"${incomingSql.substring(getLimitLastIndexOf(incomingSqlLowerCase))}"
      case WithoutWhere =>
        s"${trim(incomingSql)} $WHERE $conditionSequence"
      case _ =>
        throw new IllegalStateException(s"No matching where tag found for SQL: $incomingSql")
    }
  }

  private def getLimitLastIndexOf(sql: String): Int = {
    if (sql.contains(SAMPLE)) {
      sql.lastIndexOf(SAMPLE)
    } else {
      sql.lastIndexOf(LIMIT)
    }
  }

  private def trim(str: String): String = str.trim

}
