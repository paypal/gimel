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

package com.paypal.gimel.druid.util

import java.lang.reflect.Field

import scala.reflect.ClassTag

import io.druid.granularity.{QueryGranularities, QueryGranularity}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods._

import com.paypal.gimel.druid.conf.DruidConstants
import com.paypal.gimel.druid.model.{DruidDimension, DruidMetric}

/**
  * Object instance for Druid Utility. Contains all the utility methods required for druid-connector
  */
object DruidUtility {

  // Format object to serialize and deserialize used by json4s
  implicit val format: Formats =
    DefaultFormats + DruidMetric.drudMetricSerializer + DruidDimension.drudDimensionSerializer

  /**
    * Given a date or time in String, this method converts this datetime to
    * org.joda.time.DateTime using the specified format.
    * Returns current datetime if datetime string is null.
    * Supports - millis, seconds and other DateTime format
    *
    * @param datetime Datetime in String. Can be time in millis, seconds or in any DATETIME format
    * @param format   String format for transforming string to org.joda.time.DateTime. Default: millis
    * @return org.joda.time.DateTime given a datetime in string and the specified format.
    */
  def extractDateTime(datetime: String, format: String = "millis"): DateTime = {
    if (Option(datetime).isDefined) {
      format match {
        // Converts Milliseconds to org.joda.time.DateTime
        case DruidConstants.MILLISECONDS =>
          new DateTime(datetime.toLong, DateTimeZone.UTC)

        // Converts Seconds to org.joda.time.DateTime
        case DruidConstants.SECONDS =>
          new DateTime(toMillis(datetime.toLong), DateTimeZone.UTC)

        // Converts ISO datetime to org.joda.time.DateTime
        case DruidConstants.ISO =>
          DateTime.parse(datetime).withZone(DateTimeZone.UTC)

        // Converts all the other DateTime formats to org.joda.time.DateTime
        case otherFormat: String =>
          val formatter = DateTimeFormat.forPattern(otherFormat).withZoneUTC()
          formatter.parseDateTime(datetime)
      }
    } else {
      // Returns current time in UTC if datetime string is null.
      new DateTime(DateTimeZone.UTC)
    }
  }

  /**
    * Converts seconds to Milliseconds
    *
    * @param seconds Long seconds to be converted
    * @return Long Milliseconds corresponding to the seconds
    */
  def toMillis(seconds: Long): Long = seconds * 1000

  /**
    * Fetch List of class variables
    *
    * @param tag ClassTag[T] object
    * @tparam T Class type passed
    * @return List[Field] of fields that T class contains
    */
  def fetchClassVariable[T](implicit tag: ClassTag[T]): List[Field] =
    tag.runtimeClass.getDeclaredFields.toList


  /**
    * Get Hive Table Field names given the name of hive table
    *
    * @param dataset Hive Table name
    * @return List[String] of Field names for the hive table
    */
  def getFieldNames(dataset: String, sparkSession: SparkSession): List[String] = {
    extractFields(sparkSession.read.table(dataset).schema)
  }


  /**
    * Get Hive Table Field names given the Dataframe.
    *
    * @param dataFrame Dataframe for which schema is to be returned
    * @return List[String] of Field names for the hive table
    */
  def getFieldNames(dataFrame: DataFrame): List[String] = {
    extractFields(dataFrame.schema)
  }

  /**
    * Given a Schema StructType, extract the field names.
    *
    * @param schema StructType Schema
    * @return List[String] of field names
    */
  def extractFields(schema: StructType): List[String] = {
    Option(schema)
      .getOrElse(StructType(List.empty[StructField]))
      .map(_.name).toList
  }

  /**
    * Method to parse a string to a Custom object.
    *
    * @param value String value to be parsed.
    * @tparam T Custom object to parse the String.
    * @return Parsed object based on the value and T.
    */
  def parseString[T: ClassTag](value: String)(implicit manifest: Manifest[T]): T = {
    parse(s"""$value""")
      .extract[T](format, mf = manifest)
  }

  /**
    * Method to Fetch Query Granularity based on the String Provided.
    *
    * @param granularityString Query Granularity String to be parsed
    * @return QueryGranularity Object corresponding to the string
    */
  def fetchQueryGranularity(granularityString: String): QueryGranularity = {
    // Using Reflection, find a field with the same name
    // as the query granularity string specified by the user
    val granularityField = DruidUtility.fetchClassVariable[QueryGranularities]
      .find(field => granularityString.equalsIgnoreCase(field.getName))

    // If given Granularity is not found then throw an Error
    if (granularityField.isEmpty) {
      val errorMsg = s"Specified Query Granularity $granularityString is not a valid Granularity"
      throw new IllegalArgumentException(errorMsg)
    }

    // Extract QueryGranularity Variable value from the field
    val queryGranularity = QueryGranularities.MINUTE
    granularityField.get.get(queryGranularity).asInstanceOf[QueryGranularity]
  }
}
