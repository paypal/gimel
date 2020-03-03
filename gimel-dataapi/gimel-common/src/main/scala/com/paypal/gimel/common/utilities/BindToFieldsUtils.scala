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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import spray.json._

import com.paypal.gimel.common.catalog.{CatalogProvider, DataSetProperties}
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.utilities.FieldsJsonProtocol._
import com.paypal.gimel.common.utilities.SQLDataTypesUtils._

object BindToFieldsUtils {
  /**
    * Gets the array of fields specified by users
    *
    * @param fieldsBindToString : Json string of array of fields
    * @param fieldsBindToDataset : Dataset Name
    * @param props Map[String, Any]
    * @return (Array[Field], String)
    */
  def getFieldsBindTo(fieldsBindToString: String, fieldsBindToDataset: String, props: Map[String, Any]): (Array[Field], String) = {
    if (fieldsBindToDataset.nonEmpty) {
      val datasetProps = CatalogProvider.getDataSetProperties(fieldsBindToDataset, props)
      val datasetFields = datasetProps.fields
      if (datasetFields == null) {
        throw new IllegalStateException("Dataset does not have fields. Please specify them in DatasetProperties or check UDC.")
      }
      val fields: Array[Field] = datasetFields.map(f => Field(f.fieldName, f.fieldType,
        Literal.default(SQLDataTypesUtils.typeNameToSQLType(f.fieldType)).simpleString))
      val fieldsBindToStr = fields.toJson.compactPrint
      (fields, fieldsBindToStr)
    } else if (fieldsBindToString.nonEmpty) {
      var fieldsBindTo: Array[Field] = Array()
      try {
        fieldsBindTo = fieldsBindToString.parseJson.convertTo[Array[Field]]
      } catch {
        case ex: Throwable =>
          ex.printStackTrace()
          throw new IllegalArgumentException(
            s"""
               | Error in parsing json --> $fieldsBindToString
               | Please check the property ${GimelConstants.FIELDS_BIND_TO_JSON}
               | $ex
               |""".stripMargin)
      }
      (fieldsBindTo, fieldsBindToString)
    } else {
      (Array.empty[Field], "")
    }
  }

  /**
    * Gets the array of fields specified by users with DataSetProperties
    *
    * @param fieldsBindToString : Json string of array of fields
    * @param fieldsBindToDataset : Dataset Name
    * @param datasetProps DataSetProperties
    * @return (Array[Field], String)
    */
  def getFieldsBindTo(fieldsBindToString: String, fieldsBindToDataset: String, datasetProps: DataSetProperties): (Array[Field], String) = {
    if (fieldsBindToDataset.nonEmpty) {
      val datasetFields = datasetProps.fields
      if (datasetFields == null) {
        throw new IllegalStateException("Dataset does not have fields. Please specify them in DatasetProperties or check UDC.")
      }
      val fields: Array[Field] = datasetFields.map(f => Field(f.fieldName, f.fieldType,
        Literal.default(SQLDataTypesUtils.typeNameToSQLType(f.fieldType)).simpleString))
      val fieldsBindToStr = fields.toJson.compactPrint
      (fields, fieldsBindToStr)
    } else if (fieldsBindToString.nonEmpty) {
      var fieldsBindTo: Array[Field] = Array()
      try {
        fieldsBindTo = fieldsBindToString.parseJson.convertTo[Array[Field]]
      } catch {
        case ex: Throwable =>
          ex.printStackTrace()
          throw new IllegalArgumentException(
            s"""
               | Error in parsing json --> $fieldsBindToString
               | Please check the property ${GimelConstants.FIELDS_BIND_TO_JSON}
               | $ex
               |""".stripMargin)
      }
      (fieldsBindTo, fieldsBindToString)
    } else {
      (Array.empty[Field], "")
    }
  }

  /**
    * Gets DataFrame bind to the specified fields
    *
    * @param dataFrame
    * @param fieldsBindTo : Array of Fields specified by users
    * @return DataFrame
    */
  def getDFBindToFields(dataFrame: DataFrame, fieldsBindTo: Array[Field]): DataFrame = {
    if (!fieldsBindTo.isEmpty) {
      val columnsInDF = dataFrame.columns.map(x => x.toLowerCase)
      val fieldsNames = fieldsBindTo.map(x => x.fieldName)
      val fieldsMissingInDF: Array[String] = fieldsNames.filterNot(x => columnsInDF.contains(x.toLowerCase))
      val fieldsInDF = fieldsNames.filter(x => columnsInDF.contains(x.toLowerCase))
      val deserializedDataFiltered = dataFrame.select(fieldsInDF.map(x => col(s"""`$x`""")): _*)
      val missingFieldsList = fieldsBindTo.filter(x => fieldsMissingInDF.contains(x.fieldName)).map(field => (field.fieldName, field.fieldType, field.defaultValue ))
      missingFieldsList.foldLeft(deserializedDataFiltered) {(x, y) => x.withColumn(y._1, lit(y._3).cast(y._2))}
    } else {
      dataFrame
    }
  }

  /**
    * Gets DataFrame bind to the specified fields
    *
    * @param sparkSession
    * @param fieldsBindToJSONString : Array of Fields specified by users as json
    * @return Empty DataFrame
    */
  def getEmptyDFBindToFields(sparkSession: SparkSession, fieldsBindToJSONString: String): DataFrame = {
    var fieldsBindTo: Array[Field] = Array()
    try {
      fieldsBindTo = fieldsBindToJSONString.parseJson.convertTo[Array[Field]]
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw new IllegalArgumentException(
          s"""
             | Error in parsing json --> $fieldsBindToJSONString
             | Please check the property ${GimelConstants.FIELDS_BIND_TO_JSON}
             | $ex
             |""".stripMargin)
    }
    val fieldNamesAndTypes = fieldsBindTo.map(x => (x.fieldName, x.fieldType)).toMap
    val fieldsToSQLDataTypeMap = getFieldNameSQLDataTypes(fieldNamesAndTypes)
    val schemaRDD = StructType(fieldNamesAndTypes.map(x =>
      StructField(x._1, fieldsToSQLDataTypeMap.getOrElse(x._1, StringType), true)).toArray )
    sparkSession.sqlContext.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schemaRDD)
  }
}
