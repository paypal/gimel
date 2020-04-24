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

package com.paypal.gimel.common.security

import scala.collection.immutable.Map
import scala.util.Try

import com.paypal.gimel.common.catalog.{DataSetProperties, Field}
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.utilities.GenericUtils
import com.paypal.gimel.logger.Logger

object ColumnClassHandler {

  val logger = Logger(this.getClass)

  /**
    * Currently the code that sends alerts and throws exception if column class restriction configured, is guarded under if (false), so that
    * it wont get executed. It is for future when we enable strict restriction for not accessing or alerting when users access restricted columns
    * @param dataSetProps - All dataset related attributes
    */
  def warnIfColumnsRestricted(dataSetProps: Map[String, Any]): Unit = {

    val actualProps: DataSetProperties = dataSetProps(
      GimelConstants.DATASET_PROPS
    ).asInstanceOf[DataSetProperties]
    val restrictedColumns = getRestrictedColumns(dataSetProps)
    if (restrictedColumns.nonEmpty) {
      restrictedColumns.foreach(
        x =>
          logger.info(s"message = ${x._1 + " : " + x._2}," +
            s" sendername = ${sys.env.getOrElse("USER", "UnknownUser")}")
      )
    }
    val securityAccessRestrictionEnabled: Boolean = Try(GenericUtils
      .getConfigValue(
        GimelConstants.GIMEL_SECURITY_ACCESS_RESTRICTED_RAISE_EXCEPTION_ENABLED,
        actualProps.props,
        Map.empty,
        GimelConstants.GIMEL_SECURITY_ACCESS_RESTRICTED_RAISE_EXCEPTION_ENABLED_DEFAULT_STRING
      ).toBoolean).getOrElse(GimelConstants.GIMEL_SECURITY_ACCESS_RESTRICTED_RAISE_EXCEPTION_ENABLED_DEFAULT)
    if (securityAccessRestrictionEnabled) {
      // GimelConstants.GIMEL_SECURITY_ACCESS_RESTRICTED_RAISE_EXCEPTION_ENABLED =>
      // use this property in future if we want to restrict access
      throw new Exception(
        s"""Restricted to Access DataSet with fields of class level <= ${getRestrictionLevel(
          actualProps
        )}"""
      )
    }
  }

  /**
    * This function will get the restriction Level configured. Currently it is configured as Class 3. We want to capture Class 2 and Class 3 columns in audit logs.
    * It returns a map of ColumnClass as the key and set of comma seperated columns as values
    * @param dataSetProps - All dataset related attributes
    * @return - map of ColumnClass as the key and set of comma seperated columns as values
    */
  def getRestrictedColumns(dataSetProps: Map[String, Any]): Map[String, String] = {
    val actualProps: DataSetProperties = dataSetProps(
      GimelConstants.DATASET_PROPS
    ).asInstanceOf[DataSetProperties]
    val fields: Array[Field] = actualProps.fields
    // assume this is coming from sparkConf
    val restrictionLevel = getRestrictionLevel(actualProps)

    val columnsByColumnClass =
      getColumnsByColumnClass(fields, actualProps.props)
    val restrictedColumns = columnsByColumnClass.flatMap {
      case (columnClass, columns) if columnClass.level >= restrictionLevel =>
        Some(
          columnClass.toString -> columns
            .map(_.columnName)
            .mkString(GimelConstants.COMMA)
        )
      case _ => None
    }

    // if Debug logging on, then log restricted columns
    restrictedColumns
  }

  def getColumnsByColumnClass(fields: Array[Field], props: Map[String, String]): Map[ColumnClass, Array[Column]] = {
    fields
      .map(Column(_, props))
      .groupBy(_.columnClass)
      .flatMap {
        case (Some(columnClass), columns) =>
          Some(columnClass -> columns)
        case _ => None
      }
  }

  private def getRestrictionLevel(actualProps: DataSetProperties): Int = {
    GenericUtils
      .getConfigValue(
        GimelConstants.GIMEL_SECURITY_ACCESS_AUDIT_CLASS_LEVEL,
        actualProps.props,
        Map.empty,
        GimelConstants.GIMEL_SECURITY_ACCESS_AUDIT_CLASS_DEFAULT_LEVEL
      )
      .toInt
  }
}

case class Column(columnName: String,
                  columnType: String,
                  columnClass: Option[ColumnClass])

object Column {

  def apply(field: Field, props: Map[String, String]): Column = {
    val auditClassProvider = GenericUtils.getConfigValueOption(
      GimelConstants.GIMEL_SECURITY_ACCESS_AUDIT_CLASS_PROVIDER,
      props,
      Map.empty
    )
    val columnClassSeq = ColumnClass(field.columnClass)
    val applicableColumnClass = columnClassSeq
      .sortBy(
        columnClass =>
          (
            if (auditClassProvider.isDefined && columnClass.provider.equalsIgnoreCase(auditClassProvider.get)) {
              0
            } else {
              1
            },
            columnClass.level
          )
      )
      .headOption
    new Column(
      field.fieldName.replace("\\", ""),
      field.fieldType,
      applicableColumnClass
    )
  }
}

case class ColumnClass(level: Int, provider: String) {
  override def toString: String = s"$level:$provider"
}

object ColumnClass {

  def apply(value: String): Seq[ColumnClass] = {
    if (GenericUtils.isStrNotEmpty(value)) {
      value match {
        case _ if value.contains(GimelConstants.COLON) =>
          value
            .split(GimelConstants.COMMA)
            .flatMap(_.split(GimelConstants.COLON) match {
              case a if a.length == 2 =>
                Try(new ColumnClass(a(0).toInt, a(1))).toOption
              case _ => None
            })
        case _ => Seq.empty
      }
    } else {
      Seq.empty
    }
  }

}
