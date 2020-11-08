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

package com.paypal.gimel.druid.conf

import scala.collection.immutable.Map
import scala.reflect.ClassTag

import com.metamx.common.Granularity

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.druid.model.{DruidDimension, DruidMetric}
import com.paypal.gimel.druid.util.DruidUtility

/**
  * DruidClientConfiguration Class. Takes a map of properties and build its own properties.
  * This Class extends Serializable as it is needed to be passed to the executors.
  *
  * @param props Map[String, Any] of the properties specified by the user.
  */
@SerialVersionUID(100L)
class DruidClientConfiguration(props: Map[String, Any]) extends Serializable {

  // This is the DataSet Properties
  val datasetProps: DataSetProperties = props(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
  val tableProps: Map[String, String] = datasetProps.props

  val druidLoadType: String = fetchProperty[String](DruidConfigs.LOAD_TYPE)
    .getOrElse(DruidConstants.REALTIME_LOAD)

  // Zookeeper services running. Example: localhost:2121. Required Configuration
  val zookeeper: String = fetchProperty[String](DruidConfigs.ZOOKEEPER, isRequired = true).get

  // Index Service as specified for druid cluster. Required Configuration
  val indexService: String = fetchProperty[String](DruidConfigs.INDEX_SERVICE, isRequired = true).get

  // Duscovery Path as specified for druid cluster. Required Configuration
  val discoveryPath: String = fetchProperty[String](DruidConfigs.DISCOVERY_PATH, isRequired = true).get

  // Datasource in Druid to index for. Required Configuration
  val datasource: String = fetchProperty[String](DruidConfigs.DATASOURCE, isRequired = true).get

  val fieldNames: List[String] =
    fetchProperty[List[String]](DruidConfigs.FIELDS, isRequired = true).get

  val timestamp_field: String =
    fetchProperty[String](DruidConfigs.TIMESTAMP)
      .getOrElse(DruidConstants.TIMESTAMP_FIELD_NAME)

  val timestamp_format: String = fetchProperty[String](DruidConfigs.TIMESTAMP_FORMAT)
    .getOrElse(DruidConstants.TIMESTAMP_FORMAT)

  // Get Segment Granularity String from the props and convert it into com.metamx.common.Granularity
  val segmentGranularity: Granularity = {
    val granularityString = fetchProperty[String](DruidConfigs.SEGMENT_GRANULARITY)
      .getOrElse(DruidConstants.SEGMENT_GRANULARITY_FIFTEEN_MINUTE)

    val granularity = Granularity.values.find(g => granularityString.equalsIgnoreCase(g.toString))

    // If given Granularity is not found then throw an Error
    if (granularity.isEmpty) {
      val errorMsg = s"Specified Segment Granularity $granularityString is not a valid Granularity"
      throw new IllegalArgumentException(errorMsg)
    }

    granularity.get
  }

  // Get Segment Granularity String from the props and convert it into com.metamx.common.Granularity
  val queryGranularity: String = {
    fetchProperty[String](DruidConfigs.QUERY_GRANULARITY)
      .getOrElse(DruidConstants.QUERY_GRANULARITY_ONE_MINUTE)
  }

  // Window Period for which druid will accept the incoming data. Defaults to PT10M
  val windowPeriod: String = fetchProperty[String](DruidConfigs.WINDOW_PERIOD).getOrElse(DruidConstants.WINDOW_PERIOD)

  // Number of Partitions Defined
  val numPartitions: Int = fetchProperty[Int](DruidConfigs.PARTITIONS).getOrElse(DruidConstants.PARTITIONS)

  // Number of Replicants Specified
  val numReplicants: Int = fetchProperty[Int](DruidConfigs.REPLICANTS).getOrElse(DruidConstants.REPLICANTS)

  val ARROW = DruidConstants.ARROW
  val NEW_LINE = DruidConstants.NEW_LINE

  // Get List of Druid Field names from props that is a string value for the list.
  lazy val fields: List[DruidDimension] = fieldNames.map(DruidDimension(_))

  // Get List of Druid Dimensions from props that is a string value for the list.
  lazy val dimensions: List[DruidDimension] = {
    errorIfMissing(DruidConfigs.DIMENSIONS)

    DruidUtility.parseString[List[String]](
      fetchProperty[String](DruidConfigs.DIMENSIONS, isRequired = true).get
    ).map(DruidDimension(_))
  }

  // Get List of Druid Metric from props that is a string value for the list.
  lazy val metrics: List[DruidMetric] = {
    val metricString = fetchProperty[String](DruidConfigs.METRICS)

    // Check if metricString is not null or else return a Default count Metric
    if (metricString.isDefined) {
      DruidUtility.parseString[List[DruidMetric]](metricString.get)
    } else {
      List(DruidMetric.getDefaultMetric)
    }
  }

  /**
    * Private Method to check if the key exists in the props.
    * If the key doesnt exists than throw an error.
    *
    * @param key String value for the key
    */
  private def errorIfMissing(key: String): Unit = {
    if (tableProps.get(key).isEmpty && props.get(key).isEmpty) {
      val errorMsg = s"Missing Property: $key for the Druid Client Configuration!"
      throw new IllegalArgumentException(errorMsg)
    }
  }

  /**
    * Method to fetch property value from props and tableProps.
    * This methods first looks for a key in props, if not than looks for in tableProps.
    *
    * @param key Key of the property to be fetched
    * @param isRequired If the key is required or not.
    *                   If it is required than it throws an error if the key
    *                   does not exist in either of props or tableProps
    * @tparam T Type of the value to return for a given property
    * @return An Option of the value or None if the property key does not exist.
    */
  def fetchProperty[T](key: String, isRequired: Boolean = false)
                      (implicit tag: ClassTag[T]): Option[T] = {
    // If isRequired is true, than throw an error if the key is missing
    if (isRequired) errorIfMissing(key)

    val propValue = props.get(key).orElse(tableProps.get(key))

    if (propValue.isDefined) {
      propValue.get match {
        case _: T =>
          Option(propValue.get.asInstanceOf[T])
        case _ =>
          val errorMsg = s"Value for Property Key: $key cannot be cast."
          throw new IllegalArgumentException(errorMsg)
      }
    } else None
  }

  /**
    * Overriden Method to Print Configuration Variables for this config.
    *
    * @return Print message for this Coniguration.
    */
  override def toString: String = {
    var message = "Druid Client Configuration Parameters --->" + DruidConstants.NEW_LINE

    message += DruidConfigs.ZOOKEEPER + ARROW + this.zookeeper + NEW_LINE
    message += DruidConfigs.INDEX_SERVICE + ARROW + this.indexService + NEW_LINE
    message += DruidConfigs.DISCOVERY_PATH + ARROW + this.discoveryPath + NEW_LINE
    message += DruidConfigs.DATASOURCE + ARROW + this.datasource + NEW_LINE
    message += DruidConfigs.FIELDS + ARROW + this.fieldNames + NEW_LINE
    message += DruidConfigs.DIMENSIONS + ARROW +
      this.dimensions.map(_.name).mkString(",") + NEW_LINE
    message += DruidConfigs.METRICS + ARROW + this.metrics.mkString(",") + NEW_LINE
    message += DruidConfigs.TIMESTAMP + ARROW + this.timestamp_field + NEW_LINE
    message += DruidConfigs.TIMESTAMP_FORMAT + ARROW +
      this.timestamp_format + NEW_LINE
    message += DruidConfigs.QUERY_GRANULARITY + ARROW +
      this.queryGranularity + NEW_LINE
    message += DruidConfigs.SEGMENT_GRANULARITY + ARROW +
      this.segmentGranularity + NEW_LINE
    message += DruidConfigs.WINDOW_PERIOD + ARROW + this.windowPeriod + NEW_LINE
    message += DruidConfigs.PARTITIONS + ARROW + this.numPartitions + NEW_LINE
    message += DruidConfigs.REPLICANTS + ARROW + this.numReplicants + NEW_LINE
    message += DruidConfigs.LOAD_TYPE + ARROW + this.druidLoadType + NEW_LINE

    message
  }
}
