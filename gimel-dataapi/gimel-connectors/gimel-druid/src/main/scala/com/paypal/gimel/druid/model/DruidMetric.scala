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

package com.paypal.gimel.druid.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.druid.query.aggregation.{AggregatorFactory, CountAggregatorFactory, LongSumAggregatorFactory}
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory
import org.json4s._
import org.json4s.FieldSerializer._

import com.paypal.gimel.druid.model.DruidMetric.{MetricFieldNames, MetricTypes}

/**
  * Model class representing a DruidMetric.
  * This case class extends Serializable.
  *
  * @param metricsType Type of the metric to be computed.
  * @param fieldName Name of field to perform aggregation on
  * @param name Name of the metric
  */
@SerialVersionUID(100L)
@JsonIgnoreProperties(ignoreUnknown = true)
case class DruidMetric( metricsType: String, fieldName: String, name: String)
  extends Serializable {

  /**
    * Method to initialize a DruidMetric from a map supplied
    *
    * @param map Map[String, String] having (key -> value) for a given druid metric
    * @return DruidMetric object using values from the map
    */
  def initializeFromMap(map: Map[String, String]): DruidMetric = {
    DruidMetric(map.get(MetricFieldNames.TYPE).orNull,
      map.get(MetricFieldNames.FIELD_NAME).orNull,
      map.get(MetricFieldNames.NAME).orNull)
  }

  /**
    * Converts the given DruidMetric to its corresponding AggregatorFactory
    * Object that is used by Tranquility.
    * Supported MetricTypes - Count, LongSum, HyperUnique
    *
    * @return AggregatorFactory object corresponding to the given Metric Type
    */
  def getAggregator: AggregatorFactory = {
    metricsType match {
      case MetricTypes.LONG_SUM =>
        new LongSumAggregatorFactory(name, fieldName)
      case MetricTypes.COUNT =>
        new CountAggregatorFactory(name)
      case MetricTypes.HYPER_UNIQUE =>
        new HyperUniquesAggregatorFactory(name, fieldName)
      case otherType: String =>
        throw new Exception(s"Metric Type: $otherType is not supported.")
    }
  }
}

object DruidMetric {
  def getDefaultMetric: DruidMetric = {
    DruidMetric(MetricTypes.COUNT, null, MetricTypes.COUNT)
  }

  object MetricFieldNames {
    val TYPE = "type"
    val FIELD_NAME = "field_name"
    val NAME = "name"
  }

  object MetricTypes {
    val LONG_SUM = "longSum"
    val COUNT = "count"
    val HYPER_UNIQUE = "hyperUnique"
  }

  // Deserializer for Druid Metric.
  // Ignore fieldName if does not exists.
  // Rename metricsType -> type, fieldName -> field_name, name -> name
  val drudMetricSerializer: FieldSerializer[DruidMetric] = FieldSerializer[DruidMetric] (
    ignore("fieldName") orElse renameTo("metricsType", MetricFieldNames.TYPE) orElse
      renameTo("fieldName", MetricFieldNames.FIELD_NAME) orElse
      renameTo("name", MetricFieldNames.NAME),
    renameFrom(MetricFieldNames.TYPE, "metricsType") orElse
      renameFrom(MetricFieldNames.FIELD_NAME, "fieldName") orElse
      renameFrom(MetricFieldNames.NAME, "name")
  )
}
