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

package com.paypal.gimel.deserializers.generic

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

import com.paypal.gimel.deserializers.generic.conf.{GenericDeserializerConfigs, GenericDeserializerConfiguration, GenericDeserializerConstants}
import com.paypal.gimel.serde.common.Deserializer
import com.paypal.gimel.serde.common.utils.SQLDataTypesUtils

/*
 * Deserializer class for Json data
 */
class JsonStaticDeserializer extends Deserializer {
  override def deserialize(dataframe: DataFrame, props: Map[String, Any] = Map.empty): DataFrame = {
    val conf = new GenericDeserializerConfiguration(props)
    if (!dataframe.columns.contains(conf.columnToDeserialize)) {
      throw new IllegalArgumentException(
        s"""
           | Column to Deserialize does not exist in dataframe --> ${conf.columnToDeserialize}
           | Please set the property ${GenericDeserializerConfigs.columnToDeserializeKey}
           | Note: Default value is "${GenericDeserializerConstants.columnToDeserialize}"
         """.stripMargin
      )
    } else {
      if (conf.fieldsBindToJson.isEmpty) {
        throw new Exception ("You need to provide fields in json by setting " + GenericDeserializerConfigs.fieldsBindToJson + " property.")
      } else {
        val schema = SQLDataTypesUtils.getSchemaFromBindToFieldsJson(conf.fieldsBindToJson)
        dataframe.selectExpr("cast (" + conf.columnToDeserialize + " as string) as json")
          .select(from_json(col("json"), schema).as("data")).select("data.*")
      }
    }
  }
}
