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

import org.scalatest.{FunSpec, Matchers}

import com.paypal.gimel.common.catalog.DataSetProperties

class BindToFieldsUtilsTest extends FunSpec with Matchers {
  describe ("getFieldsBindTo") {
    it ("should get the array of Field case class given a json of field's name and type") {
      val fieldsBindToString = s"""[{"fieldName":"id","fieldType":"string","defaultValue":""},{"fieldName":"dob","fieldType":"date","defaultValue":"null"},{"fieldName":"age","fieldType":"int","defaultValue":"23"}]"""
      val (fields, fieldsJsonString): (Array[Field], String) =
        BindToFieldsUtils.getFieldsBindTo(fieldsBindToString, "", Map.empty[String, Any])
      println(fields)
      assert (fieldsJsonString == fieldsBindToString)
    }

    it ("should throw error if the given json is not valid") {
      val fieldsBindToString = s"""[{"fieldName":"id","fieldType":"string","defaultValue":""},{"fieldName":"dob","fieldType":"date","defaultValue":"null"},{"fieldName":"age","fieldType":"int","defaultValue":"2]"""
      val exception = intercept[IllegalArgumentException] {
        BindToFieldsUtils.getFieldsBindTo(fieldsBindToString, "", Map.empty[String, Any])
      }
      assert (exception.getMessage.contains("Error in parsing json"))
    }

    it ("should throw error if the given dataset does not have fields in DatasetProperties") {
      val props : Map[String, String] = Map()
      val dataSetProperties = DataSetProperties("KAFKA", null, null, props)
      val exception = intercept[IllegalStateException] {
        BindToFieldsUtils.getFieldsBindTo("", "udc.kafka.gimel_dev.default.test_topic", dataSetProperties)
      }
      assert (exception.getMessage.contains("Dataset does not have fields"))
    }
  }
}
