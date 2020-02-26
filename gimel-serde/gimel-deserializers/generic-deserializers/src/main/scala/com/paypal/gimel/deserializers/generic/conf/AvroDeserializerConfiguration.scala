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

package com.paypal.gimel.deserializers.generic.conf

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.language.implicitConversions

import com.paypal.gimel.serde.common.schema.SchemaRegistryLookUp

/**
  * Gimel Configuration for Avro Deserializer.
  *
  * @param props Avro Deserializer properties.
  */
class AvroDeserializerConfiguration(val props: Map[String, Any]) extends Serializable {

  val avroSchemaSource: String = props.getOrElse(GenericDeserializerConfigs.avroSchemaSourceKey, props.getOrElse(GenericDeserializerConfigs.avroSchemaSourceKafka1, "INLINE")).toString
  val avroSchemaURL: String = props.getOrElse(GenericDeserializerConfigs.avroSchemaSourceUrlKey, props.getOrElse(GenericDeserializerConfigs.avroSchemaSourceUrlKafka1, GenericDeserializerConstants.confluentSchemaURL)).toString
  val avroSchemaSubject: String = props.getOrElse(GenericDeserializerConfigs.avroSchemaSubjectKey, props.getOrElse(GenericDeserializerConfigs.avroSchemaSourceKeyKafka1, "")).toString
  val (avroSchemaLatest, avroSchemasSubject) =
  avroSchemaSource.toUpperCase() match {
    case GenericDeserializerConstants.avroSchemaCSR =>
      if (avroSchemaSubject.isEmpty) {
        throw new IllegalArgumentException (
          s"""
             | You need to provide schema subject with schema source ${GenericDeserializerConstants.avroSchemaCSR}.
             | Please set ${GenericDeserializerConfigs.avroSchemaSubjectKey} property.
           """.stripMargin)
      }
      val allSchemasSubject: (String, mutable.Map[Int, String]) =
        SchemaRegistryLookUp.getAllSchemasForSubject(avroSchemaSubject, avroSchemaURL)
      (allSchemasSubject._1, allSchemasSubject._2)
    case GenericDeserializerConstants.avroSchemaInline =>
      val avroSchemaString = props.getOrElse(GenericDeserializerConfigs.avroSchemaStringKey, props.getOrElse(GenericDeserializerConfigs.avroSchemaStringKeyKafka1, "")).toString
      if (avroSchemaString.isEmpty) {
        throw new IllegalArgumentException (
          s"""
             | You need to provide avro schema string with schema source ${GenericDeserializerConstants.avroSchemaInline}.
             | Please set ${GenericDeserializerConfigs.avroSchemaStringKey} property.
           """.stripMargin)
      }
      (avroSchemaString, None)
    case _ =>
      throw new IllegalArgumentException (
        s"""
           | Unknown value of Schema Source --> $avroSchemaSource.
           | Please set ${GenericDeserializerConfigs.avroSchemaSourceKey} property with either "${GenericDeserializerConstants.avroSchemaInline}" or "${GenericDeserializerConstants.avroSchemaCSR}" (Confluent Schema Registry).
           """.stripMargin)
  }
}
