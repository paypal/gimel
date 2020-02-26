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

package com.paypal.gimel.serializers.generic.conf

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.language.implicitConversions

import com.paypal.gimel.serde.common.schema.SchemaRegistryLookUp

/**
  * Gimel Configuration for Avro Deserializer.
  *
  * @param props Avro Serializer properties.
  */
class AvroSerializerConfiguration(val props: Map[String, Any]) extends Serializable {

  val avroSchemaSource: String = props.getOrElse(GenericSerializerConfigs.avroSchemaSourceKey, props.getOrElse(GenericSerializerConfigs.avroSchemaSourceKafka1, "INLINE")).toString
  val avroSchemaURL: String = props.getOrElse(GenericSerializerConfigs.avroSchemaSourceUrlKey, props.getOrElse(GenericSerializerConfigs.avroSchemaSourceUrlKafka1, GenericSerializerConstants.confluentSchemaURL)).toString
  val avroSchemaSubject: String = props.getOrElse(GenericSerializerConfigs.avroSchemaSubjectKey, props.getOrElse(GenericSerializerConfigs.avroSchemaSourceKeyKafka1, "")).toString
  val (avroSchemaLatest, avroSchemasSubject) =
  avroSchemaSource.toUpperCase() match {
    case GenericSerializerConstants.avroSchemaCSR =>
      if (avroSchemaSubject.isEmpty) {
        throw new IllegalArgumentException (
          s"""
             | You need to provide schema subject with schema source ${GenericSerializerConstants.avroSchemaCSR}.
             | Please set ${GenericSerializerConfigs.avroSchemaSubjectKey} property.
           """.stripMargin)
      }
      val allSchemasSubject: (String, mutable.Map[Int, String]) =
        SchemaRegistryLookUp.getAllSchemasForSubject(avroSchemaSubject, avroSchemaURL)
      (allSchemasSubject._1, allSchemasSubject._2)
    case GenericSerializerConstants.avroSchemaInline =>
      val avroSchemaString = props.getOrElse(GenericSerializerConfigs.avroSchemaStringKey, props.getOrElse(GenericSerializerConfigs.avroSchemaStringKeyKafka1, "")).toString
      if (avroSchemaString.isEmpty) {
        throw new IllegalArgumentException (
          s"""
             | You need to provide avro schema string with schema source ${GenericSerializerConstants.avroSchemaInline}.
             | Please set ${GenericSerializerConfigs.avroSchemaStringKey} property.
           """.stripMargin)
      }
      (avroSchemaString, None)
    case _ =>
      throw new IllegalArgumentException (
        s"""
           | Unknown value of Schema Source --> $avroSchemaSource.
           | Please set ${GenericSerializerConfigs.avroSchemaSourceKey} property with either "${GenericSerializerConstants.avroSchemaInline}" or "${GenericSerializerConstants.avroSchemaCSR}" (Confluent Schema Registry).
           """.stripMargin)
  }
}
