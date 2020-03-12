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

package com.paypal.gimel.common.gimelserde

import scala.collection.immutable.Map

import org.apache.spark.sql.SparkSession

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.utilities.GenericUtils
import com.paypal.gimel.logger.Logger

case class GimelSerDe(deSerializer : String, serializer : String)

object GimelSerdeUtils {

  val logger: Logger = Logger(this.getClass.getName)

  /**
    * Serializer Class Loader
    *
    * @param className
    * @return DataFrame after deserialization
    */
  def getSerializerObject(className: String): com.paypal.gimel.serde.common.Serializer = {
    val serializerConstructor = Class.forName(className).getConstructor()
    val serializerObj = serializerConstructor.newInstance().asInstanceOf[com.paypal.gimel.serde.common.Serializer]
    serializerObj
  }

  /**
    * Deserializer Class Loader
    *
    * @param className
    * @return DataFrame after deserialization
    */
  def getDeserializerObject(className: String): com.paypal.gimel.serde.common.Deserializer = {
    val deserializerConstructor = Class.forName(className).getConstructor()
    val deserializerObj = deserializerConstructor.newInstance().asInstanceOf[com.paypal.gimel.serde.common.Deserializer]
    deserializerObj
  }

  /**
    * Sets the appropriate De/serializer class based on the kafka.message.value.type and value.serializer properties
    * This is mainly required for backward compatibility
    *
    * @param sparkSession
    * @param datasetProps
    * @param isStream
    * @return Map of properties with deserializer class
    */
  def getSerdeClass(sparkSession: SparkSession, datasetProps: DataSetProperties, isStream: Boolean): GimelSerDe = {
    val valueMessageType = datasetProps.props.getOrElse(GimelConstants.KAFKA_MESSAGE_VALUE_TYPE, "string")
    val valueSerializer = datasetProps.props.getOrElse(GimelConstants.SERIALIZER_VALUE, GimelConstants.KAFKA_STRING_SERIALIZER)
    logger.info(s"kafka.message.value.type --> ${valueMessageType} \nValue Serializer --> ${valueSerializer}")
    val avroSchemaSource: String = datasetProps.props.getOrElse(GimelConstants.KAFKA_AVRO_SCHEMA_SOURCE,
      GimelConstants.KAFKA_AVRO_SCHEMA_SOURCE_INLINE)
    (valueMessageType, valueSerializer) match {
      // Bytes Messages
      case ("binary", GimelConstants.KAFKA_BYTE_SERIALIZER) =>
        GimelSerDe(sparkSession.conf.get(GimelConstants.GIMEL_BINARY_DESERIALIZER_CLASS,
          GimelConstants.GIMEL_BINARY_DESERIALIZER_CLASS_DEFAULT),
          "")
      // String Messages
      case ("string", GimelConstants.KAFKA_STRING_SERIALIZER) =>
        GimelSerDe(sparkSession.conf.get(GimelConstants.GIMEL_STRING_DESERIALIZER_CLASS,
          GimelConstants.GIMEL_STRING_DESERIALIZER_CLASS_DEFAULT),
          sparkSession.conf.get(GimelConstants.GIMEL_STRING_SERIALIZER_CLASS,
            GimelConstants.GIMEL_STRING_SERIALIZER_CLASS_DEFAULT))
      // JSON Messages
      case ("json", GimelConstants.KAFKA_STRING_SERIALIZER) =>
        isStream match {
          case true =>
            GimelSerDe(sparkSession.conf.get(GimelConstants.GIMEL_JSON_STATIC_DESERIALIZER_CLASS,
              GimelConstants.GIMEL_JSON_STATIC_DESERIALIZER_CLASS_DEFAULT),
              "")
          case false =>
            GimelSerDe(sparkSession.conf.get(GimelConstants.GIMEL_JSON_DYNAMIC_DESERIALIZER_CLASS,
              GimelConstants.GIMEL_JSON_DYNAMIC_DESERIALIZER_CLASS_DEFAULT),
            sparkSession.conf.get(GimelConstants.GIMEL_JSON_SERIALIZER_CLASS,
              GimelConstants.GIMEL_JSON_SERIALIZER_CLASS_DEFAULT))
          }
      // Avro - CDH | Generic Avro
      case (_, GimelConstants.KAFKA_BYTE_SERIALIZER) =>
        avroSchemaSource.toUpperCase() match {
          case GimelConstants.KAFKA_AVRO_SCHEMA_SOURCE_CDH => {
            GimelSerDe(sparkSession.conf.get(GimelConstants.GIMEL_CDH_DESERIALIZER_CLASS,
              GimelConstants.GIMEL_CDH_DESERIALIZER_CLASS_DEFAULT),
              "")
          }
          case _ =>
            GimelSerDe(sparkSession.conf.get(GimelConstants.GIMEL_AVRO_DESERIALIZER_CLASS,
                GimelConstants.GIMEL_AVRO_DESERIALIZER_CLASS_DEFAULT),
              sparkSession.conf.get(GimelConstants.GIMEL_AVRO_SERIALIZER_CLASS,
                GimelConstants.GIMEL_AVRO_SERIALIZER_CLASS_DEFAULT))
        }
      // Other Types
      case _ =>
        logger.warning("Unsupported/Empty Configuration or Deserialization Technique.")
        GimelSerDe("", "")
    }
  }

  /**
    * Sets the appropriate Deserializer class based on the kafka.message.value.type and value.serializer properties
    * This is mainly required for backward compatibility
    *
    * @param sparkSession
    * @param datasetProps
    * @param options
    * @param isStream
    * @return Map of properties with deserializer class
    */
  def setGimelDeserializer(sparkSession: SparkSession, datasetProps: DataSetProperties,
                           options: Map[String, String], isStream: Boolean = false) : Map[String, String] = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val kafkaApiVersion = GenericUtils.getValue(datasetProps.props,
      GimelConstants.GIMEL_KAFKA_VERSION, defaultValue = GimelConstants.GIMEL_KAFKA_DEFAULT_VERSION)

    (datasetProps.datasetType != "KAFKA" || kafkaApiVersion.equals(GimelConstants.GIMEL_KAFKA_VERSION_ONE)) match {
      case true =>
        options
      case false =>
        val deserializerClass = datasetProps.props.getOrElse(GimelConstants.GIMEL_DESERIALIZER_CLASS,
          getSerdeClass(sparkSession, datasetProps, isStream).deSerializer)
        deserializerClass.isEmpty match {
          case true =>
            options
          case false =>
            options ++
              Map(GimelConstants.GIMEL_DESERIALIZER_CLASS -> deserializerClass)
        }
    }
  }

  /**
    * Sets the appropriate Serializer class based on the kafka.message.value.type and value.serializer properties
    * This is mainly required for backward compatibility
    *
    * @param sparkSession
    * @param datasetProps
    * @param options
    * @param isStream
    * @return Map of properties with serializer class
    */
  def setGimelSerializer(sparkSession: SparkSession, datasetProps: DataSetProperties,
                         options: Map[String, String], isStream: Boolean = false) : Map[String, String] = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val kafkaApiVersion = GenericUtils.getValue(datasetProps.props,
      GimelConstants.GIMEL_KAFKA_VERSION, defaultValue = GimelConstants.GIMEL_KAFKA_DEFAULT_VERSION)

    (datasetProps.datasetType != "KAFKA" || kafkaApiVersion.equals(GimelConstants.GIMEL_KAFKA_VERSION_ONE)) match {
      case true =>
        options
      case false =>
        val serializerClass = datasetProps.props.getOrElse(GimelConstants.GIMEL_SERIALIZER_CLASS,
          getSerdeClass(sparkSession, datasetProps, isStream).serializer)
        serializerClass.isEmpty match {
          case true =>
            options
          case false =>
            options ++
              Map(GimelConstants.GIMEL_SERIALIZER_CLASS -> serializerClass)
        }
    }
  }
}
