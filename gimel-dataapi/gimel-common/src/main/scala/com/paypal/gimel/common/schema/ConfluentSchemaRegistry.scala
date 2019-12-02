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

package com.paypal.gimel.common.schema

import io.confluent.kafka.schemaregistry.client.rest.RestService
import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.collection.mutable
import scala.language.implicitConversions

/**
  * Extending RestService for Confluent Schema Registry Operations
  *
  * @param server Confluent Schema URL
  */
class ConfluentSchemaRegistry(server: String) extends RestService(server)

object SchemaRegistryLookUp {

  def getAllSubjectAndSchema(schemaURL: String): Map[String, (String, mutable.Map[Int, String])] = {

    try {
      val schemaRegistryClient = new ConfluentSchemaRegistry(schemaURL)
      val allSubjects = schemaRegistryClient.getAllSubjects.asScala
      val allSchemas = allSubjects.map {
        eachSubject =>
          val eachSubjectSchemas = getAllSchemasForSubject(eachSubject, schemaURL)
          (eachSubject, eachSubjectSchemas)
      }.toMap
      allSchemas
    }
    catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw new Exception("Unable to read schemas from Schema Registry -->" + schemaURL + "\n" + ex)
    }
  }

  def getAllSchemasForSubject(schemaSubject: String, avroSchemaURL: String): (String, mutable.Map[Int, String]) = {

    try {
      val schemaLookup: scala.collection.mutable.Map[Int, String] = scala.collection.mutable.Map()
      val schemaRegistryClient = new ConfluentSchemaRegistry(avroSchemaURL)
      val k = schemaRegistryClient.getAllVersions(schemaSubject).asScala
      val k2 = k.map { eachVersion =>
        val version = eachVersion.toString.toInt
        version -> schemaRegistryClient.getVersion(schemaSubject, version).getSchema
      }.toMap
      k2.foreach(entry => schemaLookup.put(entry._1, entry._2))
      val latestSchema = schemaRegistryClient.getLatestVersion(schemaSubject).getSchema
      (latestSchema, schemaLookup)
    }
    catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw new Exception("Unable to read schema subject '" + schemaSubject + "' from Schema Registry -->" + avroSchemaURL + "\n" + ex)
    }
  }

}
