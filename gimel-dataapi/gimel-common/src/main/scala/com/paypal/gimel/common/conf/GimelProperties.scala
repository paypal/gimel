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

package com.paypal.gimel.common.conf

import java.io.{File, FileInputStream}
import java.util.{Calendar, Properties}

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.paypal.gimel.logger.Logger

class GimelProperties(userProps: Map[String, String] = Map[String, String]()) {
  // Get Logger
  val logger = Logger()
  logger.info(s"Initiating --> ${this.getClass.getName}")
  // Get Properties
  val props: mutable.Map[String, String] = getProps
  val runTagUUID: String = java.util.UUID.randomUUID.toString
  val startTimeMS: String = Calendar.getInstance().getTimeInMillis.toString
  val tagToAdd: String = s"_$startTimeMS"

  private def getConf(key: String): String = {
    userProps.getOrElse(key, props(key))
  }

  // Kafka Properties
  val kafkaBroker: String = getConf(GimelConstants.KAFKA_BROKER_LIST)
  val kafkaConsumerCheckPointRoot: String = getConf(GimelConstants.KAFKA_CONSUMER_CHECKPOINT_PATH)
  val kafkaAvroSchemaKey: String = getConf(GimelConstants.KAFKA_CDH_SCHEMA)
  val confluentSchemaURL: String = getConf(GimelConstants.CONFLUENT_SCHEMA_URL)
  val hbaseNameSpace: String = getConf(GimelConstants.HBASE_NAMESPACE)
  val zkHostAndPort: String = getConf(GimelConstants.ZOOKEEPER_LIST)
  val zkPrefix: String = getConf(GimelConstants.ZOOKEEPER_STATE)
  val esHost: String = getConf(GimelConstants.ES_NODE)
  val esPort: String = getConf(GimelConstants.ES_PORT)

  // Kerberos
  val keytab: String = getConf(GimelConstants.KEY_TAB)
  val principal: String = getConf(GimelConstants.KEY_TAB_PRINCIPAL)
  val cluster: String = getConf(GimelConstants.CLUSTER)
  val dataSetDeploymentClusters: String = getConf(GimelConstants.DEPLOYMENT_CLUSTERS)


  val defaultESCluster: String = props(GimelConstants.ES_POLLING_STORAGES)

  def hiveURL(cluster: String): String = {
    userProps.getOrElse(s"gimel.hive.$cluster.url", props(s"gimel.hive.$cluster.url"))
  }

  def esURL(escluster: String): String = {
    val alternateConfig = props(s"gimel.es.${defaultESCluster}.url")
    userProps.getOrElse(GimelConstants.ES_URL_WITH_PORT, alternateConfig)
  }

  /**
    * Returns Properties from the resources file
    *
    * @return mutable.Map[String, String]
    */
  private def getProps: mutable.Map[String, String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val props: Properties = new Properties()
    userProps.contains(GimelConstants.GIMEL_PROPERTIES_FILE_KEY) match {
      case true =>
        val propsFile = new File(userProps.get(GimelConstants.GIMEL_PROPERTIES_FILE_KEY).get)
        if (propsFile.exists()) props.load(new FileInputStream(propsFile))
        props.asScala
      case false =>
        val propsFile = GimelConstants.GIMEL_PROPERTIES_FILE_NAME
        val configStream = this.getClass.getResourceAsStream(propsFile)
        props.load(configStream)
        configStream.close()
        val finalProps: mutable.Map[String, String] = mutable.Map(props.asScala.toSeq: _*)
        finalProps
    }
  }

  logger.info(s"Completed Building --> ${this.getClass.getName}")
}

/**
  * Companion Object
  */
object GimelProperties {

  /**
    * If nothing is supplied from User ; Load all props from file in resources folder
    *
    * @return PCatalogProperties
    */
  def apply(): GimelProperties = new GimelProperties()

  /**
    * Use the properties supplied by user & load the defaults from resources where-ever applicable
    *
    * @param params User Supplied properties as a KV Pair
    * @return PCatalogProperties
    */
  def apply(params: Map[String, String]): GimelProperties = new GimelProperties(params)

}
