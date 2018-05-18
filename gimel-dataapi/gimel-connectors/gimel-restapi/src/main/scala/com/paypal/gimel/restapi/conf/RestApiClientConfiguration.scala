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

package com.paypal.gimel.restapi.conf

import scala.collection.immutable.Map
import scala.language.implicitConversions

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.{GimelConstants, GimelProperties}
import com.paypal.gimel.logger.Logger

/**
  * Gimel Client Configuration for RestAPI Dataset Operations.
  *
  * @param props RestAPI Client properties.
  */
class RestApiClientConfiguration(val props: Map[String, Any]) {

  private val logger = Logger()
  logger.info(s"Begin Building --> ${this.getClass.getName}")
  logger.info(s"Incoming Properties --> ${props.map(x => s"${x._1} -> ${x._2}").mkString("\n")}")

  // Load Default Prop from Resource File
  val pcatProps: GimelProperties = GimelProperties()

  // appTag is used to maintain various other factors that are unique to the application
  val appTag: String = props.getOrElse(GimelConstants.APP_TAG, "").toString

  // This is the DataSet Properties
  val dataSetProps: DataSetProperties = props(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
  val tableProps: Map[String, String] = dataSetProps.props
  //  val urlPattern: String = props.getOrElse(RestApiConfigs.urlPattern, tableProps.getOrElse(RestApiConfigs.urlPattern, "")).toString
  val writeMode: String = props.getOrElse(RestApiConfigs.writeMode, tableProps.getOrElse(RestApiConfigs.writeMode, RestApiConstants.writeModePost)).toString
  val parsePayloadFlag: Boolean = props.getOrElse(RestApiConfigs.parsePayloadFlag, tableProps.getOrElse(RestApiConfigs.parsePayloadFlag, RestApiConstants.falseString)).toString.toBoolean
  val usePayloadFlag: Boolean = props.getOrElse(RestApiConfigs.usePayloadFlag, tableProps.getOrElse(RestApiConfigs.usePayloadFlag, RestApiConstants.falseString)).toString.toBoolean
  val allProps: Map[String, String] = tableProps ++ props.map { x => (x._1, x._2.toString) }
  val payloadFieldName: String = RestApiConstants.payloadString
  lazy val urlFromProps: String = resolveUrl(allProps)
  // Get hardcoded URL if available then fetch in following order of priority :
  // 1. from Run time props,
  // 2. from  getting from DataSetProps,
  // 3. else use the pattern approach - which is the typical use case.
  val resolvedUrl = new java.net.URL(props.getOrElse(RestApiConfigs.url, tableProps.getOrElse(RestApiConfigs.url, urlFromProps)).toString)
  val httpsFlag: Boolean = resolvedUrl.getProtocol == "https"

  logger.info(s"Fields Initiated --> ${this.getClass.getFields.map(f => s"${f.getName} --> ${f.get().toString}").mkString("\n")}")
  logger.info(s"Completed Building --> ${this.getClass.getName}")

  /**
    * Resolves all the parameters in PCatalog Properties, returns a URL that can be used for API calls
    *
    * @param props All the properties required to resolve a URL
    * @return Complete URL String for API calls
    */
  def resolveUrl(props: Map[String, String]): String = {

    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val criteriaKey: String = props(RestApiConfigs.urlPattern)
    val urlFromMap: String = props(criteriaKey)
    //    val propsToReplace: Map[String, String] = props.map { x =>
    //      val resolvedKey = if (x._1.startsWith(RestApiConstants.restApiPatternString)) x._1.stripPrefix("gimel.restapi.") else x._1
    //      (resolvedKey, x._2)
    //    }
    logger.info(s"Props are -->")
    props.foreach(println)
    val resolvedUrl: String = props.foldLeft(urlFromMap) { (url, eachProp) =>
      url.replaceAllLiterally(s"{${eachProp._1}}", eachProp._2)
    }
    logger.info(s"resolved URL is --> ${resolvedUrl}")
    resolvedUrl
  }

}

