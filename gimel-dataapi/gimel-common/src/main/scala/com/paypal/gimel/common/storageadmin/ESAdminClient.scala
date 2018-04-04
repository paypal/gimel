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

package com.paypal.gimel.common.storageadmin

import org.apache.http.client.methods._
import org.apache.http.impl.client.DefaultHttpClient

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.logger.Logger

object ESAdminClient {

  val logger = Logger()

  /**
    * Deletes ES Index
    *
    * @param url Delete ES Index
    */
  def deleteIndex(url: String): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    logger.info("delete index")
    logger.info(url)
    try {
      val client = new DefaultHttpClient()
      val delete = new HttpDelete(url)
      delete.setHeader("Accept", "application/json")
      val httpResponse = client.execute(delete)
      logger.info(s"Response is --> ${httpResponse.getStatusLine}")
      val response = httpResponse.getStatusLine.getStatusCode
      if (response != GimelConstants.HTTP_SUCCESS_STATUS_CODE) {
        logger.error(s"Unable to delete the data: API did not return 200 Status Message ")
      }
      logger.info(response.toString)
      client.close()
      "Success"
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to delete ES Index")
        throw ex
    }
  }
}
