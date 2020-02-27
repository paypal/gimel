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
package com.paypal.gimel.sql

import org.scalatest.{BeforeAndAfterEach, FunSpec, Matchers}

class GimelQueryUtilsSpec
    extends FunSpec
    with SharedSparkSession
    with Matchers
    with BeforeAndAfterEach {

  // add things to do before each test for this specific files
  protected override def beforeEach(): Unit = {
    GimelQueryUtils.setCatalogProvider("UDC")
  }

  // add things to do after each test for this specific files
  protected override def afterEach(): Unit = {
    GimelQueryUtils.setCatalogProvider("UDC")
  }

  describe("setCatalogProvider") {
    it("should set user specified CatalogProvider") {

      // UDC Catalog provider
      GimelQueryUtils.setCatalogProvider("UDC")
      GimelQueryUtils.getCatalogProvider() should be("UDC")

      GimelQueryUtils.setCatalogProvider("HIVE")
      GimelQueryUtils.getCatalogProvider() should be("HIVE")

      GimelQueryUtils.setCatalogProvider("PCATALOG")
      GimelQueryUtils.getCatalogProvider() should be("PCATALOG")

    }

    it("should throw warning if the catalog provider is not UDC/HIVE/PCATALOG") {
      GimelQueryUtils.setCatalogProvider("TEST")
      GimelQueryUtils.getCatalogProvider() should be("UDC")

    }
  }

  describe("tokenizeSql") {
    it("should tokenize the string passed to it") {
      GimelQueryUtils.tokenizeSql(SQLMasterList.simpleInsertSelect2) should be(
        Array(
          "INSERT",
          "INTO",
          "UDC.Mysql.datalake.test.YELP_REVIEW_WRITE",
          "SELECT",
          "*",
          "FROM",
          "udc.kafka.tau.yelp.review"
        )
      )
    }
  }

}
