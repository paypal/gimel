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

import com.paypal.gimel.common.conf.GimelConstants

class SQLParseDDLSpec
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

  describe("DROP TABLE TEMP TABLE") {
    it("It should return true") {

      GimelQueryUtils.isDropTableATempTable("DROP TABLE basu", spark) should be(
        false
      )
    }
  }

  describe("DROP TABLE with IF exists") {
    it("It should return true") {

      GimelQueryUtils.isDDL(SQLMasterList.dropIfExistsDDL, spark) should be(
        true
      )
    }
  }

  describe("DROP TABLE without IF exists") {
    it("It should return true") {

      GimelQueryUtils.isDDL(SQLMasterList.dropPlainDDL, spark) should be(true)
    }
  }

  describe("DROP view with IF exists") {
    it("It should return true") {

      GimelQueryUtils.isDDL(SQLMasterList.dropIfExistsViewDDL, spark) should be(
        true
      )
    }
  }

  describe("DROP view without IF exists") {
    it("It should return true") {

      GimelQueryUtils.isDDL(SQLMasterList.dropPlainViewDDL, spark) should be(
        true
      )
    }
  }

  describe("truncate table") {
    it("It should return true") {
      GimelQueryUtils.isDDL(SQLMasterList.truncateTableDDL, spark) should be(
        true
      )
    }
  }

  describe("Complex Create External table") {
    it("It should return true") {

      GimelQueryUtils.isDDL(SQLMasterList.complexCreateDDL, spark) should be(
        true
      )
    }
  }

  describe("createTablePattern") {
    it("It should return true") {

      GimelQueryUtils.isUDCDataDefinition(SQLMasterList.createTablePattern) should be(
        true
      )
    }
  }

  describe("createExternalTablePattern") {
    it("It should return true") {

      GimelQueryUtils.isUDCDataDefinition(
        SQLMasterList.createExternalTablePattern
      ) should be(true)
    }
  }

  describe("multisetPattern") {
    it("It should return true") {

      GimelQueryUtils.isUDCDataDefinition(SQLMasterList.multisetPattern) should be(
        true
      )
    }
  }

  describe("setPattern") {
    it("It should return true") {

      GimelQueryUtils.isUDCDataDefinition(SQLMasterList.setPattern) should be(
        true
      )
    }
  }

  describe("dropTablePattern") {
    it("It should return true") {

      GimelQueryUtils.isUDCDataDefinition(SQLMasterList.dropTablePattern) should be(
        true
      )
    }
  }

  describe("truncateTablePattern") {
    it("It should return true") {

      GimelQueryUtils.isUDCDataDefinition(SQLMasterList.truncateTablePattern) should be(
        true
      )
    }
  }

  describe("deleteFromPattern") {
    it("It should return true") {

      GimelQueryUtils.isUDCDataDefinition(SQLMasterList.deleteFromPattern) should be(
        true
      )
    }
  }

  describe("deletePattern") {
    it("It should return true") {

      GimelQueryUtils.isUDCDataDefinition(SQLMasterList.deletePattern) should be(
        true
      )
    }
  }

  describe(s"${GimelConstants.DO_NOT_LOG_CONF} - default behavior") {
    it("It should return return the right filter flag (true | false)") {

      spark.conf.unset(GimelConstants.DO_NOT_LOG_CONF)

      GimelQueryUtils.doNotLog("set gimel.bigquery.refresh.token=XXX", spark) should be(
        true
      )
      GimelQueryUtils.doNotLog("set gimel.jdbc.password=XXX", spark) should be(
        true
      )
      GimelQueryUtils.doNotLog("set gimel.jdbc.user=XXX", spark) should be(
        false
      )
    }
  }

  describe(s"${GimelConstants.DO_NOT_LOG_CONF} - runtime behavior") {
    it("It should return return the right filter flag (true | false)") {

      // User supplied behavior
      spark.sql(s"set ${GimelConstants.DO_NOT_LOG_CONF}=gimel.jdbc.user,gimel.some.other.key")

      GimelQueryUtils.doNotLog("set gimel.bigquery.refresh.token=XXX", spark) should be(
        false
      )
      GimelQueryUtils.doNotLog("set gimel.jdbc.password=XXX", spark) should be(
        false
      )
      GimelQueryUtils.doNotLog("set gimel.jdbc.user=XXX", spark) should be(
        true
      )
      GimelQueryUtils.doNotLog("set gimel.some.other.key=XXX", spark) should be(
        true
      )
      GimelQueryUtils.doNotLog("set gimel.one.more.key=XXX", spark) should be(
        false
      )

      // Switch back to default behavior in the code
      spark.conf.unset("gimel.logging.no.audit.props")

      GimelQueryUtils.doNotLog("set gimel.bigquery.refresh.token=XXX", spark) should be(
        true
      )
      GimelQueryUtils.doNotLog("set gimel.jdbc.password=XXX", spark) should be(
        true
      )
      GimelQueryUtils.doNotLog("set gimel.jdbc.user=XXX", spark) should be(
        false
      )
    }
  }


}
