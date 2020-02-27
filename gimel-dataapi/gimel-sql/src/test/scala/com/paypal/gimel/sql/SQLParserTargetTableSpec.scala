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

import org.scalatest.{FunSpec, Matchers}

import com.paypal.gimel.sql.SQLMasterList._

class SQLParserTargetTableSpec extends FunSpec with Matchers {


  describe("getTargetTables") {

    it("should pick the TARGET Table Accurately from the SQL without 'table' keyword") {
      SQLParser.getTargetTables(insertSQL1) shouldBe Some("data_source_tab2")
    }

    it("should pick the TARGET Table Accurately from the SQL with 'table' keyword") {
      SQLParser.getTargetTables(insertSQL2) shouldBe Some("data_source_tab2")
    }

    it("should pick the TARGET Table Accurately from the SQL when there is an 'override' keyword") {
      SQLParser.getTargetTables(insertSQL3) shouldBe Some("data_source_tab2")
    }

    it("should pick the TARGET Table Accurately from the SQL when there is an 'override' keyword 1") {
      SQLParser.getTargetTables(insertSQL4) shouldBe Some("data_source_tab2")
    }

    it("should pick the TARGET Table Accurately from the SQL when the SQL has a DB.Table format") {
      SQLParser.getTargetTables(insertSQL5) shouldBe Some("pcatalog.elastic_test_cluster_yelp_review_data")
    }

    it("should pick correct table name from the SELECT QUERY") {
      GimelQueryUtils.getTablesFrom(simpleSelect1) should equal(Array("udc.mysql.datalake.test.yelp_review_read"))
    }

    it("should pick proper table name from the only the SELECT Query") {
      GimelQueryUtils.getTablesFrom(simpleInsertSelect1) should equal(Array("udc.kafka.tau.yelp.review",
        "udc.mysql.datalake.test.yelp_review_write"))
    }
  }


  describe("isQueryContainingPartitioning") {

    it("should return true if query contains ; insert into partitions of target table. ") {
      GimelQueryUtils.isQueryContainingPartitioning(insertPartitionedTable1) shouldBe (true)
      GimelQueryUtils.isQueryContainingPartitioning(insertSQL1) shouldBe (true)
      GimelQueryUtils.isQueryContainingPartitioning(insertSQL2) shouldBe (true)
      GimelQueryUtils.isQueryContainingPartitioning(insertSQL3) shouldBe (true)
      GimelQueryUtils.isQueryContainingPartitioning(insertSQL4) shouldBe (true)
    }

    it("should return false if query does not contain partition") {
      GimelQueryUtils.isQueryContainingPartitioning(insertSQL5) shouldBe (false)
      GimelQueryUtils.isQueryContainingPartitioning(insertSQL6) shouldBe (false)
    }
  }
}
