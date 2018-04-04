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

class SQLParserSpec extends FunSpec with Matchers {


  it("should pick the Target Table Accurately from the SQL without 'table' keyword") {

    com.paypal.gimel.sql.SQLParser.getTargetTables(
      """
        |INSERT INTO data_source_tab2 PARTITION (p1 = 'part1', p2)
        |  SELECT id, 'part' || id FROM RANGE(1, 3)
        |
      """.stripMargin) shouldBe Some("data_source_tab2")

  }

  it("should pick the Target Table Accurately from the SQL with 'table' keyword") {
    com.paypal.gimel.sql.SQLParser.getTargetTables(
      """
        |INSERT INTO table data_source_tab2 PARTITION (p1 = 'part1', p2)
        |  SELECT id, 'part' || id FROM RANGE(1, 3)
        |
      """.stripMargin) shouldBe Some("data_source_tab2")

  }

  it("should pick the Target Table Accurately from the SQL when there is an 'override' keyword") {
    com.paypal.gimel.sql.SQLParser.getTargetTables(
      """
        |INSERT OVERWRITE TABLE data_source_tab2 PARTITION (p1 = 'partNew1', p2)
        |  VALUES (3, 'partNew2')
      """.stripMargin) shouldBe Some("data_source_tab2")

  }

  it("should pick the Target Table Accurately from the SQL when there is an 'override' keyword 1") {
    com.paypal.gimel.sql.SQLParser.getTargetTables(
      """
        |INSERT OVERWRITE TABLE data_source_tab2 PARTITION (p1 = 'part1', p2)
        |  VALUES (5, 'part1')
      """.stripMargin) shouldBe Some("data_source_tab2")
  }

  it("should pick the Target Table Accurately from the SQL when the SQL has a DB.Table format") {
    com.paypal.gimel.sql.SQLParser.getTargetTables(
      """insert into pcatalog.elastic_cluster_flights_log_notebook_data
        |select * from pcatalog.kafka_flights_log
        |
      """.stripMargin) shouldBe Some("pcatalog.elastic_cluster_flights_log_notebook_data")
  }
}
