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

import com.paypal.gimel.logger.Logger
import com.paypal.gimel.parser.utilities.SQLNonANSIJoinParser
import com.paypal.gimel.sql.SQLMasterList._

class SQLParserSourceTableSpec extends FunSpec with Matchers {

  private val logger = Logger(this.getClass.getName)

  describe("getSourceTablesFromNonAnsi") {
    it("should pick correct table names from the SELECT QUERY") {

      SQLNonANSIJoinParser.getSourceTablesFromNonAnsi(sqlANSIanNonANSI) should equal(
        List("a", "b", "c", "d", "k", "l")
      )
      SQLNonANSIJoinParser.getSourceTablesFromNonAnsi(sqlANSISimple) should equal(
        List("a", "b", "c", "d")
      )
      SQLNonANSIJoinParser.getSourceTablesFromNonAnsi(sqlANSISimpleSubQuery) should equal(
        List("a", "b", "c", "d")
      )
      SQLNonANSIJoinParser.getSourceTablesFromNonAnsi(sqlANSIOnly) should equal(
        List()
      )
      SQLNonANSIJoinParser.getSourceTablesFromNonAnsi(joinANSISQLViaUDC) should equal(
        List()
      )
      SQLNonANSIJoinParser.getSourceTablesFromNonAnsi(mixANSINonANSISQL) should equal(
        List(
          "udc.kafka.test.test.emp",
          "udc.hive.test_cluster.test.calendar",
          "c",
          "udc.teradata.test_cluster.testdb.lkp"
        )
      )
    }
  }

  describe("getAll UDC TableSources") {
    it("should pick correct table names from the SELECT QUERY") {

      GimelQueryUtils.getTablesFrom(sqlANSIanNonANSI) should equal(List())
      GimelQueryUtils.getTablesFrom(sqlANSISimple) should equal(List())
      GimelQueryUtils.getTablesFrom(sqlANSISimpleSubQuery) should equal(List())
      GimelQueryUtils.getTablesFrom(sqlANSIOnly) should equal(List())
      GimelQueryUtils.getTablesFrom(mixANSINonANSISQL) should equal(
        List(
          "udc.kafka.test.test.emp",
          "udc.hive.test_cluster.test.calendar",
          "udc.teradata.test_cluster.testdb.lkp"
        )
      )
      GimelQueryUtils.getTablesFrom(baSQL4) should equal(
        List(
          "pcatalog.teradata.tau.yelp.review",
          "pcatalog.teradata.tau.yelp.user"
        )
      )
      GimelQueryUtils.getTablesFrom(joinANSISQLViaUDC).sorted should equal(
        List(
          "udc.kafka.test.emp.address",
          "udc.kafka.test.emp.loc"
        ).sorted
      )
      GimelQueryUtils.getAllTableSources(joinANSISQLViaUDC).sorted should equal(
        List(
          "testdb.emp",
          "udc.kafka.test.emp.address",
          "udc.kafka.test.emp.loc"
        ).sorted
      )
    }
  }

  describe("get All Source Tables") {
    it("should pick correct table names from the SELECT QUERY") {

      GimelQueryUtils.getAllTableSources(sqlANSIanNonANSI) should equal(
        List("a", "b", "c", "d", "k", "l", "t2")
      )
      GimelQueryUtils.getAllTableSources(sqlANSISimple) should equal(
        List("a", "b", "c", "d")
      )
      GimelQueryUtils.getAllTableSources(sqlANSISimpleSubQuery) should equal(
        List("a", "b", "c", "d")
      )
      GimelQueryUtils.getAllTableSources(sqlANSIOnly) should equal(
        List("emp_loc", "testdb.emp")
      )
      GimelQueryUtils.getAllTableSources(joinANSISQLViaUDC).sorted should equal(
        List(
          "testdb.emp",
          "udc.kafka.test.emp.address",
          "udc.kafka.test.emp.loc"
        ).sorted
      )
      GimelQueryUtils.getAllTableSources(mixANSINonANSISQL).sorted should equal(
        List(
          "testdb.emp",
          "udc.kafka.test.test.emp",
          "udc.hive.test_cluster.test.calendar",
          "c",
          "udc.teradata.test_cluster.testdb.lkp"
        ).sorted
      )
    }
  }

  describe("isSQLNonANSIJoin") {
    it(
      "should pick tell correctly if a SQL is ANSI only or has NON-ANSI joins as well"
    ) {

      SQLNonANSIJoinParser.isSQLNonANSIJoin(sqlANSIanNonANSI) should equal(true)
    }
  }

  describe("All DDL DML type") {
    it("should pick correct table names ") {
      GimelQueryUtils.getAllTableSources(
        "collect statistics on yelp.tmp_table"
      ) should equal(List("yelp.tmp_table"))
      GimelQueryUtils.getAllTableSources("DELETE ALL yelp.tmp_table") should equal(
        List("yelp.tmp_table")
      )
      GimelQueryUtils.getAllTableSources("DELETE yelp.tmp_table ALL") should equal(
        List("yelp.tmp_table")
      )
      GimelQueryUtils.getAllTableSources("DESCRIBE yelp.tmp_table") should equal(
        List("yelp.tmp_table")
      )
      GimelQueryUtils.getAllTableSources("HELP table yelp.tmp_table") should equal(
        List("yelp.tmp_table")
      )
      GimelQueryUtils.getAllTableSources("show view yelp.tmp_table") should equal(
        List("yelp.tmp_table")
      )
    }
    it("should exclude join desc") {
      GimelQueryUtils.getAllTableSources("DESC yelp.tmp_table") should equal(
        List()
      )
    }
    it("should pick table names from CACHE table") {
      assert(
        GimelQueryUtils
          .getAllTableSources("""cache table work_day_employees as
            |select * from udc.SFTP.Test.default.Files;""".stripMargin) == List(
          "work_day_employees",
          "udc.sftp.test.default.files"
        )
      )
      assert(
        GimelQueryUtils
          .getAllTableSources(
            """cache table workday_dump1 as
              |select
              |lower(a.ntid) as username
              |,a.`Employee QID` as employee_qid
              |,a.`Emplyee Last Name` as last_name
              |,a.`Employee First Name` as first_name
              |,concat(a.`Emplyee Last Name`,',',a.`Employee First Name`) as full_name
              |,a.`Org Description` as org_desc
              |,a.`Org ID ` as org_id
              |,a.`Loaction` as location
              |,lower(a.`Manager ID`) as manager_qid
              |,lower(b.ntid) as manager_username
              |from work_day_employees a
              |left join work_day_employees_b b
              |on a.`Manager ID` = b.`Employee QID`;""".stripMargin
          ) == List(
          "workday_dump1",
          "work_day_employees",
          "work_day_employees_b"
        )
      )

      assert(
        GimelQueryUtils
          .getAllTableSources(
            """set gimel.jdbc.p.strategy=file;
              |set gimel.jdbc.p.file=/user/testuser/udc.prod.pass;
              |set gimel.jdbc.username=testadmin;
              |
              |insert into udc.MySql.UDC.pcatalog.workday_dump
              |select * from workday_dump1 """.stripMargin
          ) == List("workday_dump1", "udc.mysql.udc.pcatalog.workday_dump")
      )
    }
  }

  describe("Check multiple match criteria within same SQL ") {
    it("should extract valid table names ") {
      logger.info(
        GimelQueryUtils.getAllTableSources(
          "drop table if exists udc.hive.test.testdb.emp"
        )
      )

      logger.info(
        GimelQueryUtils.getAllTableSources(
          """cache table td_views_hive_all
                                                   |select distinct * from
                                                   |(
                                                   |select *
                                                   | from
                                                   | udc.hive.test.default.teradata_db_views_test
                                                   |union
                                                   |select *
                                                   | from
                                                   | udc.hive.test.default.teradata_2_db_views_test
                                                   |)""".stripMargin
        )
      )
      logger.info(
        GimelQueryUtils.getAllTableSources(
          """cache table td_views_hive_all
                                             |select distinct tlb.* from
                                             |(
                                             |select *
                                             |from
                                             |udc.hive.test.default.teradata_db_views_test
                                             |union
                                             |select *
                                             |from
                                             |udc.hive.test.default.teradata_2_db_views_test
                                             |) tlb""".stripMargin
        )
      )
    }
  }
}
