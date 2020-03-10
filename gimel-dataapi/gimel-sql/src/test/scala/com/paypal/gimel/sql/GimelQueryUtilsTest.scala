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

import scala.util.{Failure, Success, Try}

import org.apache.commons.lang3.StringUtils
import org.mockito.Mockito._
import org.scalamock.scalatest.MockFactory
import org.scalatest._

import com.paypal.gimel.common.catalog.CatalogProvider
import com.paypal.gimel.common.gimelservices.{GimelServicesProperties, GimelServiceUtilities}
import com.paypal.gimel.parser.utilities.{QueryParserUtils, SearchSchemaUtils}

class GimelQueryUtilsTest extends FunSuite with Matchers with MockFactory {

  import com.paypal.gimel.parser.utilities.QueryParserUtils._

  test("Test Extraction of tablename ") {
    assert(
      extractTableName(
        "udc.teradata.test_cluster.yelp.review"
      ) == "yelp.review"
    )
    assert(
      extractTableName(
        "udc.Teradata.Test_cluster.yelp.business_details"
      ) == "yelp.business_details"
    )
    val tableName = "udc.teradata.test_cluster.yelp.review"
    assert(extractTableName(tableName, 1) === "review")
    assert(extractTableName(tableName, 2) === "yelp.review")
    testErrorCase(extractTableName("", 2))
    testErrorCase(
      extractTableName("yelp.review", 2)
    )
    testErrorCase(extractTableName(null, 2))
    testErrorCase(extractTableName("tablename") === "tablename")
  }

  test("Execute query ") {
    println(
      QueryParserUtils.isQueryOfGivenSeqType(
        "sel * from udc.Teradata.Test_cluster.yelp.business_details sample 10;"
      )
    )
    println(
      QueryParserUtils.isQueryOfGivenSeqType(
        "sel * fromudc.Teradata.Test_cluster.yelp.reviews sample 10;"
      )
    )
    println(
      QueryParserUtils.isQueryOfGivenSeqType(
        "DELETE udc.Teradata.Test_cluster.yelp.business_details ALL"
      )
    )
  }

  test("Transform SQL name") {
    println(
      QueryParserUtils.isQueryOfGivenSeqType(
        "show select * from udc.Teradata.Test_cluster.yelp.business_details"
      )
    )
    validateTransformSQL(
      "DELETE udc.Teradata.Test_cluster.yelp.business_details ALL",
      "DELETE yelp.business_details ALL"
    )
    validateTransformSQL(
      "show select * from udc.Teradata.Test_cluster.yelp.business_details",
      "show select * from yelp.business_details"
    )
    validateTransformSQL(
      """ INSERT INTO udc.Teradata.Test_cluster.yelp.business_details (
        | id,
        | created_date )
        | VALUES ('123fvf', '2019-08-09')""".stripMargin,
      """ INSERT INTO yelp.business_details (
        | id,
        | created_date )
        | VALUES ('123fvf', '2019-08-09')""".stripMargin
    )
  }

  def testErrorCase[R](block: => R): Option[R] = {
    Try(block) match {
      case Success(value) => Option(value)
      case Failure(exception) =>
        exception.printStackTrace()
        None
    }
  }

  test("Replace SQL") {
    val sql =
      """INSERT INTO udc.teradata.test_cluster.yelp.business_details VALUES ('123fvfv',
        |'2019-08-05')""".stripMargin
    var transformedSQL = sql
    val tables = GimelQueryUtils.getAllTableSources(
      sql,
      searchList = SearchSchemaUtils.ALL_TABLES_SEARCH_CRITERIA
    )
    println("Tables -> " + tables)
    tables.foreach(
      tableName =>
        transformedSQL =
          transformedSQL.replaceAll(tableName, extractTableName(tableName))
    )
    println("transformedSQL -> " + transformedSQL)
    assert(
      transformedSQL ===
        """INSERT INTO yelp.business_details VALUES ('123fvfv',
          |'2019-08-05')""".stripMargin
    )
  }

  private def validateTransformSQL(sql: String, assertString: String) = {
    assert(
      transformSQL(sql, QueryParserUtils.getDatasets(sql)) === assertString
    )
  }

  def transformSQL(sql: String, datasets: Seq[String]): String = {
    var transformedSQL = sql
    datasets.foreach(
      datasetName =>
        transformedSQL = StringUtils.replaceIgnoreCase(
          transformedSQL,
          datasetName,
          extractTableName(datasetName)
      )
    )

    transformedSQL
  }

  ignore("No connection to UDC service: TC 2") {
    test("validateAllDatasetsAreFromSameJdbcSystem") {

      val gimelServiceProps = spy(new GimelServicesProperties())
      val serviceUtilities = mock[GimelServiceUtilities]
      when(
        serviceUtilities
          .getSystemAttributesMapByName("udc.teradata.test_cluster.yelp.review")
      ).thenReturn(
        Map(
          "gimel.storage.type" -> "JDBC",
          "gimel.jdbc.url" -> "jdbc:teradata://teradata-host",
          "gimel.jdbc.driver.class" -> "com.teradata.jdbc.TeraDriver",
          "storageSystemID" -> "11"
        )
      )
      println(
        CatalogProvider
          .getStorageSystemProperties("udc.teradata.test_cluster.yelp.review")
      )
      println("Hello")
    }
  }

  test("extractSystemFromDatasetName") {
    assert(
      extractSystemFromDatasetName("udc.teradata.test_cluster.yelp.review") === "teradata.test_cluster"
    )
    try {
      extractSystemFromDatasetName("yelp.review")
    } catch {
      case e: IllegalStateException => e.printStackTrace()
    }
    try {
      extractSystemFromDatasetName(null)
    } catch {
      case e: IllegalArgumentException => e.printStackTrace()
    }
    assert(
      extractSystemFromDatasetName("udc. kafka.test_cluster.yelp.review ") === "kafka.test_cluster"
    )
  }

  test(" IS Select Quey") {
    assert(
      QueryParserUtils.isSelectQuery(
        "select * from udc.teradata.test_cluster.yelp.review sample 10;"
      )
    )
  }

  test(" getTablesFrom SQL ") {
    assert(
      GimelQueryUtils
        .getTablesFrom("help table udc.teradata.test_cluster.yelp.review;")
        .sameElements(Array("udc.teradata.test_cluster.yelp.review"))
    )

    assert(
      GimelQueryUtils
        .getAllTableSources(
          "help table udc.teradata.test_cluster.yelp.review;",
          searchList = SearchSchemaUtils.TARGET_TABLES_SEARCH_CRITERIA
        ) == List("udc.teradata.test_cluster.yelp.review")
    )

    assert(
      GimelQueryUtils
        .getAllTableSources(
          """
            |create multiset table ${targetDb}.enriched_data as
            |select
            |   review.review_id,
            |   review.review_text,
            |   review.user_id,
            |   review.review_date,
            |   review.business_id,
            |   business_details.name as business_name,
            |   postal_geo_map.latitude as business_latitude,
            |   postal_geo_map.longitude as business_longitude,
            |   yelp_user.name as user_name,
            |   yelp_user.review_count as user_review_count,
            |   yelp_user.yelping_since as user_yelping_since
            |from
            |   pcatalog.teradata.tau.yelp.review review
            |inner join
            |   pcatalog.teradata.tau.yelp.business_details business_details
            |on
            |   review.business_id = business_details.business_id
            |join
            |   pcatalog.teradata.tau.yelp.business_address business_address
            |on
            |   review.business_id = business_address.business_id
            |join
            |   pcatalog.teradata.tau.yelp.user yelp_user
            |on
            |   yelp_user.user_id = review.user_id
            |join
            |   pcatalog.teradata.tau.yelp.postal_geo_map
            |on
            |   business_address.postal_code = postal_geo_map.postal_code
            |where
            |   review.review_date > current_date -150
            |and
            |   review.business_id = 'ogpiys3gnfZNZBTEJw5-1Q'
            |""".stripMargin,
          searchList = SearchSchemaUtils.ALL_TABLES_SEARCH_CRITERIA
        ).sorted.sameElements(Array(
        "pcatalog.teradata.tau.yelp.review",
        "${targetdb}.enriched_data",
        "pcatalog.teradata.tau.yelp.business_details",
        "pcatalog.teradata.tau.yelp.business_address",
        "pcatalog.teradata.tau.yelp.user",
        "pcatalog.teradata.tau.yelp.postal_geo_map"
      ).sorted)
    )
  }

  // Substitutes dataset name with tmp table in sql using regex
  test ("getSQLWithTmpTable") {
    // Should match as "udc.hive.test.flights" is preceded by space and is at end of the line
    assert(GimelQueryUtils.getSQLWithTmpTable("select * from udc.hive.test.flights",
      "udc.hive.test.flights",
      "tmp_flights")
      == "select * from tmp_flights")

    // Should not match as "udc.hive.test.flights" is not preceded by any white space
    assert(GimelQueryUtils.getSQLWithTmpTable("select * fromudc.hive.test.flights",
      "udc.hive.test.flights",
      "tmp_flights")
      == "select * fromudc.hive.test.flights")

    // Should not match as "udc.hive.test.flights" is not followed by any white space, ; or ,
    assert(GimelQueryUtils.getSQLWithTmpTable("select * from udc.hive.test.flights_schedule",
      "udc.hive.test.flights",
      "tmp_flights")
      == "select * from udc.hive.test.flights_schedule")

    // Should match as "udc.hive.test.flights" is preceded by space and followed by new line
    assert(GimelQueryUtils.getSQLWithTmpTable("select * from udc.hive.test.flights\n",
      "udc.hive.test.flights",
      "tmp_flights")
      == "select * from tmp_flights\n")

    // Should match as "udc.hive.test.flights" is preceded by space and followed by ,
    assert(GimelQueryUtils.getSQLWithTmpTable("select * from udc.hive.test.flights, udc.hive.test.flights_schedule",
      "udc.hive.test.flights",
      "tmp_flights")
      == "select * from tmp_flights, udc.hive.test.flights_schedule")

    // Should match as "udc.hive.test.flights" is preceded and followed by space
    assert(GimelQueryUtils.getSQLWithTmpTable(
      "select * from udc.hive.test.flights where flights_id = 123",
      "udc.hive.test.flights",
      "tmp_flights")
      == "select * from tmp_flights where flights_id = 123")
  }
}
