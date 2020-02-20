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

package com.paypal.gimel.parser.utilities

import org.scalatest.FunSuite

class QueryParserUtilsTest extends FunSuite {

  private val query1: String =
    """
      |insert into ${targetDb}.enriched_data
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
      |""".stripMargin

  test("Teradata ANSI getSourceTables ") {
    val result = Seq(
      "pcatalog.teradata.tau.yelp.review",
      "pcatalog.teradata.tau.yelp.business_details",
      "pcatalog.teradata.tau.yelp.business_address",
      "pcatalog.teradata.tau.yelp.user",
      "pcatalog.teradata.tau.yelp.postal_geo_map",
      "${targetdb}.enriched_data"
    )

    assert(
      QueryParserUtils.getAllSourceTables(query1).forall(result.contains(_))
    )
  }

  test("getTargetTables") {
    assert(
      QueryParserUtils
        .getTargetTables(query1)
        .head === "${targetDb}.enriched_data"
    )
  }

  test("isHavingLimit") {
    val sql = "select * from udc.hbase.cluster_name.default_test limit 10"
    assert(
      QueryParserUtils.isHavingLimit(sql) == true
    )
  }

  test("getLimit") {
    val sql1 = "select * from udc.hbase.cluster_name.default_test limit 10"
    println("Checking for SQL -> " + sql1)
    assert(
      QueryParserUtils.getLimit(sql1) == 10
    )

    val sql2 = "select * from udc.hbase.cluster_name.default_test limit"
    println("Checking for SQL -> " + sql2)
    val exception = intercept[Exception] {
      QueryParserUtils.getLimit(sql2)
    }
    assert(exception.getMessage.contains("Invalid SQL"))
  }
}
