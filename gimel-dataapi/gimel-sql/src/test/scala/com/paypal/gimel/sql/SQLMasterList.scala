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

object SQLMasterList {

  val insertSQL1 =
    """
      |INSERT INTO data_source_tab2 PARTITION (p1 = 'part1', p2)
      |  SELECT id, 'part' || id FROM RANGE(1, 3)
      |
    """.stripMargin

  val insertSQL2 =
    """
      |INSERT INTO table data_source_tab2 PARTITION (p1 = 'part1', p2)
      |  SELECT id, 'part' || id FROM RANGE(1, 3)
      |
    """.stripMargin

  val insertSQL3 =
    """
      |INSERT OVERWRITE TABLE data_source_tab2 PARTITION (p1 = 'partNew1', p2)
      |  VALUES (3, 'partNew2')
    """.stripMargin

  val insertSQL4 =
    """
      |INSERT OVERWRITE TABLE data_source_tab2 PARTITION (p1 = 'part1', p2)
      |  VALUES (5, 'part1')
    """.stripMargin

  val insertSQL5 =
    """insert into pcatalog.Elastic_Test_Cluster_yelp_review_data
      |select * from pcatalog.kafka_test_cluster_yelp_review
      |
    """.stripMargin

  val insertSQL6 =
    """
      |insert into tgt select * from src
    """.stripMargin

  val insertPartitionedTable1 =
    """
      |INSERT INTO TABLE temp_table2 PARTITION(col1) SELECT col1, col2, col3, col4 FROM temp_table1;
    """.stripMargin

  val baSQL4 =
    """
      |select yelp_user.*, last_user_review.last_review_date
      |from pcatalog.teradata.tau.yelp.user yelp_user
      |join (
      |select user_id,
      |max(review_date) as last_review_date
      |from pcatalog.teradata.tau.yelp.review review
      |group by user_id
      |) last_user_review
      |on yelp_user.user_id = last_user_review.user_id
      |where yelp_user.review_count > 100
      |and yelp_user.useful> 100
      |and fans > 100
      |and last_user_review.last_review_date < current_date - 180
      |sample 10
    """.stripMargin

  val sqlANSIanNonANSI =
    """select t1.* from
      |(select
      |b.*
      |from
      |a k,b l, c l, d k ) join t2
      |on t1.id = t2.id where
      |1=2 and
      |(select * from k , l where k.id = l.id)
    """.stripMargin

  val sqlANSISimple =
    """
      |select
      |b.*
      |from
      |a k,b l, c l, d k
    """.stripMargin

  val sqlANSISimpleSubQuery =
    """
      |select t.* from
      |(select
      |b.*
      |from
      |a k,b l, c l, d k ) t
    """.stripMargin

  val sqlANSIOnly =
    """
      |select * from
      |testdb.emp d
      |left join emp_loc f
      |on d.id = f.id
    """.stripMargin
  val plainSQL =
    """
      |select
      | * from abc;
    """.stripMargin

  val innerSQL =
    """
      |select * from (select * from a) tbl
    """.stripMargin

  val joinANSISQLViaUDC =
    """
      |select t2.c1, t2.c2
      |, t1.*
      |from
      |testdb.emp t1
      |join (
      |select f1.c11, f2.c11
      |from udc.kafka.test.emp.address f1
      |join udc.kafka.test.emp.loc f2
      |on f1.id = f2.id
      |) t2
      |on t1.id = t2.id
    """.stripMargin


  val mixANSINonANSISQL =
    """
      |select * from
      |testdb.emp s join
      |(
      |select
      |* from
      |udc.kafka.test.test.emp a, udc.hive.test_cluster.test.calendar b, c
      |where a.id = b.id
      |and c.id1 = c.id1
      |) t
      |on s.key = b.key
      |where 1= 2
      |and exists (select 1 from udc.teradata.test_cluster.testdb.lkp where lkp.id3 = s.id3);
      |
    """.stripMargin

  val commaTypeSQL =
    """
      |select f.*, d.*
      |from f , d
      |where f.id = d.id
    """.stripMargin

  val mixCaseSQL = "select * FRom tmp"

  val simpleSelect1 = "SELECT * FROM UDC.Mysql.datalake.test.YELP_REVIEW_READ"

  val simpleInsertSelect1 =
    """
      | INSERT INTO UDC.Mysql.datalake.test.YELP_REVIEW_WRITE
      | SELECT * FROM udc.kafka.tau.yelp.review
    """.stripMargin

  val simpleInsertSelect2 =
    "INSERT INTO UDC.Mysql.datalake.test.YELP_REVIEW_WRITE \n " +
      "SELECT * FROM udc.kafka.tau.yelp.review" + "\t" + " "

  // All DDLs are addressed here

  val simpleCreateDDL =
    "CREATE table tempTable (ageField int)"

  val complexCreateDDL =
    """CREATE EXTERNAL TABLE pcatalog.elastic_smoke_test(data string)
      |STORED AS TEXTFILE\nLOCATION 'hdfs:///tmp/pcatalog/elastic_smoke_test'
      |TBLPROPERTIES (
      |'gimel.storage.type' = 'ELASTIC_SEARCH',
      |'es.mapping.date.rich' = 'true',
      |'es.nodes' = 'http://es-host',
      |'es.port' = '8080',
      |'es.resource' = 'flights/data')
    """.stripMargin

  val dropIfExistsDDL =
    """DROP TABLE IF EXISTS pcatalog.elastic_smoke_test"""

  val dropPlainDDL =
    """DROP TABLE pcatalog.elastic_smoke_test"""

  val dropIfExistsViewDDL =
    """DROP TABLE IF EXISTS pcatalog.elastic_smoke_test"""

  val dropPlainViewDDL =
    """DROP TABLE pcatalog.elastic_smoke_test"""

  val truncateTableDDL =
    """TRUNCATE TABLE pcatalog.elastic_smoke_test"""

  val createTablePattern =
    """CREATE TABLE udc.mive.test_cluster.default.temp age (int)"""

  val createExternalTablePattern =
    """CREATE EXTERNAL TABLE udc.mive.test_cluster.default.temp age (int)"""

  val multisetPattern =
    """CREATE MULTISET TABLE udc.mive.test_cluster.default.temp age (int)"""

  val setPattern =
    """CREATE SET TABLE udc.mive.test_cluster.default.temp age (int)"""

  val dropTablePattern =
    """DROP TABLE udc.mive.test_cluster.default.temp"""

  val truncateTablePattern =
    """TRUNCATE      TABLE udc.mive.test_cluster.default.temp"""

  val deleteFromPattern =
    """DELETE FROM udc.mive.test_cluster.default.temp"""

  val deletePattern =
    """DELETE udc.mive.test_cluster.default.temp"""

}
