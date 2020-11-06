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

package com.paypal.gimel.hbase.utilities

import org.apache.spark.sql.types.StringType
import org.scalatest.{BeforeAndAfterAll, Matchers}

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.hbase.conf.HbaseConfigs

class HBaseUtilitiesTest extends HBaseLocalClient with Matchers with BeforeAndAfterAll {

  var hbaseUtilities : HBaseUtilities = new HBaseUtilities(sparkSession)

  test ("castAllColsToString") {
    // it should cast all the columns in dataframe to string
    assert (hbaseUtilities.castAllColsToString(mockDataInDataFrame(5)).schema.filter(col => col.dataType != StringType).length == 0)
  }

  test ("getColumnMappingForColumnFamily") {
    // it should return the map of column family to column with correct pattern
    val mapping = hbaseUtilities.getColumnMappingForColumnFamily("cf1:c1,cf1:c2,cf1:c3,cf2:c4")
    assert(mapping("cf1").sameElements(Array("c1", "c2", "c3")))
    assert(mapping("cf2").sameElements(Array("c4")))

    // it should return the map of column family to column with correct pattern including :key
    val mapping1 = hbaseUtilities.getColumnMappingForColumnFamily(":key,cf1:c1,cf1:c2,cf1:c3,cf2:c4")
    assert(mapping1("cf1").sameElements(Array("c1", "c2", "c3")))
    assert(mapping1("cf2").sameElements(Array("c4")))

    val mapping2 = hbaseUtilities.getColumnMappingForColumnFamily("cf1:c1,:key,cf1:c2,cf1:c3,cf2:c4")
    assert(mapping2("cf1").sameElements(Array("c1", "c2", "c3")))
    assert(mapping2("cf2").sameElements(Array("c4")))
  }
}
