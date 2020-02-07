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

package com.paypal.gimel.serde.common.utils

import org.apache.spark.sql.types._
import org.scalatest._

class SQLDataTypesUtilsTest extends FunSpec with Matchers {
  describe("getFieldNameSQLDataTypes") {
    it ("should convert the type names for fields in a given map to SQL data types") {
      val fieldTypeNameMap = Map("id" -> "int", "name" -> "string", "age" -> "int", "salary" -> "long")
      val fieldToSQLTypeMap = SQLDataTypesUtils.getFieldNameSQLDataTypes(fieldTypeNameMap)
      assert(fieldToSQLTypeMap
        .sameElements(Map("id" -> IntegerType, "name" -> StringType, "age" -> IntegerType, "salary" -> LongType)))
    }
  }

  describe("typeNameToSQLType") {
    it ("should convert a type name to SQL data type") {
      SQLDataTypesUtils.typeNameToSQLType("null").shouldEqual(NullType)
      SQLDataTypesUtils.typeNameToSQLType("date").shouldEqual(DateType)
      SQLDataTypesUtils.typeNameToSQLType("timestamp").shouldEqual(TimestampType)
      SQLDataTypesUtils.typeNameToSQLType("binary").shouldEqual(BinaryType)
      SQLDataTypesUtils.typeNameToSQLType("int").shouldEqual(IntegerType)
      SQLDataTypesUtils.typeNameToSQLType("boolean").shouldEqual(BooleanType)
      SQLDataTypesUtils.typeNameToSQLType("long").shouldEqual(LongType)
      SQLDataTypesUtils.typeNameToSQLType("double").shouldEqual(DoubleType)
      SQLDataTypesUtils.typeNameToSQLType("float").shouldEqual(FloatType)
      SQLDataTypesUtils.typeNameToSQLType("short").shouldEqual(ShortType)
      SQLDataTypesUtils.typeNameToSQLType("byte").shouldEqual(ByteType)
      SQLDataTypesUtils.typeNameToSQLType("string").shouldEqual(StringType)
      SQLDataTypesUtils.typeNameToSQLType("calendarinterval").shouldEqual(CalendarIntervalType)
      SQLDataTypesUtils.typeNameToSQLType("decimal").shouldEqual(DecimalType.USER_DEFAULT)
      SQLDataTypesUtils.typeNameToSQLType("decimal(4, 1)").shouldEqual(DecimalType(4, 1))
    }
  }

  describe("getSchemaFromBindToFieldsJson") {
    it ("should convert a json of field and types to spark schema") {
      val fieldsBindToString =
        s"""[{"fieldName":"isUsCitizen","fieldType":"boolean","defaultValue":"true"},
           |{"fieldName":"dob","fieldType":"date","defaultValue":"null"},
           |{"fieldName":"name","fieldType":"string","defaultValue":"null"},
           |{"fieldName":"age","fieldType":"int","defaultValue":"23"},
           |{"fieldName":"salary","fieldType":"long","defaultValue":"4000"}]
           |""".stripMargin

      val schema = SQLDataTypesUtils.getSchemaFromBindToFieldsJson(fieldsBindToString)
      schema.shouldBe(StructType(List(
        StructField("name", StringType, true),
        StructField("isUsCitizen", BooleanType, true),
        StructField("dob", DateType, true),
        StructField("age", IntegerType, true),
        StructField("salary", LongType, true))))
    }
  }
}
