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

package com.paypal.gimel.serde.common.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SQLContext}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import org.scalatest.concurrent.Eventually

trait SharedSparkSession
    extends BeforeAndAfterEach
    with BeforeAndAfterAll
    with Eventually { self: Suite =>

  protected val additionalConfig: Map[String, String] = Map.empty

  /**
    * The [[SparkSession]] to use for all tests in this suite.
    *
    * By default, the underlying [[org.apache.spark.SparkContext]] will be run in local
    * mode with the default test configurations.
    */
  @transient private var _spark: SparkSession = null

  /**
    * This is the SparkSession to be accessed everywhere within the module for tests
    */
  protected implicit def spark: SparkSession = _spark

  /**
    * This is the SqlContext tio be accessed everywhere within the module for tests
    */
  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  /**
    * Make sure the [[SparkSession]] is initialized before any tests are run.
    */
  protected override def beforeAll(): Unit = {
    initializeSession(additionalConfig)

    // Ensure we have initialized the context before calling parent code
    super.beforeAll()
  }

  /**
    *  Generally, this is just called from
    * beforeAll; however, in test using styles other than FunSuite, there is
    * often code that relies on the session between test group constructs and
    * the actual tests, which may need this session.  It is purely a semantic
    * difference, but semantically, it makes more sense to call
    * 'initializeSession' between a 'describe' and an 'it' call than it does to
    * call 'beforeAll'.
    */
  protected def initializeSession(conf: Map[String, String]): Unit = {
    if (_spark == null) {
      _spark = createSparkSession(conf)
    }
  }

  /**
    *
    * @return sparkSession
    */
  protected def createSparkSession(conf: Map[String, String]): SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Spark Unit Tests")
      .config(sparkConf(conf))
      .getOrCreate()
  }

  // Here add all the spark confs to be initialized in order to start the sparksession with.
  protected def sparkConf(conf: Map[String, String]): SparkConf = {
    val sparkConf = new SparkConf()
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
    conf.foreach(kv => sparkConf.set(kv._1, kv._2))
    sparkConf
  }

  /**
    * Stop the underlying [[org.apache.spark.SparkContext]], if any.
    */
  protected override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      try {
        if (_spark != null) {
          try {
            _spark.sessionState.catalog.reset()
          } finally {
            _spark.stop()
            _spark = null
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }

  /**
    * Things to do before each test
    */
  protected override def beforeEach(): Unit = {
    super.beforeEach()
  }

  /**
    * Things to do after each test
    */
  protected override def afterEach(): Unit = {
    super.afterEach()
    // Clear all persistent datasets after each test
    spark.sharedState.cacheManager.clearCache()
  }

  // Mocks data for testing
  def mockDataInDataFrame(numberOfRows: Int): DataFrame = {
    def stringed(n: Int) = s"""{"id": "$n","name": "MAC-$n", "address": "MAC-${n + 1}", "age": "${n + 1}", "company": "MAC-$n", "designation": "MAC-$n", "salary": "${n * 10000}" }"""
    val texts: Seq[String] = (1 to numberOfRows).map { x => stringed(x) }
    val rdd: RDD[String] = spark.sparkContext.parallelize(texts)
    val dataFrame: DataFrame = spark.read.json(rdd)
    dataFrame
  }

  // Mocks data for testing
  def mockDataInDataFrameWithJsonString(numberOfRows: Int): DataFrame = {
    def stringed(n: Int) = s"""{"id": "$n","name": "MAC-$n", "address": "MAC-${n + 1}", "age": "${n + 1}", "company": "MAC-$n", "designation": "MAC-$n", "salary": "${n * 10000}" }"""
    val texts: Seq[String] = (1 to numberOfRows).map { x => stringed(x) }
    val rdd: RDD[Row] = spark.sparkContext.parallelize(texts).map(x => Row(x))
    spark.createDataFrame(rdd, StructType(List(StructField("value", org.apache.spark.sql.types.StringType, true))))
  }

  // Mocks data for testing
  def mockDataInDataFrameWithJsonStringBytes(numberOfRows: Int): DataFrame = {
    def stringed(n: Int) = s"""{"id": "$n","name": "MAC-$n", "address": "MAC-${n + 1}", "age": "${n + 1}", "company": "MAC-$n", "designation": "MAC-$n", "salary": "${n * 10000}" }"""
    val texts: Seq[Array[Byte]] = (1 to numberOfRows).map { x => stringed(x).getBytes }
    val rdd: RDD[Row] = spark.sparkContext.parallelize(texts).map(x => Row(x))
    spark.createDataFrame(rdd, StructType(List(StructField("value", org.apache.spark.sql.types.BinaryType, true))))
  }
}
