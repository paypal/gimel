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

package com.paypal.gimel.kafka2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.scalatest._

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.utilities.DataSetUtils
import com.paypal.gimel.kafka2.conf.{KafkaConfigs, KafkaConstants}
import com.paypal.gimel.kafka2.utilities.ImplicitKafkaConverters._
import com.paypal.gimel.logger.Logger

class DataSetTest extends FunSuite with SharedSparkSession {
  val kafkaCluster = new EmbeddedSingleNodeKafkaCluster()
  var dataSet: DataSet = _
  val topic = "test_gimel_dataset"
  val logger = Logger()
  logger.setLogLevel("INFO")
  logger.consolePrintEnabled = true
  var appTag: String = _
  val checkpointRoot = "/pcatalog/kafka_consumer/checkpoint"
  val zkNodeParent = checkpointRoot + "/" + appTag

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    kafkaCluster.start()
    kafkaCluster.createTopic(topic)
    appTag = com.paypal.gimel.common.utilities.DataSetUtils.getAppTag(spark.sparkContext)
    dataSet = new DataSet(spark)
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    kafkaCluster.deleteTopicIfExists(topic)
    kafkaCluster.stop()
  }

  test("Write operation") {
    val props : Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topic,
      KafkaConfigs.kafkaServerKey -> kafkaCluster.bootstrapServers(),
      KafkaConfigs.zookeeperCheckpointHost -> kafkaCluster.zookeeperConnect())
    val dataSetName = "Kafka.Local.default." + topic
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
      GimelConstants.APP_TAG -> appTag)
    val dataFrame = mockDataInDataFrame(10)
    // Serialize dataframe to json
    val serializedDF = dataFrame.toJSON.toDF
    serializedDF.show(1)
    val df = dataSet.write(dataSetName, serializedDF, datasetProps)
    val dfRead = readDataFromKafkaWithoutDataset(topic)
    dfRead.show(1)
    assert(dfRead.count() == 10)
  }

  test("Read operation") {
    val topicName = topic + "_1"
    val dataFrame = mockDataInDataFrame(10)
    // Serialize dataframe to json
    val serializedDF = dataFrame.toJSON.toDF
    mockDataInKafkaWithoutDataset(topicName, serializedDF)
    val props : Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topicName,
      KafkaConfigs.kafkaServerKey -> kafkaCluster.bootstrapServers(),
      KafkaConfigs.zookeeperCheckpointHost -> kafkaCluster.zookeeperConnect())
    val dataSetName = "Kafka.Local.default." + topicName
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
      GimelConstants.APP_TAG -> appTag)
    val df = dataSet.read(dataSetName, datasetProps)
    // Deserialize json messages
    val deserializedDS: RDD[String] = df.rdd.map { eachRow => {
      eachRow.getAs("value").asInstanceOf[Array[Byte]].map(_.toChar).mkString
    } }
    val deserializedDF: DataFrame = spark.read.json(deserializedDS)
    deserializedDF.show(1)
    assert(df.count() == 10)
    assert(deserializedDF.columns
      .sameElements(Array("address", "age", "company", "designation", "id", "name", "salary")))
  }

  test("saveCheckpoint") {
    val topicName = topic + "_2"
    val dataFrame = mockDataInDataFrame(10)
    // Serialize dataframe to json
    val serializedDF = dataFrame.toJSON.toDF
    mockDataInKafkaWithoutDataset(topicName, serializedDF)
    val props : Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topicName,
      KafkaConfigs.kafkaServerKey -> kafkaCluster.bootstrapServers(),
      KafkaConfigs.zookeeperCheckpointHost -> kafkaCluster.zookeeperConnect())
    val dataSetName = "Kafka.Local.default." + topicName
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    // Setting these props to read only 5 records from kafka topic
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
      GimelConstants.APP_TAG -> DataSetUtils.getAppTag(spark.sparkContext),
      KafkaConfigs.rowCountOnFirstRunKey -> "10",
      KafkaConfigs.maxRecordsPerPartition -> "5")
    val df = dataSet.read(dataSetName, datasetProps)
    // Deserialize json messages
    val deserializedDS: RDD[String] = df.rdd.map { eachRow => {
      eachRow.getAs("value").asInstanceOf[Array[Byte]].map(_.toChar).mkString
    } }
    val deserializedDF: DataFrame = spark.read.json(deserializedDS)
    deserializedDF.show(1)
    dataSet.saveCheckPoint()
    logger.info("readTillOffsetRange -> " + dataSet.readTillOffsetRange.get.toStringOfKafkaOffsetRanges)
    val zkNode = "/pcatalog/kafka_consumer/checkpoint/file:/" + DataSetUtils.getUserName(spark.sparkContext) + "/Spark-Unit-Tests/" + topicName
    val zkNodes: Option[String] = com.paypal.gimel.common.storageadmin.ZooKeeperAdminClient.readFromZK(kafkaCluster.zookeeperConnect(), zkNode)
    logger.info("saveCheckpoint | zkNodes -> " + zkNodes.get)
    assert(zkNodes.get == s"$topicName,0,0,5")
  }

  test("read with checkpoint present") {
    val topicName = topic + "_3"
    val dataFrame = mockDataInDataFrame(10)
    // Serialize dataframe to json
    val serializedDF = dataFrame.toJSON.toDF
    mockDataInKafkaWithoutDataset(topicName, serializedDF)
    val props : Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topicName,
      KafkaConfigs.kafkaServerKey -> kafkaCluster.bootstrapServers(),
      KafkaConfigs.zookeeperCheckpointHost -> kafkaCluster.zookeeperConnect(),
      KafkaConfigs.kafkaSourceFieldsListKey -> "all")
    val dataSetName = "Kafka.Local.default." + topicName
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
      GimelConstants.APP_TAG -> DataSetUtils.getAppTag(spark.sparkContext),
      KafkaConfigs.rowCountOnFirstRunKey -> "10",
      KafkaConfigs.maxRecordsPerPartition -> "5")
    val df = dataSet.read(dataSetName, datasetProps)
    df.show(1)
    dataSet.saveCheckPoint()
    val dfReadFromCheckpoint = dataSet.read(dataSetName, datasetProps)
    val maxOffset = dfReadFromCheckpoint.groupBy().max("offset").head.get(0)
    val minOffset = dfReadFromCheckpoint.groupBy().min("offset").head.get(0)
    dfReadFromCheckpoint.show(1)
    logger.info("max(offset) -> " + maxOffset)
    logger.info("min(offset) -> " + minOffset)
    assert(maxOffset == 9)
    assert(minOffset == 5)
  }

  test("clearCheckPoint") {
    val topicName = topic + "_4"
    val dataFrame = mockDataInDataFrame(10)
    // Serialize dataframe to json
    val serializedDF = dataFrame.toJSON.toDF
    mockDataInKafkaWithoutDataset(topicName, serializedDF)
    val props : Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topicName,
      KafkaConfigs.kafkaServerKey -> kafkaCluster.bootstrapServers(),
      KafkaConfigs.zookeeperCheckpointHost -> kafkaCluster.zookeeperConnect(),
      KafkaConfigs.kafkaSourceFieldsListKey -> "all")
    val dataSetName = "Kafka.Local.default." + topicName
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
      GimelConstants.APP_TAG -> DataSetUtils.getAppTag(spark.sparkContext),
      KafkaConfigs.rowCountOnFirstRunKey -> "10",
      KafkaConfigs.maxRecordsPerPartition -> "5")
    val df = dataSet.read(dataSetName, datasetProps)
    df.show(1)
    dataSet.saveCheckPoint()
    val zkNode = "/pcatalog/kafka_consumer/checkpoint/file:/" + DataSetUtils.getUserName(spark.sparkContext) + "/Spark-Unit-Tests/" + topicName
    val zkNodes: Option[String] = com.paypal.gimel.common.storageadmin.ZooKeeperAdminClient.readFromZK(kafkaCluster.zookeeperConnect(), zkNode)
    println("before clearCheckPoint | zkNodes -> " + zkNodes.get)
    dataSet.clearCheckPoint()
    logger.info("after clearCheckPoint | zkNodes -> ")
    val zkNodes2 = com.paypal.gimel.common.storageadmin.ZooKeeperAdminClient.readFromZK(kafkaCluster.zookeeperConnect(), zkNode)
    assert(zkNodes2 == None)
  }

  /*
   * Read with gimel.kafka.source.fields.list = all
   * It should get all the source fields from kafka on dataset.read
   * By default it gets only value field
   */
  test("read operation with " + KafkaConfigs.kafkaSourceFieldsListKey + " = all") {
    val topicName = topic + "_5"
    val dataFrame = mockDataInDataFrame(10)
    // Serialize dataframe to json
    val serializedDF = dataFrame.toJSON.toDF
    mockDataInKafkaWithoutDataset(topicName, serializedDF)
    val props : Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topicName,
      KafkaConfigs.kafkaServerKey -> kafkaCluster.bootstrapServers(),
      KafkaConfigs.zookeeperCheckpointHost -> kafkaCluster.zookeeperConnect(),
      KafkaConfigs.kafkaSourceFieldsListKey -> "all")
    val dataSetName = "Kafka.Local.default." + topicName
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
      GimelConstants.APP_TAG -> DataSetUtils.getAppTag(spark.sparkContext))
    val df = dataSet.read(dataSetName, datasetProps)
    df.show(1)
    assert(df.columns
      .sameElements(Array("key",
        "value",
        "topic",
        "partition",
        "offset",
        "timestamp",
        "timestampType")))
  }

  /*
   * Read operation with rowCountOnFirstRunKey set,
   * it should limit the number of records fetched from latest offset on first run without checkpoint
   * Example: If gimel.kafka.throttle.batch.fetchRowsOnFirstRun = 100,000 and latest offset is at 200,000
   * It will fetch 100,000 back from latest offset -> [100,000 - 200,000]
   */
  test("read operation with " + KafkaConfigs.rowCountOnFirstRunKey) {
    val topicName = topic + "_6"
    val dataFrame = mockDataInDataFrame(10)
    // Serialize dataframe to json
    val serializedDF = dataFrame.toJSON.toDF
    mockDataInKafkaWithoutDataset(topicName, serializedDF)
    val props : Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topicName,
      KafkaConfigs.kafkaServerKey -> kafkaCluster.bootstrapServers(),
      KafkaConfigs.zookeeperCheckpointHost -> kafkaCluster.zookeeperConnect(),
      KafkaConfigs.kafkaSourceFieldsListKey -> "all")
    val dataSetName = "Kafka.Local.default." + topicName
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
      GimelConstants.APP_TAG -> DataSetUtils.getAppTag(spark.sparkContext),
      KafkaConfigs.rowCountOnFirstRunKey -> "7")
    val df = dataSet.read(dataSetName, datasetProps)
    df.show(1)
    assert(df.count() == 7)
  }

  // Read operation with maxRecordsPerPartition set, it should limit the number of records read per kafka partition
  test("read operation with " + KafkaConfigs.maxRecordsPerPartition) {
    val topicName = topic + "_7"
    val dataFrame = mockDataInDataFrame(10)
    // Serialize dataframe to json
    val serializedDF = dataFrame.toJSON.toDF
    mockDataInKafkaWithoutDataset(topicName, serializedDF)
    val props : Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topicName,
      KafkaConfigs.kafkaServerKey -> kafkaCluster.bootstrapServers(),
      KafkaConfigs.zookeeperCheckpointHost -> kafkaCluster.zookeeperConnect(),
      KafkaConfigs.kafkaSourceFieldsListKey -> "all")
    val dataSetName = "Kafka.Local.default." + topicName
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
      GimelConstants.APP_TAG -> DataSetUtils.getAppTag(spark.sparkContext),
      KafkaConfigs.maxRecordsPerPartition -> "5")
    val df = dataSet.read(dataSetName, datasetProps)
    df.show(1)
    assert(df.count() == 5)
  }

  /* Read with minRowsPerParallel set, it should divide each kafka partition data into spark partitions with minimum these many records
   * Example: If minRowsPerParallel = 100,000 and total records fetched from kafka = 200,000
   * It should divide the records in each kafka partition into 3 spark partitions, [0 - 100,000), [100,000 - 200,000), [200,000 - 200,000)
   */
  test("read operation with " + KafkaConfigs.minRowsPerParallelKey + " = 2") {
    val topicName = topic + "_7"
    val dataFrame = mockDataInDataFrame(10)
    // Serialize dataframe to json
    val serializedDF = dataFrame.toJSON.toDF
    mockDataInKafkaWithoutDataset(topicName, serializedDF)
    val props : Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topicName,
      KafkaConfigs.kafkaServerKey -> kafkaCluster.bootstrapServers(),
      KafkaConfigs.zookeeperCheckpointHost -> kafkaCluster.zookeeperConnect(),
      KafkaConfigs.kafkaSourceFieldsListKey -> "all")
    val dataSetName = "Kafka.Local.default." + topicName
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
      GimelConstants.APP_TAG -> DataSetUtils.getAppTag(spark.sparkContext),
      KafkaConfigs.minRowsPerParallelKey -> "2")
    val df = dataSet.read(dataSetName, datasetProps)
    df.show(1)
    val numPartitions = df.rdd.getNumPartitions
    logger.info("Partitions -> " + numPartitions)
    assert(numPartitions == 11)
  }

  def mockDataInKafkaWithoutDataset (topicName: String, dataFrame: DataFrame): Unit = {
    dataFrame
      .write
      .format(KafkaConstants.KAFKA_FORMAT)
      .option(KafkaConstants.KAFKA_BOOTSTRAP_SERVERS, kafkaCluster.bootstrapServers())
      .option(KafkaConstants.KAFKA_TOPIC, topicName)
      .save()
  }

  def readDataFromKafkaWithoutDataset(topicName: String): DataFrame = {
    spark
      .read
      .format(KafkaConstants.KAFKA_FORMAT)
      .option(KafkaConstants.KAFKA_BOOTSTRAP_SERVERS, kafkaCluster.bootstrapServers())
      .option(KafkaConstants.KAFKA_SUBSCRIBE, topicName)
      .load()
  }
}
