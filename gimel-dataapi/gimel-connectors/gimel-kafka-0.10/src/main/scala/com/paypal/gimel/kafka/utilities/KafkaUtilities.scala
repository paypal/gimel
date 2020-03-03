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

package com.paypal.gimel.kafka.utilities

import java.io.{Closeable, Serializable}
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.util.parsing.json.JSON

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.KafkaUtils._
import spray.json._
import spray.json.DefaultJsonProtocol._

import com.paypal.gimel.common.catalog.CatalogProvider
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.schema.ConfluentSchemaRegistry
import com.paypal.gimel.common.storageadmin
import com.paypal.gimel.common.storageadmin.KafkaAdminUtils
import com.paypal.gimel.common.utilities.DataSetUtils._
import com.paypal.gimel.datastreamfactory.{StreamCheckPointHolder, WrappedData}
import com.paypal.gimel.kafka.avro.SparkAvroUtilities._
import com.paypal.gimel.kafka.conf._
import com.paypal.gimel.kafka.conf.KafkaJsonProtocol.{offsetPropertiesFormat, offsetRangePropertiesFormat}
import com.paypal.gimel.kafka.utilities.ImplicitKafkaConverters._
import com.paypal.gimel.kafka.utilities.ImplicitZKCheckPointers._


case class MessageInfo[T: TypeTag](key: String, message: T, topic: String, partition: Int, offset: Long)

/*
Case classes for reading custom offset properties from the user defined properties
 */
case class OffsetRangeProperties(partition: Int,
                                 from: Long,
                                 to: Option[Long])

case class OffsetProperties(topic: String,
                            offsetRange: Array[OffsetRangeProperties])

object KafkaUtilities {

  val logger = com.paypal.gimel.logger.Logger()

  /**
    * This is a Map of Properties that will be used to set the batch parameters
    * , based on the incoming volume of data & user supplied parameters
    */
  val defaultRowsPerBatch: Map[Int, Map[String, String]] = Map(
    100000000 -> Map(
      KafkaConfigs.batchFetchSize -> "500"
      , KafkaConfigs.maxRecordsPerPartition -> "100000000"
      , KafkaConfigs.minRowsPerParallelKey -> "100000"
    )
    , 50000000 -> Map(
      KafkaConfigs.batchFetchSize -> "500"
      , KafkaConfigs.maxRecordsPerPartition -> "50000000"
      , KafkaConfigs.minRowsPerParallelKey -> "100000"
    )
    , 25000000 -> Map(
      KafkaConfigs.batchFetchSize -> "250"
      , KafkaConfigs.maxRecordsPerPartition -> "25000000"
      , KafkaConfigs.minRowsPerParallelKey -> "100000"
    )
    , 10000000 -> Map(
      KafkaConfigs.batchFetchSize -> "100"
      , KafkaConfigs.maxRecordsPerPartition -> "10000000"
      , KafkaConfigs.minRowsPerParallelKey -> "100000"
    )
    , 1000000 -> Map(
      KafkaConfigs.batchFetchSize -> "20"
      , KafkaConfigs.maxRecordsPerPartition -> "1000000"
      , KafkaConfigs.minRowsPerParallelKey -> "100000"
    )
    , 100000 -> Map(
      KafkaConfigs.batchFetchSize -> "10"
      , KafkaConfigs.maxRecordsPerPartition -> "100000"
      , KafkaConfigs.minRowsPerParallelKey -> "10000"
    )
    , 30000 -> Map(
      KafkaConfigs.batchFetchSize -> "10"
      , KafkaConfigs.maxRecordsPerPartition -> "100000"
      , KafkaConfigs.minRowsPerParallelKey -> "10000"
    )
  )


  /**
    * Determines whether an incoming volume of messages
    * from Kafka is Streamable with given parameters.
    *
    * @param sparkSession : SparkSession
    * @param props        Properties
    * @param rowsInBatch  RowsPerBatch Map
    * @return true if data is within streaming capacity
    *         , false if we need to switch to batch
    */
  def isStreamable(sparkSession: SparkSession, props: Map[String, String]
                   , rowsInBatch: Map[Int, Map[String, String]] = defaultRowsPerBatch): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    //    val dSet = com.paypal.gimel.DataSet(hiveContext)
    val dataSet = props(GimelConstants.DATASET)
    //    import com.paypal.gimel.DataSetUtils._
    // This is the DataSet Properties
    val datasetProps = CatalogProvider.getDataSetProperties(dataSet)
    logger.info(
      s"""DataSet Props -->
          |${datasetProps.props.map(x => s"${x._1} --> ${x._2}").mkString("\n")}""".stripMargin)
    val newProps: Map[String, Any] = getProps(props) ++ Map(
      GimelConstants.DATASET_PROPS -> datasetProps,
      GimelConstants.DATASET -> dataSet,
      GimelConstants.RESOLVED_HIVE_TABLE -> resolveDataSetName(dataSet),
      GimelConstants.APP_TAG -> getAppTag(sparkSession.sparkContext))
    val conf = new KafkaClientConfiguration(newProps)
    logger.info(s"Zookeeper Details --> ${conf.zkHostAndPort} | ${conf.zkCheckPoints}")
    val thresholdRows = 1000000000
    val lastCheckPoint: Option[Array[OffsetRange]] = getLastCheckPointFromZK(conf.zkHostAndPort
      , conf.zkCheckPoints)
    val availableOffsetRange: Array[OffsetRange] = {
      BrokersAndTopic(conf.kafkaHostsAndPort, conf.kafkaTopics).toKafkaOffsetsPerPartition
    }
    if (lastCheckPoint.isDefined) {
      logger.info(s"Offsets in CheckPoint --> ${lastCheckPoint.get.mkString("\n")}")
    }
    logger.info(s"Offsets in Kafka --> ${availableOffsetRange.mkString("\n")}")
    val newOffsetRangesForReader: Array[OffsetRange] = {
      getNewOffsetRangeForReader(lastCheckPoint, availableOffsetRange, thresholdRows)
    }
    logger.info(s"New Offsets to Fetch --> ${newOffsetRangesForReader.mkString("\n")}")
    val totalMessages = newOffsetRangesForReader.map(oR => oR.untilOffset - oR.fromOffset).sum.toInt
    logger.info(s"Total Messages from New Offsets to Fetch --> $totalMessages")
    val userSuppliedMaxRows = {
      sparkSession.conf.get(KafkaConfigs.rowCountOnFirstRunKey, totalMessages.toString)
    }
    val totalRows = if (lastCheckPoint.isEmpty) userSuppliedMaxRows.toInt else totalMessages
    logger.info(s"Final Total Messages to Fetch --> $totalRows")
    val streamCutOff = sparkSession.conf.get(KafkaConfigs.streamCutOffThresholdKey, "100000").toInt
    val (batchProps, isStreamable) = totalRows match {
      case n if 50000000 <= n =>
        (rowsInBatch(100000000), false)
      case n if 25000000 <= n =>
        (rowsInBatch(50000000), false)
      case n if 10000000 <= n =>
        (rowsInBatch(25000000), false)
      case n if 1000000 <= n =>
        (rowsInBatch(10000000), false)
      case n if streamCutOff <= n =>
        (rowsInBatch(1000000), false)
      case _ =>
        (Map(), true)
    }
    logger.info(s"Batch Props --> $batchProps")
    val resolvedProps = props ++ batchProps
    logger.info(s"Resolved Props --> $resolvedProps")
    logger.info(s"isStreamable --> $isStreamable")
    resolvedProps.foreach(p => sparkSession.conf.set(p._1, p._2.toString))
    isStreamable
  }

  /**
    * Convenience Function to checkpoint a given OffsetRange
    *
    * @param zkHost      Host Server for Zookeeper
    * @param zkNodes      Node where we want to checkPoint
    * @param offsetRange Array[OffsetRange]
    * @return Boolean indicating checkpointing status
    */

  def inStreamCheckPoint(zkHost: String, zkNodes: Seq[String]
                         , offsetRange: Array[OffsetRange]): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val zk = ZooKeeperHostAndNodes(zkHost, zkNodes)
    (zk, offsetRange).saveZkCheckPoint
  }

  /**
    * Convenience Function to checkpoint a given OffsetRange
    *
    * @param sparkSession Spark Session
    * @param zkHost      Host Server for Zookeeper
    * @param zkNodes      Node where we want to checkPoint
    * @return Boolean indicating checkpointing status
    */

  def inStructuredStreamCheckPoint(sparkSession: SparkSession, zkHost: String, zkNodes: Seq[String]): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    sparkSession.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(event: QueryStartedEvent): Unit = Unit
      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        val queryStatusMap = JSON.parseFull(event.progress.json).get.asInstanceOf[Map[String, Any]]
        val endOffsetsMap: Map[String, Map[Any, Any]] = queryStatusMap.get("sources").head.asInstanceOf[List[Any]].head.asInstanceOf[Map[Any, Any]].get("endOffset").head.asInstanceOf[Map[String, Map[Any, Any]]]
        val endOffsets = endOffsetsMap.flatMap { x =>
          x._2.map { y =>
            OffsetRange(topic = x._1, partition = y._1.asInstanceOf[String].toInt, fromOffset = 0L, untilOffset = y._2.asInstanceOf[Double].longValue())
          }
        }.toArray
        StreamCheckPointHolder().setCurentCheckPoint(endOffsets)
        inStreamCheckPoint(zkHost, zkNodes, endOffsets)
      }
      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
        sparkSession.streams.removeListener(this)
      }
    })
  }

  /**
    * Gets the Appropriate Serializer Class
    *
    * @param serializerClassName Name of the Serializer Class
    * @return Serializer Class
    */

  def getSerializer(serializerClassName: String)
  : Class[_ >: StringSerializer with ByteArraySerializer <: Serializer[_ >: String with Array[Byte]]] = {
    serializerClassName match {
      case "org.apache.kafka.common.serialization.StringSerializer" => {
        classOf[org.apache.kafka.common.serialization.StringSerializer]
      }
      case "org.apache.kafka.common.serialization.ByteArraySerializer" => {
        classOf[org.apache.kafka.common.serialization.ByteArraySerializer]
      }
      case _ => {
        throw new Exception(s"UnSupported Serializer Class Requested : ${serializerClassName}")
      }
    }
  }

  /**
    * Gets the Appropriate DeSerializer Class
    *
    * @param deserializerClassName Name of the DeSerializer Class
    * @return DeSerializer Class
    */

  def getDeserializer(deserializerClassName: String)
  : Class[_ >: StringDeserializer with ByteArrayDeserializer <: Deserializer[_ >: String with Array[Byte]]] = {
    deserializerClassName match {
      case "org.apache.kafka.common.serialization.StringDeserializer" => {
        classOf[org.apache.kafka.common.serialization.StringDeserializer]
      }
      case "org.apache.kafka.common.serialization.ByteArrayDeserializer" => {
        classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer]
      }
      case _ => {
        throw new Exception(s"UnSupported DeSerializer Class Requested : ${deserializerClassName}")
      }
    }
  }

  /**
    * Gets the Appropriate De/Serializer Class
    *
    * @param serDe Name of the De/Serializer Class
    * @return De/Serializer Class
    */

  def getSerDe(serDe: String): Class[_ >: StringDeserializer
    with ByteArrayDeserializer with StringSerializer with ByteArraySerializer <: Closeable] = {
    serDe match {
      case "org.apache.kafka.common.serialization.StringDeserializer" => {
        classOf[org.apache.kafka.common.serialization.StringDeserializer]
      }
      case "org.apache.kafka.common.serialization.ByteArrayDeserializer" => {
        classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer]
      }
      case "org.apache.kafka.common.serialization.StringSerializer" => {
        classOf[org.apache.kafka.common.serialization.StringSerializer]
      }
      case "org.apache.kafka.common.serialization.ByteArraySerializer" => {
        classOf[org.apache.kafka.common.serialization.ByteArraySerializer]
      }
      case _ => {
        throw new Exception(s"UnSupported serDe Class Requested : ${serDe}")
      }
    }
  }

  /**
    * Converts RDD[WrappedData] to DataFrame with just 1 column -
    * which is the entire message String from Kafka
    *
    * @param sqlContext  SQLContext
    * @param columnAlias Name of Column in DataFrame
    * @param wrappedData WrappedData
    * @return DataFrame
    */
  def wrappedStringDataToDF(columnAlias: String, sqlContext: SQLContext
                            , wrappedData: RDD[WrappedData]): DataFrame = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    logger.info("Attempting to Convert Value in Wrapped Data to String Type")
    try {
      val rdd: RDD[(String, String)] = wrappedData.map { x =>
        (x.key.asInstanceOf[String], x.value.asInstanceOf[String])
      }
      val df = rddAsDF(sqlContext, columnAlias, rdd)
      logger.info("Completed --> Convert Value to String Type")
      df
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }

  }

  /**
    * Completely Clear the CheckPointed Offsets, leading to Read from Earliest offsets from Kafka
    *
    * @param zkHost Zookeeper Host
    * @param zkNodes Zookeeper Path
    * @param msg    Some Message or A Reason for Clearing CheckPoint
    */
  def clearCheckPoint(zkHost: String, zkNodes: Seq[String], msg: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val zk = ZooKeeperHostAndNodes(zkHost, zkNodes)
    zk.deleteZkCheckPoint()
  }


  /**
    * Gets the Latest CheckPoint from Zookeeper, if available
    *
    * @param zkHost Host Server for Zookeeper
    * @param zkNodes Node where we want to checkPoint
    * @return Option[Array[OffsetRange]
    */

  def getLastCheckPointFromZK(zkHost: String, zkNodes: Seq[String]): Option[Array[OffsetRange]] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    try {
      val zk = ZooKeeperHostAndNodes(zkHost, zkNodes)
      val lastCheckPoint: Option[Array[OffsetRange]] = zk.fetchZkCheckPoint
      lastCheckPoint
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

  /**
    * Function Gets
    * Either : The difference between lastCheckPoint & latestOffsetRange
    * Or : latestOffsetRange from Kafka
    *
    * @param lastCheckPoint       savedCheckPoint, if available
    * @param availableOffsetRange latestOfffsetRange from Kafka
    * @param fetchRowsOnFirstRun  This will be used if reading from kafka without
    *                             any prior checkpoint,
    *                             to ensure we read only last N messages
    *                             from topic as requested by client
    * @return Array[OffsetRange]
    */

  def getNewOffsetRangeForReader(lastCheckPoint: Option[Array[OffsetRange]]
                                 , availableOffsetRange: Array[OffsetRange]
                                 , fetchRowsOnFirstRun: Long): Array[OffsetRange] = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    try {
      val newOffsetRangesForReader = lastCheckPoint match {
        case None => {
          logger.warning(
            s"""No CheckPoint Found.
                |Reader will attempt to fetch "from beginning" From Kafka !""".stripMargin)
          availableOffsetRange.map {
            eachOffsetRange =>
              val fromOffset = scala.math.min(fetchRowsOnFirstRun
                , eachOffsetRange.untilOffset - eachOffsetRange.fromOffset)
              logger.info(s"Since this is first run," +
                s" will try to fetch only ${fromOffset} rows from Kafka")
              OffsetRange(eachOffsetRange.topic, eachOffsetRange.partition
                , eachOffsetRange.untilOffset - fromOffset, eachOffsetRange.untilOffset)
          }
        }
        case Some(lastCheckPoint) => {
          logger.info("""Found CheckPoint """)
          (lastCheckPoint, availableOffsetRange).toNewOffsetRanges
        }
      }
      newOffsetRangesForReader
    }
    catch {
      case ex: Throwable => {
        ex.printStackTrace()
        throw ex
      }
    }
  }

  /**
    * Function Gets
    * a custom offset range as a JSON from the user defined properties
    * Converts it to an array of offset ranges and returns them
    *
    * @param kafkaTopics sequence of topics
    * @param offsetRange user given custom offset ranges, if available
    * @return Array[OffsetRange]
    */

  def getCustomOffsetRangeForReader(kafkaTopics: Seq[String], offsetRange: String, consumerMode: String): Array[OffsetRange] = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    try {
      val offsetRangeObject = offsetRange.parseJson.convertTo[Seq[OffsetProperties]]
      val finalOffsetRanges = offsetRangeObject.flatMap {
        eachTopicRange =>
          eachTopicRange.offsetRange.map {
            eachOffsetRange => {
              var toOffset = 0L
              if (consumerMode == KafkaConstants.gimelAuditRunTypeStream) {
                toOffset = eachOffsetRange.to.getOrElse(-1)
              }
              else if (consumerMode == KafkaConstants.gimelAuditRunTypeBatch) {
                toOffset = eachOffsetRange.to.get
              }
              if(!kafkaTopics.contains(eachTopicRange.topic)) {
                throw new Exception("The topic specified in custom offset range does not match the subscribed topic! Please unset the previous value or check your properties")
              }
              OffsetRange(eachTopicRange.topic, eachOffsetRange.partition, eachOffsetRange.from, toOffset)
            }
          }
      }.toArray
      finalOffsetRanges
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

  /**
    * Converts an RDD[Wrapped Data] into RDD[GenericRecord]
    *
    * @param wrappedDataRDD   RDD[WrappedData]
    * @param avroSchemaKey    AvroSchemaKey | Example flights , flights.flights_log
    * @param avroSchemaURL    Confluent Schema Registry URL:Port
    * @param avroSchemaSource Specifies whether schema is inline text or from CDH schema registry
    * @param avroSchemaString Avro Schema String for flights
    * @param isStreamParallel true indicates : can repartition data for parallelism.
    *                         false is usually set for preserving ordering of data
    *                         as received from kafka
    * @param streamParallels  Repartition factor, for example : 10 indicates repartition to
    *                         10 executors
    * @return RDD[GenericRecord]
    */
  def wrappedDataToAvro(wrappedDataRDD: RDD[WrappedData], avroSchemaKey: String,
                        avroSchemaURL: String,
                        avroSchemaSource: String, avroSchemaString: String,
                        isStreamParallel: Boolean, streamParallels: Int,
                        cdhAllSchemaDetails: Option[Map[String,
                          (String, mutable.Map[Int, String])]]): RDD[GenericRecord] = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    try {
      val parallelRDD = if (isStreamParallel) {
        wrappedDataRDD.repartition(streamParallels)
      } else {
        wrappedDataRDD
      }
      val avroRecord: RDD[GenericRecord] = parallelRDD.map {
        x => bytesToGenericRecord(x.value.asInstanceOf[Array[Byte]], avroSchemaString)
      }
      val finalAvroRecord = avroSchemaSource.toUpperCase() match {
        case "CDH" =>
          deserializeCurRec(avroRecord, cdhAllSchemaDetails)
        case _ => avroRecord
      }
      finalAvroRecord
    }
    catch {
      case ex: Throwable => {
        ex.printStackTrace()
        throw ex
      }
    }
  }

  /**
    * Fetches the Schema for each Topic with version
    *
    * @param schemaSubject Schema Key
    * @param avroSchemaURL Confluent Schema URL
    * @return Map of Topic -> (Version & Schema)
    */

  def getAllSchemasForSubject(schemaSubject: String, avroSchemaURL: String)
  : (String, mutable.Map[Int, String]) = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val schemaLookup: scala.collection.mutable.Map[Int, String] = scala.collection.mutable.Map()
    val schemaRegistryClient = new ConfluentSchemaRegistry(avroSchemaURL)
    val k = schemaRegistryClient.getAllVersions(schemaSubject).asScala
    val k2 = k.map { eachVersion =>
      val version = eachVersion.toString.toInt
      version -> schemaRegistryClient.getVersion(schemaSubject, version).getSchema
    }.toMap
    k2.foreach(entry => schemaLookup.put(entry._1, entry._2))
    val latestSchema = schemaRegistryClient.getLatestVersion(schemaSubject).getSchema
    (latestSchema, schemaLookup)
  }


  /**
    * Deserialize the CDH record (bytes) , get GenericRecord
    *
    * @param avroRecord          Avro GenericRecord RDD
    * @param cdhAllSchemaDetails All the Subjects with LatestSchema and EachVersion
    * @return Avro GenericRecord RDD
    */
  def deserializeCurRec(avroRecord: RDD[GenericRecord]
                        , cdhAllSchemaDetails: Option[Map[String,
    (String, mutable.Map[Int, String])]]): RDD[GenericRecord] = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val additionalFields = getAdditionalFields()
    try {
      val actualRecord: RDD[GenericRecord] = avroRecord.map { eachRecord =>
        val eachRecordSchemaSubject = eachRecord.get("schemaSubject").toString
        val schemaThisRec = cdhAllSchemaDetails.get(eachRecordSchemaSubject)._1
        val eachRec: Array[Byte] = eachRecord.get("currentRecord").asInstanceOf[ByteBuffer].array()
        var metaColumnsMap = scala.collection.immutable.Map[String, String]()
        // Add mandatory meta columns,  gg commit timestamp, rba and opType
        additionalFields.foreach {
          field => metaColumnsMap += (field._1 -> eachRecord.get(field._2).toString)
        }
        val genericRecord: GenericRecord = bytesToGenericRecord(eachRec, schemaThisRec)
        val newSchema = addAdditionalFieldsToSchema(additionalFields.keySet.toList, schemaThisRec)
        val newGenericRecord = copyToGenericRecord(genericRecord, schemaThisRec, newSchema)
        metaColumnsMap.foreach { kv => newGenericRecord.put(kv._1, kv._2) }
        newGenericRecord
      }
      actualRecord
    }
    catch {
      case ex: Throwable => {
        ex.printStackTrace()
        throw ex
      }
    }
  }

  /**
    * Lists Additional fields to pick from CDH metadata record.
    *
    * @return List of Metadata columns
    */
  def getAdditionalFields(): scala.collection.immutable.Map[String, String] =
  scala.collection.immutable.Map("gg_commit_timestamp" -> "opTs"
    , "opt_type" -> "opType", "trail_seq_no" -> "trailSeqno", "trail_rba" -> "trailRba")


  /**
    * Adds additional fields to the Avro Schem
    *
    * @param additionalFields List of fields to Add
    * @param schemaString     Input Avro Schema
    * @return Updated Avro Schema String
    */
  def addAdditionalFieldsToSchema(additionalFields: List[String], schemaString: String)
  : String = {
    // Parse as JsValue
    val schemaAsJsVal = schemaString.parseJson
    // Convert to JsObject
    val schemaAsJsObject = schemaAsJsVal.asJsObject
    // Get the Map of each element & Value
    val schemaElementsMap: Map[String, JsValue] = schemaAsJsObject.fields
    // These fields will be added with "to-add" fields
    val schemaFields = schemaAsJsObject.getFields("fields").head.convertTo[Seq[JsValue]]
    val additionalFieldsJSON: List[String] = additionalFields.map {
      x => s"""{"name":"${x}","type":["null","string"]}""".stripMargin
    } // "to-add" fields
    val additionalFieldsAsJsVal: List[JsValue] = additionalFieldsJSON.map { x => x.parseJson }
    // added both fields
    val combinedFields: Seq[JsValue] = schemaFields ++ additionalFieldsAsJsVal
    // formation of a String so it can be inferred as JsVal
    val combinedFieldsAsString = combinedFields.map {
      x => x.asJsObject.compactPrint
    }.mkString("[", ",", "]")
    val combinedFieldsAsJsValue = combinedFieldsAsString.parseJson
    val toOverride = scala.collection.Map("fields" -> combinedFieldsAsJsValue)
    val k12 = schemaElementsMap ++ toOverride
    k12.toJson.compactPrint
  }

  /**
    * Get the Column Alias Name for a Given Single Column DF to be read from Kafka Topic
    * that has human readable message
    *
    * @param conf KafkaClientConfiguration
    * @return column alias name
    */
  def kafkaMessageColumnAlias(conf: KafkaClientConfiguration): String = {
    conf.tableProps.getOrElse("kafka.message.column.alias", "message").toString
  }


  /**
    * InTakes RDD And Converts to DataFrame
    *
    * @param sqlContext         SQL Context
    * @param messageColumnAlias Message Column Name
    * @param rdd                RDD[(String,String)]
    * @return DataFrame
    */
  def stringRddAsDF(sqlContext: SQLContext, messageColumnAlias: String
                    , rdd: RDD[(String, String)]): DataFrame = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    try {
      val dataIntermediate = sqlContext.createDataFrame(rdd)
        .withColumnRenamed("_2", "message")
        .withColumnRenamed("_1", "key")
      val df = dataIntermediate.select("message").withColumnRenamed("message", messageColumnAlias)
      df
    }
    catch {
      case ex: Throwable => {
        ex.printStackTrace()
        logger.error(s"Failed While Attempting to Convert RDD to DF")
        throw ex
      }
    }
  }

  /**
    * Converts RDD[WrappedData] to DataFrame
    *
    * @param sqlContext                SQLContext
    * @param valueMessageType          Message Type From Kafka - such as string, json, binary..
    * @param keySerializer             Key Serializer
    * @param valueSerializer           Value Serializer
    * @param rdd                       RDD[Wrapped Data]
    * @param kafkaValueMessageColAlias Column Alias in DataFrame for Messages from Kafka
    * @param avroSchemaString          Avro Schema String for Derserialization
    * @param avroSchemaSource          Avro Schema Source such as Inline or CDH Confluent Schema Registry
    * @param cdhTopicSchemaMetadata    CDH TopicSchema Details
    * @param cdhAllSchemaDetails       The Topic , Version, Schema information
    * @return DataFrame
    */

  def rddToDF(sqlContext: SQLContext
              , valueMessageType: Option[String]
              , keySerializer: String
              , valueSerializer: String
              , rdd: RDD[WrappedData]
              , kafkaValueMessageColAlias: String = "value"
              , avroSchemaString: String
              , avroSchemaSource: String
              , cdhTopicSchemaMetadata: Option[String]
              , cdhAllSchemaDetails: Option[Map[String, (String, mutable.Map[Int, String])]])
  : DataFrame = {
    (valueMessageType, valueSerializer) match {
      // Bytes Messages
      case (Some("binary"), "org.apache.kafka.common.serialization.ByteArraySerializer") =>
        val rDD = rdd.map { x => (x.key.asInstanceOf[String], x.value.asInstanceOf[Array[Byte]]) }
        //        logger.info("Byte Messages -->");
        //        rDD.cache.collect.take(10).foreach(x => logger.info(x))
        val columnAlias = kafkaValueMessageColAlias
        byteRddAsDF(sqlContext, columnAlias, rDD)
      // String Messages
      case (Some("string"), "org.apache.kafka.common.serialization.StringSerializer") =>
        val rDD = rdd.map { x => (x.key.asInstanceOf[String], x.value.asInstanceOf[String]) }
        //        logger.info("String Messages -->");
        //        rDD.cache.collect.take(10).foreach(x => logger.info(x))
        val columnAlias = kafkaValueMessageColAlias
        stringRddAsDF(sqlContext, columnAlias, rDD)
      // JSON Messages
      case (Some("json"), "org.apache.kafka.common.serialization.StringSerializer") =>
        val rDD: RDD[String] = rdd.map { x => x.value.asInstanceOf[String] }
        //        logger.info("JSON Messages -->");
        //        rDD.cache.collect.take(10).foreach(x => logger.info(x))
        sqlContext.read.json(rDD)
      // Avro - CDH | Generic Avro
      case (_, "org.apache.kafka.common.serialization.ByteArraySerializer") =>
        val rDD = rdd.map { x => (x.key, x.value.asInstanceOf[Array[Byte]]) }
        //        logger.info("Raw Messages -->");
        //        rDD.cache.collect.take(10).foreach(x => logger.info(x))
        val avroRecord: RDD[GenericRecord] = rDD.map { x =>
          bytesToGenericRecord(x._2, avroSchemaString)
        }
        val (finalAvroRecord, finalSchema) = avroSchemaSource.toUpperCase() match {
          case KafkaConstants.gimelKafkaAvroSchemaCDH => {
            val newSchemaCDH = addAdditionalFieldsToSchema(getAdditionalFields().keySet.toList
              , cdhTopicSchemaMetadata.get)
            (deserializeCurRec(avroRecord, cdhAllSchemaDetails), newSchemaCDH)
          }
          case _ => (avroRecord, avroSchemaString)
        }
        genericRecordtoDF(sqlContext, finalAvroRecord, finalSchema)
      // Other Types
      case _ => throw new Exception("Unsupported Configuration or Serialization Techniques")
    }
  }

  /**
    * Returns A Wrapped Message from Kafka
    *
    * @param sqlContext         SQLContext
    * @param conf               KafkaClientConfiguration
    * @param parallelizedRanges Array[OffsetRange]
    * @return RDD[WrappedData]
    */

  def getFromKafkaAsWrappedData(sqlContext: SQLContext
                                , conf: KafkaClientConfiguration
                                , parallelizedRanges: Array[OffsetRange]
                               ): RDD[WrappedData] = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val (avroSchemaString, avroSchemaKey, avroSchemaURL) = (conf.avroSchemaString
      , conf.avroSchemaKey
      , conf.avroSchemaURL
      )
    val kafkaParams: java.util.Map[String, Object] = new java.util.HashMap()
    conf.kafkaConsumerProps.foreach { x => kafkaParams.put(x._1, x._2) }
    logger.info(s"Final Kafka Params --> ${kafkaParams.asScala.mkString("\n", "\n", "\n")}")
    logger.info(
      s"""kafka.message.value.type --> ${conf.kafkaMessageValueType}
          |\nValue Serializer --> ${conf.kafkaValueSerializer}""".stripMargin
    )
    try {

      val rdd: RDD[_ >: (String, Array[Byte]) with (String, String) <: (String, Serializable)] =
        (conf.kafkaMessageValueType, conf.kafkaValueSerializer) match {
          // Bytes Messages
          case (Some("binary"), "org.apache.kafka.common.serialization.ByteArraySerializer") =>
            val rDDConsumerRec: RDD[ConsumerRecord[String, Array[Byte]]] =
              createRDD[String, Array[Byte]](
                sqlContext.sparkContext, kafkaParams
                , parallelizedRanges, LocationStrategies.PreferConsistent)
            rDDConsumerRec.map { x => (x.key(), x.value()) }
          // String Messages
          case (Some("string"), "org.apache.kafka.common.serialization.StringSerializer") =>
            val rDDConsumerRec: RDD[ConsumerRecord[String, String]] =
              createRDD[String, String](sqlContext.sparkContext
                , kafkaParams, parallelizedRanges, LocationStrategies.PreferConsistent)
            rDDConsumerRec.map { x => (x.key(), x.value()) }
          // JSON Messages
          case (Some("json"), "org.apache.kafka.common.serialization.StringSerializer") =>
            val rDDConsumerRec: RDD[ConsumerRecord[String, String]] =
              createRDD[String, String](sqlContext.sparkContext
                , kafkaParams, parallelizedRanges, LocationStrategies.PreferConsistent)
            rDDConsumerRec.map { x => (x.key(), x.value()) }
          // Avro - CDH | Generic Avro
          case (_, "org.apache.kafka.common.serialization.ByteArraySerializer") =>
            val rDDConsumerRec: RDD[ConsumerRecord[String, Array[Byte]]] =
              createRDD[String, Array[Byte]](sqlContext.sparkContext
                , kafkaParams, parallelizedRanges, LocationStrategies.PreferConsistent)
            rDDConsumerRec.map { x => (x.key(), x.value()) }
          // Other Types
          case _ => throw new Exception("Unsupported Configuration or Serialization Techniques")
        }

      rdd.map(x => WrappedData(x._1, x._2))
    }
    catch {
      case ex: Throwable => {
        ex.printStackTrace()
        val messageString =
          s"""kafkaParams --> ${kafkaParams.asScala.mkString(" \n ")}""".stripMargin
        logger.error(s"Unable to Fetch from Kafka for given parameters -->  ${messageString}")
        throw ex
      }
    }
  }

  /**
    * Returns DataFrame -fetching messages from Kafka
    *
    * @param sqlContext         SQLContext
    * @param conf               KafkaClientConfiguration
    * @param parallelizedRanges Array[OffsetRange]
    * @return DataFrame
    */

  def getAsDFFromKafka(sqlContext: SQLContext, conf: KafkaClientConfiguration
                       , parallelizedRanges: Array[OffsetRange]): DataFrame = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val kafkaParams: java.util.Map[String, Object] = new java.util.HashMap()
    conf.kafkaConsumerProps.foreach { x => kafkaParams.put(x._1, x._2) }
    logger.info(s"Final Kafka Params --> ${kafkaParams.asScala.mkString("\n", "\n", "\n")}")
    logger.info(
      s"""kafka.message.value.type --> ${conf.kafkaMessageValueType}
          |\nValue Serializer --> ${conf.kafkaValueSerializer}""".stripMargin)
    val wrappedDataRdd: RDD[WrappedData] = getFromKafkaAsWrappedData(sqlContext, conf, parallelizedRanges)
    rddToDF(sqlContext, conf.kafkaMessageValueType, conf.kafkaKeySerializer
      , conf.kafkaValueSerializer, wrappedDataRdd, "value", conf.avroSchemaString
      , conf.avroSchemaSource, conf.cdhTopicSchemaMetadata, conf.cdhAllSchemaDetails)
  }

  /**
    * Converts Avro RDD to Spark DataFrame
    *
    * @param avroRecord             RDD Generic Record
    * @param sqlContext             SQLContext
    * @param avroSchemaString       Avro Schema String
    * @param avroSchemaSource       Avro Schema Source
    * @param cdhTopicSchemaMetadata CDH Topic Metadata Details
    * @param cdhAllSchemaDetails    CDH Schema Details (Keys, Schemas..)
    * @return DataFrame
    */

  @deprecated
  def avroToDF1(avroRecord: RDD[GenericRecord]
                , sqlContext: SQLContext
                , avroSchemaString: String
                , avroSchemaSource: String
                , cdhTopicSchemaMetadata: Option[String]
                , cdhAllSchemaDetails: Option[Map[String, (String, mutable.Map[Int, String])]])
  : DataFrame = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val (finalAvroRecord, finalSchema) = avroSchemaSource match {
      case KafkaConstants.gimelKafkaAvroSchemaCDH => {
        val newSchemaCDH = addAdditionalFieldsToSchema(getAdditionalFields().keySet.toList
          , cdhTopicSchemaMetadata.get)
        (deserializeCurRec(avroRecord, cdhAllSchemaDetails), newSchemaCDH)
      }
      case _ => (avroRecord, avroSchemaString)
    }
    val df = genericRecordtoDF(sqlContext, finalAvroRecord, finalSchema)
    df
  }

  /**
    * InTakes RDD And Converts to DataFrame
    *
    * @param sqlContext         SQL Context
    * @param messageColumnAlias Message Column Name
    * @param rdd                RDD[(String, String)]
    * @return DataFrame
    */
  def rddAsDF(sqlContext: SQLContext, messageColumnAlias: String
              , rdd: RDD[(String, String)]): DataFrame = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      val dataIntermediate = sqlContext.createDataFrame(rdd)
        .withColumnRenamed("_2", "message").withColumnRenamed("_1", "key")
      dataIntermediate.select("message").withColumnRenamed("message", messageColumnAlias)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Failed While Attempting to Convert RDD to DF")
        throw ex
    }
  }

  /**
    * InTakes RDD And Converts to DataFrame
    *
    * @param sqlContext         SQL Context
    * @param messageColumnAlias Message Column Name
    * @param rdd                RDD[(String,Array[Byte])]
    * @return DataFrame
    */
  def byteRddAsDF(sqlContext: SQLContext, messageColumnAlias: String
                  , rdd: RDD[(String, Array[Byte])]): DataFrame = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    try {
      val dataIntermediate = sqlContext.createDataFrame(rdd)
        .withColumnRenamed("_2", "message").withColumnRenamed("_1", "key")
      dataIntermediate.select("message").withColumnRenamed("message", messageColumnAlias)
    }
    catch {
      case ex: Throwable => {
        ex.printStackTrace()
        logger.error(s"Failed While Attempting to Convert RDD to DF")
        throw ex
      }
    }
  }

  /**
    * Creates a Topic in Kafka if it does not exists
    *
    * @param zookKeeperHostAndPort Zookeeper Host & Port | Example localhost:2181
    * @param kafkaTopicName        Kafka Topic Name
    * @param numberOfPartitions    Number of Partitions
    * @param numberOfReplica       Number of Replicas
    */
  def createTopicIfNotExists(zookKeeperHostAndPort: String, kafkaTopicName: String
                             , numberOfPartitions: Int, numberOfReplica: Int): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    KafkaAdminUtils.createTopicIfNotExists(
      zookKeeperHostAndPort
      , kafkaTopicName
      , numberOfPartitions
      , numberOfReplica
    )
  }

  /**
    * Delete a Topic if it exists
    *
    * @param zookKeeperHostAndPort Zookeeper Host & Port | Example localhost:2181
    * @param kafkaTopicName        Kafka Topic Name
    */
  def deleteTopicIfExists(zookKeeperHostAndPort: String, kafkaTopicName: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    storageadmin.KafkaAdminUtils.deleteTopicIfExists(
      zookKeeperHostAndPort
      , kafkaTopicName
    )
  }
}

/**
  * Custom Exception for KafkaUtilities related errors
  *
  * @param message Message to Throw
  * @param cause   A Throwable Cause
  */
class KafkaUtilitiesException(message: String, cause: Throwable)
  extends RuntimeException(message) {
  if (cause != null) {
    initCause(cause)
  }

  def this(message: String) = this(message, null)
}
