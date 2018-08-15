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

package com.paypal.gimel.kafka.avro

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.avro.{specific, Schema}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._
import spray.json._
import spray.json.DefaultJsonProtocol._

import com.paypal.gimel.kafka.conf.KafkaClientConfiguration
import com.paypal.gimel.logger.Logger

/**
  * Avro - Spark Conversion operations are implemented here
  */

object SparkAvroUtilities {

  val logger = Logger()

  /**
    * Converts a DataFrame into RDD[Avro Generic Record]
    *
    * @param dataFrame        DataFrame
    * @param avroSchemaString Avro Schema String
    * @return RDD[GenericRecord]
    */

  def dataFrametoGenericRecord(dataFrame: DataFrame, avroSchemaString: String): RDD[GenericRecord] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      if (!isDFFieldsEqualAvroFields(dataFrame, avroSchemaString)) {
        throw new SparkAvroConversionException(s"Incompatible DataFrame Schema Vs Provided Avro Schema.")
      }
      dataFrame.rdd.map { row =>
        val avroSchema = (new Schema.Parser).parse(avroSchemaString)
        val fields = avroSchema.getFields.asScala.map { x => x.name() }.toArray
        val cols: Map[String, Any] = row.getValuesMap(fields)
        val genericRecord: GenericRecord = new GenericData.Record(avroSchema)
        cols.foreach(x => genericRecord.put(x._1, x._2))
        genericRecord
      }
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw new SparkAvroConversionException("Failed while converting DataFrame to Generic Record")
    }
  }

  /**
    * Converts an RDD[Avro GenericRecord] into a DataFrame
    *
    * @param sqlContext    SQLContext
    * @param genericRecRDD RDD[GenericRecord]
    * @param schemaString  The AVRO schema String
    * @return DataFrame
    */
  def genericRecordtoDF(sqlContext: SQLContext, genericRecRDD: RDD[GenericRecord], schemaString: String): DataFrame = {

    genericRecordToDFViaAvroSQLConvertor(sqlContext, genericRecRDD, schemaString)
  }

  /**
    * Converts an RDD[Avro GenericRecord] into a DataFrame
    *
    * @param sqlContext    SQLContext
    * @param genericRecRDD RDD[GenericRecord]
    * @param schemaString  The AVRO schema String
    * @return DataFrame
    */

  def genericRecordToDFViaAvroSQLConvertor(sqlContext: SQLContext, genericRecRDD: RDD[GenericRecord], schemaString: String): DataFrame = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    import com.databricks.spark.avro.SchemaConverters._
    try {
      val rowRDD: RDD[Row] = genericRecRDD.map { x =>
        val avroSchema: Schema = (new Schema.Parser).parse(schemaString)
        val converter = AvroToSQLSchemaConverter.createConverterToSQL(avroSchema)
        converter(x).asInstanceOf[Row]
      }
      val avroSchema: Schema = (new Schema.Parser).parse(schemaString)
      val schemaType = toSqlType(avroSchema)
      sqlContext.createDataFrame(rowRDD, schemaType.dataType.asInstanceOf[StructType])
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw new SparkAvroConversionException("Failed while converting Generic Record to DataFrame")
    }
  }

  /**
    * Compare Fields of Avro Schema with Fields of DataFrame
    * Return true if both match false if there is any mismatch
    * Also log/print the differences.
    *
    * @param dataFrame        DataFrame
    * @param avroSchemaString Avro Schema String
    * @return Boolean
    */
  def isDFFieldsEqualAvroFields(dataFrame: DataFrame, avroSchemaString: String): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      val dfFields = dataFrame.schema.fieldNames
      val avroSchema = (new Schema.Parser).parse(avroSchemaString)
      val avroFields = avroSchema.getFields.asScala.map { x => x.name() }.toArray
      val inDFMissingInAvro = dfFields.diff(avroFields)
      val inAvroMissingInDF = avroFields.diff(dfFields)
      val isMatching = inDFMissingInAvro.isEmpty && inAvroMissingInDF.isEmpty
      if (!isMatching) {
        val warningMessage =
          s"""
             |Provided Avro Fields --> ${avroFields.mkString(",")}
             |Determined DataFrame Fields --> ${dfFields.mkString(",")}
             |Missing Fields in Avro --> ${inDFMissingInAvro.mkString(",")}
             |Missing Fields in DataFrame --> ${inAvroMissingInDF.mkString(",")}
          """.stripMargin
        logger.warning(warningMessage)
      }
      isMatching
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw new SparkAvroConversionException(s"Failed While Comparing DF Fields match against Fields in Avro Schema String $avroSchemaString")
    }

  }

  /**
    * Gets the fields from a Avro Schema String
    *
    * @param avroSchema Avro Schema String
    * @return Fields
    */
  def getFieldsFromAvroSchemaString(avroSchema: String): Seq[String] = {
    val schemaAsJsVal = avroSchema.parseJson // parse as JsValue
    val schemaAsJsObject = schemaAsJsVal.asJsObject // Convert to JsObject
    val schemaFields = schemaAsJsObject.getFields("fields").head.convertTo[Seq[JsValue]]
    val existingFields = schemaFields.map { x => x.asJsObject.fields("name").toString().replace("\"", "") }
    existingFields
  }

  /**
    * DeSerialize an Avro Generic Record
    *
    * @param serializedBytes A Serialized Byte Array (serialization should have been done through Avro Serialization)
    * @param schemaString    An Avro Schema String
    * @return An Avro Generic Record
    */

  def bytesToGenericRecord(serializedBytes: Array[Byte], schemaString: String): GenericRecord = {

    try {
      // Build Avro Schema From String
      val avroSchema = (new Schema.Parser).parse(schemaString)
      // Initiate AVRO Reader from Factory
      val reader = new specific.SpecificDatumReader[GenericRecord](avroSchema)
      // Initiate a new Java Byte Array Input Stream
      val in = new ByteArrayInputStream(serializedBytes)
      // Get appropriate AVRO Decoder from Factory
      val decoder = DecoderFactory.get().binaryDecoder(in, null)
      // Get AVRO generic record
      val genericRecordRead = reader.read(null, decoder)
      genericRecordRead
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

  /**
    * Copies to a new generic record
    *
    * @param genericRecord    Input Generic Record
    * @param avroSchemaString Avro Schema that can be used to parse input Generic Record
    * @param newAvroString    New Avro Schema for the Outgoing Generic Record
    * @return Outgoing Generic Record copied from Input
    */
  def copyToGenericRecord(genericRecord: GenericRecord, avroSchemaString: String, newAvroString: String): GenericRecord = {
    val existingFields = getFieldsFromAvroSchemaString(avroSchemaString)
    val newAvroSchema = (new Schema.Parser).parse(newAvroString)
    val newGenericRec: GenericRecord = new GenericData.Record(newAvroSchema)
    existingFields.foreach(field => newGenericRec.put(field, genericRecord.get(field)))
    newGenericRec
  }

  /**
    * A Functionality to Perform 2nd level De Serialization in case the data is from CDH
    * This is necessary since Actual Data in CDH is wrapped by a Raw Record which get Deserialized when read from Kafka
    * When this functionality is called, we check if the data is CDH type, then do second level deserialization
    * If the data is not of CDH type, then we skip 2nd level deserialization
    *
    * @param avroRecordRDD RDD[GenericRecord]
    * @param conf          KafkaClientConfiguration
    * @return RDD[GenericRecord]
    */
  def deserializeCurrentRecord(avroRecordRDD: RDD[GenericRecord], conf: KafkaClientConfiguration): RDD[GenericRecord] = {
    val schemaRegistryClient = new RestService(conf.avroSchemaURL)
    val schemaLookup: scala.collection.mutable.Map[Int, String] = scala.collection.mutable.Map()
    val actualRecord = avroRecordRDD.map { eachRecord =>
      val eachRecordSchemaVersion: Int = eachRecord.get("schemaVersion").toString.toInt
      val schemaForThisRecord = schemaLookup.get(eachRecordSchemaVersion) match {
        case None =>
          val schema = schemaRegistryClient.getVersion(conf.avroSchemaKey, eachRecordSchemaVersion).getSchema
          schemaLookup.put(eachRecordSchemaVersion, schema)
          schema
        case Some(x) =>
          x
      }

      val eachRecordBytes: Array[Byte] = eachRecord.get("currentRecord").asInstanceOf[Array[Byte]]
      bytesToGenericRecord(eachRecordBytes, schemaForThisRecord)
    }
    actualRecord
  }

  /**
    * Serialize Avro GenericRecord into Byte Array
    *
    * @param rec          An Avro Generic Record
    * @param schemaString An Avro Schema String
    * @return Serialized Byte Array
    */

  def genericRecordToBytes(rec: GenericRecord, schemaString: String): Array[Byte] = {

    try {
      // Build Avro Schema From String
      val avroSchema = (new Schema.Parser).parse(schemaString)
      // Initiate a new Java Byte Array Output Stream
      val out = new ByteArrayOutputStream()
      // Get appropriate AVRO Decoder from Factory
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      // Write the Encoded data's output (Byte Array) into the Output Stream
      // Initiate AVRO Writer from Factory
      val writer = new SpecificDatumWriter[GenericRecord](avroSchema)
      writer.write(rec, encoder)
      // Flushes Data to Actual Output Stream
      encoder.flush()
      // Close the Output Stream
      out.close()
      val serializedBytes: Array[Byte] = out.toByteArray
      serializedBytes
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

  /**
    * Converts an RDD[Avro GenericRecord] into a DataFrame
    *
    * @param sqlContext    SQLContext
    * @param genericRecRDD RDD[GenericRecord]
    * @param schemaString  The AVRO schema String
    * @return DataFrame
    */
  def genericRecordToDataFrameViaJSON(sqlContext: SQLContext, genericRecRDD: RDD[GenericRecord], schemaString: String): DataFrame = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      val avroSchema: Schema = (new Schema.Parser).parse(schemaString)
      val fields: Seq[String] = avroSchema.getFields.asScala.map { x => x.name() }.toArray.toSeq
      sqlContext.read.json(genericRecRDD.map(_.toString)).selectExpr(fields: _*)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw new SparkAvroConversionException("Failed while converting Generic Record to DataFrame")
    }
  }

  /**
    * Custom Exception
    *
    * @param message Message to Throw
    * @param cause   A Throwable Cause
    */
  private class SparkAvroConversionException(message: String, cause: Throwable)
    extends RuntimeException(message) {
    if (cause != null) {
      initCause(cause)
    }

    def this(message: String) = this(message, null)
  }

}


