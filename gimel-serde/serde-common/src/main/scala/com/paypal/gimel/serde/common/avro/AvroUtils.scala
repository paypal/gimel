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

package com.paypal.gimel.serde.common.avro

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.collection.mutable

import com.databricks.spark.avro.SchemaConverters._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.json.JsValue

import com.paypal.gimel.logger.Logger
import com.paypal.gimel.serde.common.schema.SchemaRegistryLookUp

object AvroUtils extends Serializable {

  val logger = Logger()

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
    * @param writerSchema    Avro Schema String used by writer
    * @param readerSchema    Avro Schema String used by Reader
    * @return An Avro Generic Record
    */

  def bytesToGenericRecordWithSchemaRecon(serializedBytes: Array[Byte], writerSchema: String, readerSchema: String): GenericRecord = {

    try {

      // Build Avro Schema From String - for writer and reader schema
      val writerAvroSchema: Schema = (new Schema.Parser).parse(writerSchema)
      val readerAvroSchema: Schema = (new Schema.Parser).parse(readerSchema)
      // Initiate AVRO Reader from Factory
      val reader = new org.apache.avro.generic.GenericDatumReader[GenericRecord](writerAvroSchema, readerAvroSchema)
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
    * Adds additional fields to the Avro Schema
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
    * Deserializes the given column in input dataframe
    *
    * @param dataframe Dataframe
    * @param columnToDeserialize Column to deserialize in dataframe
    * @param avroSchemaString Avro Schema for deserializing the columnToDeserialize
    * @param newAvroSchemaString Avro schema with deserialized along with all existing dataframe columns
    * @return Deserialized RDD
    */
  def getDeserializedRDD(dataframe: DataFrame, columnToDeserialize: String, avroSchemaString: String, newAvroSchemaString: String): RDD[GenericRecord] = {
    val originalFields = dataframe.columns.filter(field => field != columnToDeserialize)
    try {
      dataframe.rdd.map { eachRow =>
        val recordToDeserialize: Array[Byte] = eachRow.getAs(columnToDeserialize).asInstanceOf[Array[Byte]]
        val originalColumnsMap = originalFields.map {
          field => {
            val index = eachRow.fieldIndex(field)
            if (eachRow.isNullAt(index)) {
              (field -> "null")
            } else {
              (field -> eachRow.getAs(field).toString)
            }
          }
        }
        val deserializedRecord = bytesToGenericRecordWithSchemaRecon(recordToDeserialize, avroSchemaString, avroSchemaString)
        val newDeserializedRecord = copyToGenericRecord(deserializedRecord, avroSchemaString, newAvroSchemaString)
        originalColumnsMap.foreach { kv => newDeserializedRecord.put(kv._1, kv._2) }
        newDeserializedRecord
      }
    } catch {
      case ex: Throwable => {
        ex.printStackTrace()
        throw ex
      }
    }
  }

  /**
    * Deserializes the given column in input dataframe
    *
    * @param dataframe Dataframe
    * @param columnToDeserialize Column to deserialize in dataframe
    * @param avroSchemaString Avro Schema for deserializing the columnToDeserialize
    * @return Deserialized RDD
    */
  def getDeserializedDataFrame(dataframe: DataFrame, columnToDeserialize: String, avroSchemaString: String): DataFrame = {
    val originalFields: Array[String] = dataframe.columns.filter(field => field != columnToDeserialize)
    val newAvroSchemaString = addAdditionalFieldsToSchema(originalFields.toList, avroSchemaString)

    try {
      dataframe.map { eachRow =>
        val recordToDeserialize: Array[Byte] = eachRow.getAs(columnToDeserialize).asInstanceOf[Array[Byte]]
        val originalColumnsMap = originalFields.map {
          field => {
            val index = eachRow.fieldIndex(field)
            if (eachRow.isNullAt(index)) {
              (field -> "null")
            } else {
              (field -> eachRow.getAs(field).toString)
            }
          }
        }
        val deserializedGenericRecord: GenericRecord = bytesToGenericRecordWithSchemaRecon(recordToDeserialize, avroSchemaString, avroSchemaString)
        val newDeserializedGenericRecord: GenericRecord = copyToGenericRecord(deserializedGenericRecord, avroSchemaString, newAvroSchemaString)
        originalColumnsMap.foreach { kv => newDeserializedGenericRecord.put(kv._1, kv._2) }
        val avroSchemaObj: Schema = (new Schema.Parser).parse(newAvroSchemaString)
        val converter = AvroToSQLSchemaConverter.createConverterToSQL(avroSchemaObj)
        converter(newDeserializedGenericRecord).asInstanceOf[Row]
      } {
        val avroSchema: Schema = (new Schema.Parser).parse(newAvroSchemaString)
        val schemaType: SchemaType = toSqlType(avroSchema)
        val encoder = RowEncoder(schemaType.dataType.asInstanceOf[StructType])
        encoder
      }.toDF
    } catch {
      case ex: Throwable => {
        ex.printStackTrace()
        throw ex
      }
    }
  }

  /**
    * Deserializes the given column in input dataframe
    *
    * @param dataframe Dataframe
    * @param columnToDeserialize Column to deserialize in dataframe
    * @param avroSchemaString Avro Schema for deserializing the columnToDeserialize
    * @param avroSchemaURL Schema Registry URL
    * @param avroAllSubjectSchemas All schemas for a subject from Schema Registry
    * @return Deserialized RDD
    */
  def getDeserializedRDD(dataframe: DataFrame, columnToDeserialize: String, avroSchemaString: String, avroSchemaURL: String, avroAllSubjectSchemas: mutable.Map[Int, String]): RDD[GenericRecord] = {
    val originalFields = dataframe.columns.filter(field => field != columnToDeserialize)
    try {
      dataframe.rdd.map { eachRow =>
        val recordToDeserialize: Array[Byte] = eachRow.getAs(columnToDeserialize).asInstanceOf[Array[Byte]]
        val originalColumnsMap = originalFields.map {
          field => {
            val index = eachRow.fieldIndex(field)
            if (eachRow.isNullAt(index)) {
              (field -> "null")
            } else {
              (field -> eachRow.getAs(field).toString)
            }
          }
        }
        val eachRecordSchemaSubject = eachRow.getAs("schemasubject").toString
        val eachRecordSchemaVersion = eachRow.getAs("schemaversion").toString.toInt
        val schemaThisRec = avroAllSubjectSchemas.get(eachRecordSchemaVersion) match {
          case None =>
            // If current record's version of schema is not available then lookup schema registry and cache
            val schemas = SchemaRegistryLookUp.getAllSchemasForSubject(eachRecordSchemaSubject, avroSchemaURL)._2
            schemas.foreach(x => avroAllSubjectSchemas.put(x._1, x._2))
            avroAllSubjectSchemas(eachRecordSchemaVersion)
          // If current record's schema is already cached, then reuse.
          case Some(x) => x
        }
        val schemaMaxVersion = avroAllSubjectSchemas(avroAllSubjectSchemas.keys.max)
        val deserializedRecord = bytesToGenericRecordWithSchemaRecon(recordToDeserialize, schemaThisRec, schemaMaxVersion)
        val newAvroSchemaString = addAdditionalFieldsToSchema(originalFields.toList, schemaMaxVersion)
        val newDeserializedRecord = copyToGenericRecord(deserializedRecord, schemaMaxVersion, newAvroSchemaString)
        originalColumnsMap.foreach { kv => newDeserializedRecord.put(kv._1, kv._2) }
        newDeserializedRecord
      }
    } catch {
      case ex: Throwable => {
        ex.printStackTrace()
        throw ex
      }
    }
  }

  /**
    * Converts a DataFrame into RDD[Avro Generic Record]
    *
    * @param dataFrame        DataFrame
    * @param avroSchemaString Avro Schema String
    * @return RDD[GenericRecord]
    */

  def dataFrametoBytes(dataFrame: DataFrame, avroSchemaString: String): DataFrame = {
    import dataFrame.sparkSession.implicits._
    try {
      if (!isDFFieldsEqualAvroFields(dataFrame, avroSchemaString)) {
        throw new Exception(s"Incompatible DataFrame Schema Vs Provided Avro Schema.")
      }
      dataFrame.map { row =>
        val avroSchema = (new Schema.Parser).parse(avroSchemaString)
        val fields = avroSchema.getFields.asScala.map { x => x.name() }.toArray
        val cols: Map[String, Any] = row.getValuesMap(fields)
        val genericRecord: GenericRecord = new GenericData.Record(avroSchema)
        cols.foreach(x => genericRecord.put(x._1, x._2))
        genericRecordToBytes(genericRecord, avroSchemaString)
      }.toDF("value")
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
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
        throw ex
    }

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
}
