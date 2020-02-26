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

import java.nio.ByteBuffer
import java.util

import scala.collection.JavaConverters._

import org.apache.avro.{Schema}
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * This logic is borrowed from databricks spark-avro-2_10.jar to aid in the conversion of avro RDD to DataFrame.
  *
  * https://github.com/databricks/spark-avro/blob/master/src/main/scala/com/databricks/spark/avro/SchemaConverters.scala
  *
  * This object contains method that are used to convert sparkSQL schemas to avro schemas and vice versa.
  *
  * Note that original code has been enhanced. Please ensure notes are maintained for new additions to track deviations from original code.
  *
  * 2017-08-19 : Added support for Set(STRING, LONG) : This enabled Reading FTPI data
  */
object AvroToSQLSchemaConverter {

  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
    * This function takes an avro schema and returns a sql schema.
    */
  def toSqlType(avroSchema: Schema): SchemaType = {
    avroSchema.getType match {
      case INT =>
        SchemaType(IntegerType, nullable = false)
      case STRING =>
        SchemaType(StringType, nullable = false)
      case BOOLEAN =>
        SchemaType(BooleanType, nullable = false)
      case BYTES =>
        SchemaType(BinaryType, nullable = false)
      case DOUBLE =>
        SchemaType(DoubleType, nullable = false)
      case FLOAT =>
        SchemaType(FloatType, nullable = false)
      case LONG =>
        SchemaType(LongType, nullable = false)
      case FIXED =>
        SchemaType(BinaryType, nullable = false)
      case ENUM =>
        SchemaType(StringType, nullable = false)

      case RECORD =>
        val fields = avroSchema.getFields.asScala.map { f =>
          val schemaType = toSqlType(f.schema())
          StructField(f.name, schemaType.dataType, schemaType.nullable)
        }

        SchemaType(StructType(fields), nullable = false)

      case ARRAY =>
        val schemaType = toSqlType(avroSchema.getElementType)
        SchemaType(
          ArrayType(schemaType.dataType, containsNull = schemaType.nullable),
          nullable = false)

      case MAP =>
        val schemaType = toSqlType(avroSchema.getValueType)
        SchemaType(
          MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable),
          nullable = false)

      case UNION =>
        if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL).toList
          if (remainingUnionTypes.size == 1) {
            toSqlType(remainingUnionTypes.head).copy(nullable = true)
          } else {
            toSqlType(Schema.createUnion(remainingUnionTypes.asJava)).copy(nullable = true)
          }
        } else avroSchema.getTypes.asScala.map(_.getType) match {
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            SchemaType(LongType, nullable = false)
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            SchemaType(DoubleType, nullable = false)
          case other =>
            throw new UnsupportedOperationException(
              s"This mix of union types is not supported (see README): $other")
        }

      case other =>
        throw new UnsupportedOperationException(s"Unsupported type $other")
    }
  }

  /**
    * Returns a function that is used to convert avro types to their
    * corresponding sparkSQL representations.
    */
  def createConverterToSQL(schema: Schema): Any => Any = {
    schema.getType match {
      // Avro strings are in Utf8, so we have to call toString on them
      case STRING | ENUM =>
        (item: Any) => if (item == null) null else item.toString
      case INT | BOOLEAN | DOUBLE | FLOAT | LONG =>
        identity
      // Byte arrays are reused by avro, so we have to make a copy of them.
      case FIXED =>
        (item: Any) =>
          if (item == null) {
            null
          } else {
            item.asInstanceOf[Fixed].bytes().clone()
          }
      case BYTES =>
        (item: Any) =>
          if (item == null) {
            null
          } else {
            val bytes = item.asInstanceOf[ByteBuffer]
            val javaBytes = new Array[Byte](bytes.remaining)
            bytes.get(javaBytes)
            javaBytes
          }
      case RECORD =>
        val fieldConverters = schema.getFields.asScala.map(f => createConverterToSQL(f.schema))
        (item: Any) =>
          if (item == null) {
            null
          } else {
            val record = item.asInstanceOf[GenericRecord]
            val converted = new Array[Any](fieldConverters.size)
            var idx = 0
            while (idx < fieldConverters.size) {
              converted(idx) = fieldConverters.apply(idx)(record.get(idx))
              idx += 1
            }
            Row.fromSeq(converted.toSeq)
          }
      case ARRAY =>
        val elementConverter = createConverterToSQL(schema.getElementType)
        (item: Any) =>
          if (item == null) {
            null
          } else {
            item.asInstanceOf[GenericData.Array[Any]].asScala.map(elementConverter)
          }
      case MAP =>
        val valueConverter = createConverterToSQL(schema.getValueType)
        (item: Any) =>
          if (item == null) {
            null
          } else {
            item.asInstanceOf[util.HashMap[Any, Any]].asScala.map { case (k, v) =>
              (k.toString, valueConverter(v))
            }.toMap
          }
      case UNION =>
        if (schema.getTypes.asScala.exists(_.getType == NULL)) {
          val remainingUnionTypes = schema.getTypes.asScala.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            createConverterToSQL(remainingUnionTypes.head)
          } else {
            createConverterToSQL(Schema.createUnion(remainingUnionTypes.asJava))
          }
        } else schema.getTypes.asScala.map(_.getType) match {
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            (item: Any) => {
              item match {
                case l: Long =>
                  l
                case i: Int =>
                  i.toLong
                case null =>
                  null
              }
            }
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            (item: Any) => {
              item match {
                case d: Double =>
                  d
                case f: Float =>
                  f.toDouble
                case null =>
                  null
              }
            }
          case Seq(t1, t2) if Set(t1, t2) == Set(STRING, LONG) =>
            (item: Any) => {
              // @todo This fix is pending as currently we are unable to convert Avro to Spark types for this combination (STRING, LONG). Wip !
              item match {
                case l: Long =>
                  l
                case js: org.apache.avro.util.Utf8 =>
                  js.toString
                case null =>
                  null
              }
            }
          case other =>
            throw new UnsupportedOperationException(
              s"This mix of union types is not supported (see README): $other")
        }
      case other =>
        throw new UnsupportedOperationException(s"invalid avro type: $other")
    }
  }

}
