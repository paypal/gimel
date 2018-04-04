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

package com.paypal.gimel.hive.utilities

import org.apache.spark.sql.types.StructType

/**
  *
  * This functionality generates Hive Schema for a Given set of Table parameters
  * Functionality is to intake various parameters (below) and generate a Hive DDL
  * DatabaseName, TableName
  * TableType (External)
  * HDFS Location
  * Partitioning and Bucketing
  * Column Set generation
  * spark-sql Serde Tables such as org.apache.spark.sql.parquet or org.apache.spark.sql.json
  *
  */

object HiveSchemaUtils {


  /**
    * This function generates a Hive Table Schema
    *
    * @param tableType      indicates where table an External Table or Managed Table
    * @param databaseName   the Hive database name
    * @param tableName      the Hive table Name
    * @param tableLocation  Hive Table's HDFS path
    * @param storageFormat  underlying storage format such as parquet
    * @param columnSet      An Array of ColumnNames that can be used to generate the Column Set, Each column will be considered as format "String"
    * @param fieldDelimiter field delimiter : Example "\\010" or "\\t"
    * @param rowDelimiter   Row delimiter : Example "\\020" or "\\n"
    * @return Hive Table Schema that can be executed in sqlContext
    *
    *
    *
    */
  def generateTableDDL(tableType: String, databaseName: String, tableName: String, tableLocation: String, storageFormat: String,
                       columnSet: Array[String], fieldDelimiter: String, rowDelimiter: String): String = {

    val tableParams = Map(
      "varTableType" -> tableType,
      "varDatabaseName" -> databaseName,
      "varTableName" -> tableName,
      "varColumnSet" -> getColumnSet(getListOfFieldSpec(columnSet)),
      "varStorageFormat" -> storageFormat.toUpperCase(),
      "varLocation" -> tableLocation,
      "varFieldDelimiter" -> fieldDelimiter,
      "varRowDelimiter" -> rowDelimiter
    )

    generateTableDDL("RFDTable", tableParams)
  }

  /**
    * This function generates a Hive Table Schema
    *
    * @param tableType      indicates where table an External Table or Managed Table
    * @param databaseName   the Hive database name
    * @param tableName      the Hive table Name
    * @param tableLocation  Hive Table's HDFS path
    * @param storageFormat  underlying storage format such as parquet
    * @param columnSet      An Array of ColumnNames that can be used to generate the Column Set, Each column will be considered as format "String"
    * @param fieldDelimiter field delimiter : Example "\\010" or "\\t"
    * @param rowDelimiter   Row delimiter : Example "\\020" or "\\n"
    * @param partitionKey   The columnSet that should be used for partitioning.
    *                       Each column will be considered as type `String`
    * @return Hive Table Schema that can be executed in sqlContext
    *
    */
  def generateTableDDL(tableType: String, databaseName: String, tableName: String, tableLocation: String, storageFormat: String,
                       columnSet: Array[String], fieldDelimiter: String, rowDelimiter: String,
                       partitionKey: Array[String]): String = {

    val tableParams = Map(
      "varTableType" -> tableType,
      "varDatabaseName" -> databaseName,
      "varTableName" -> tableName,
      "varColumnSet" -> getColumnSet(getListOfFieldSpec(columnSet)),
      "varStorageFormat" -> storageFormat.toUpperCase(),
      "varLocation" -> tableLocation,
      "varFieldDelimiter" -> fieldDelimiter,
      "varRowDelimiter" -> rowDelimiter,
      "varPartitionColumnSet" -> getColumnSet(getListOfFieldSpec(partitionKey))
    )

    generateTableDDL("RFDTPartitionedTable", tableParams)
  }


  /**
    * This function generates a Hive Table Schema
    *
    * @param tableType      indicates where table an External Table or Managed Table
    * @param databaseName   the Hive database name
    * @param tableName      the Hive table Name
    * @param tableLocation  Hive Table's HDFS path
    * @param storageFormat  underlying storage format such as parquet
    * @param columnSet      An Array of ColumnNames that can be used to generate the Column Set, Each column will be considered as format "String"
    * @param fieldDelimiter field delimiter : Example "\\010" or "\\t"
    * @param rowDelimiter   Row delimiter : Example "\\020" or "\\n"
    * @param partitionKey   The columnSet that should be used for partitioning.
    *                       Each column will be considered as type `String`
    * @param clusterKey     A comma-separated list of columns that should be used for clustering.
    *                       Example "id, date"
    * @param clusterNum     Number of Buckets that should be created
    * @return Hive Table Schema that can be executed in sqlContext
    *
    */
  def generateTableDDL(tableType: String, databaseName: String, tableName: String, tableLocation: String, storageFormat: String,
                       columnSet: Array[String], fieldDelimiter: String, rowDelimiter: String,
                       partitionKey: Array[String], clusterKey: String, clusterNum: String): String = {

    val tableParams = Map(
      "varTableType" -> tableType,
      "varDatabaseName" -> databaseName,
      "varTableName" -> tableName,
      "varColumnSet" -> getColumnSet(getListOfFieldSpec(columnSet)),
      "varStorageFormat" -> storageFormat.toUpperCase(),
      "varLocation" -> tableLocation,
      "varFieldDelimiter" -> fieldDelimiter,
      "varRowDelimiter" -> rowDelimiter,
      "varClusterKey" -> clusterKey,
      "varClusterCount" -> clusterNum,
      "varPartitionColumnSet" -> getColumnSet(getListOfFieldSpec(partitionKey))
    )

    generateTableDDL("RFDTPartitionedBucketedTable", tableParams)
  }


  /**
    * This function generates a Hive Table Schema
    *
    * @param tableType     indicates where table an External Table or Managed Table
    * @param databaseName  the Hive database name
    * @param tableName     the Hive table Name
    * @param tableLocation Hive Table's HDFS path
    * @param storageFormat underlying storage format such as parquet
    * @param columnSet     An Array of ColumnNames that can be used to generate the Column Set, Each column will be considered as format "String"
    * @return Hive Table Schema that can be executed in sqlContext
    *
    */
  def generateTableDDL(tableType: String, databaseName: String, tableName: String, tableLocation: String, storageFormat: String,
                       columnSet: Array[String]): String = {

    val tableParams = Map(
      "varTableType" -> tableType,
      "varDatabaseName" -> databaseName,
      "varTableName" -> tableName,
      "varColumnSet" -> getColumnSet(getListOfFieldSpec(columnSet)),
      "varStorageFormat" -> storageFormat,
      "varLocation" -> tableLocation
    )

    generateTableDDL("SimpleTable", tableParams)
  }

  /**
    * This function generates a Hive Table Schema for hbase table with single column family
    *
    * @param databaseName       Hive DataBase Name
    * @param tableLocation      Hive Table Location
    * @param hbaseNameSpace     HBASE Name Space
    * @param hbaseTableName     HBASE Table Name
    * @param columnSet          Columns in HBASE table (including key)
    * @param hbaseHiveTableName Columns in HBASE table (including key)
    * @param rowkeyColumn       Row Key of HBASE Table
    * @param columnFamily       Column Family Name of HBASE Table
    * @return Hive Table Schema that can be executed in sqlContext
    */
  def generateTableDDL(databaseName: String, tableLocation: String, hbaseNameSpace: String, hbaseTableName: String,
                       columnSet: Array[String], hbaseHiveTableName: String, rowkeyColumn: String, columnFamily: String): String = {

    val tableParams = Map(
      "varDatabaseName" -> databaseName,
      "varLocation" -> tableLocation,
      "varTableName" -> s"$hbaseHiveTableName",
      "varColumnSet" -> getColumnSet(getListOfFieldSpec(columnSet)),
      "varHBaseTableName" -> s"$hbaseNameSpace:$hbaseTableName",
      "varHBaseTableMapping" -> columnSet.map(x => s"$columnFamily:" + x).map(x => if (x != s"$columnFamily:$rowkeyColumn") x else ":key").mkString(",")
    )

    generateTableDDL("SimpleHBaseTable", tableParams)
  }

  /**
    * This function generates a Hive Table Schema for hbase table with multiple column families
    *
    * @param databaseName   Hive DataBase Name
    * @param hbaseNameSpace HBASE Name Space
    * @param hbaseTableName HBASE Table Name
    * @param columnSet      Columns in all column families of HBASE table (including key)
    * @param rowkeyColumn   Row Key of HBASE Table
    * @param cfColsMap      Map of [Column family -> Array[Columns ] ]
    * @return Hive Table Schema that can be executed in sqlContext
    */
  def generateTableDDL(databaseName: String, hbaseHiveTableName: String, hbaseNameSpace: String, hbaseTableName: String, columnSet: Array[String], rowkeyColumn: String, cfColsMap: Map[String, Array[String]]): String = {

    val hBaseTableMapping = cfColsMap.map {
      case ("rowKey", _) =>
        ":key"
      case (k, v) =>
        v.map(eachCol => s"$k:$eachCol").mkString(",")
    }.mkString(",")
    val tableParams = Map(
      "varDatabaseName" -> databaseName,
      "varTableName" -> s"$hbaseHiveTableName",
      "varColumnSet" -> getColumnSet(getListOfFieldSpec(columnSet)),
      "varHBaseTableName" -> s"$hbaseNameSpace:$hbaseTableName",
      "varHBaseNamespace" -> hbaseNameSpace,
      "varHBaseTableMapping" -> hBaseTableMapping
      // "varHbaseSiteXML" -> hbaseSiteXML
    )

    generateTableDDL("SimpleHBaseTable", tableParams)
  }


  /**
    * This function generates a Hive Table Schema for hbase table with multiple column families
    *
    * @param databaseName   Hive DataBase Name
    * @param hbaseNameSpace HBASE Name Space
    * @param hbaseTableName HBASE Table Name
    * @param columnSet      Columns in all column families of HBASE table (including key)
    * @param rowkeyColumn   Row Key of HBASE Table
    * @param cfColsMap      Map of [Column family -> Array[Columns ] ]
    * @return Hive Table Schema that can be executed in sqlContext
    */
  def generateTableDDL(columnSet: Array[String], databaseName: String, hbaseHiveTableName: String, hbaseNameSpace: String, hbaseTableName: String, rowkeyColumn: String, cfColsMap: Map[String, Array[String]]): String = {

    val tableParams = Map(
      "varDatabaseName" -> databaseName,
      "varTableName" -> s"$hbaseHiveTableName",
      "varColumnSet" -> getColumnSet(getListOfFieldSpec(columnSet)),
      "varHBaseTableName" -> s"$hbaseNameSpace:$hbaseTableName",
      "varHBaseNamespace" -> s"$hbaseNameSpace",
      "varHBaseTableMapping" -> cfColsMap.map(x => if (x._1 == "rowKey") ":key" else x._2.map(eachCol => x._1 + ":" + eachCol).mkString(",")).mkString(",")
      // "varHbaseSiteXML" -> hbaseSiteXML
    )

    generateTableDDL("SimpleHBaseTableWithoutHandler", tableParams)
  }

  /**
    * This function generates a Hive Table Schema
    *
    * @param tableType     indicates where table an External Table or Managed Table
    * @param databaseName  the Hive database name
    * @param tableName     the Hive table Name
    * @param location      Hive Table's HDFS path
    * @param sparkSQLSerde The spark sql serde ..
    *                      Example parquet or json, that will be converted to org.apache.spark.sql.parquet or org.apache.spark.sql.json
    * @return Hive Table Schema that can be executed in sqlContext
    *
    */

  def generateTableDDL(tableType: String, databaseName: String, tableName: String,
                       location: String, sparkSQLSerde: String): String = {

    val tableParams: Map[String, String] = Map(
      "varTableType" -> tableType,
      "varDatabaseName" -> databaseName,
      "varTableName" -> tableName,
      "varLocation" -> location,
      "varSparkSQLSerde" -> sparkSQLSerde
    )

    generateTableDDL("SerdeTable", tableParams)

  }

  /**
    * This function generates a Hive Table Schema
    *
    * @param tableType     indicates where table an External Table or Managed Table
    * @param databaseName  the Hive database name
    * @param tableName     the Hive table Name
    * @param location      Hive Table's HDFS path
    * @param storageFormat underlying storage format such as parquet
    * @param columnSet     An array of column names that can be used to generate the Column Set.
    *                      Each column will be considered as type `String`.
    * @param partitionKey  The columnSet that should be used for partitioning.
    *                      Each column will be considered as type `String`
    * @param clusterKey    A comma-separated list of columns that should be used for clustering.
    *                      Example "id, date"
    * @param clusterNum    Number of Buckets that should be created
    * @return Hive Table Schema that can be executed in sqlContext
    *
    */

  def generateTableDDL(tableType: String, databaseName: String, tableName: String, location: String,
                       storageFormat: String, columnSet: Array[String], partitionKey: Array[String],
                       clusterKey: String, clusterNum: String): String = {

    val tableParams = Map(
      "varTableType" -> tableType,
      "varDatabaseName" -> databaseName,
      "varTableName" -> tableName,
      "varColumnSet" -> getColumnSet(getListOfFieldSpec(columnSet)),
      "varStorageFormat" -> storageFormat,
      "varLocation" -> location,
      "varClusterKey" -> clusterKey,
      "varClusterCount" -> clusterNum,
      "varPartitionColumnSet" -> getColumnSet(getListOfFieldSpec(partitionKey))
    )

    generateTableDDL("PartitionedBucketedTable", tableParams)

  }

  /**
    * This function generates a Hive Table Schema
    *
    * @param tableType     indicates where table an External Table or Managed Table
    * @param databaseName  the Hive database name
    * @param tableName     the Hive table Name
    * @param tableLocation Hive Table's HDFS path
    * @param storageFormat underlying storage format such as parquet
    * @param columnSet     An Array of ColumnNames that can be used to generate the Column Set, Each column will be considered as type "String"
    * @param partitionKey  The columnset that should be used for partitioning, Each column will be considered as type "string"
    * @return Hive Table Schema that can be executed in sqlContext
    *
    */

  def generateTableDDL(tableType: String, databaseName: String, tableName: String, tableLocation: String, storageFormat: String,
                       columnSet: Array[String], partitionKey: Array[String]): String = {
    val tableParams: Map[String, String] = Map(
      "varTableType" -> tableType,
      "varDatabaseName" -> databaseName,
      "varTableName" -> tableName,
      "varColumnSet" -> getColumnSet(getListOfFieldSpec(columnSet)),
      "varStorageFormat" -> storageFormat,
      "varLocation" -> tableLocation,
      "varPartitionColumnSet" -> getColumnSet(getListOfFieldSpec(partitionKey))
    )

    generateTableDDL("PartitionedTable", tableParams)

  }

  /**
    * This function generates a Hive Table Schema
    *
    * @param tableType     indicates where table an External Table or Managed Table
    * @param databaseName  the Hive database name
    * @param tableName     the Hive table Name
    * @param tableLocation Hive Table's HDFS path
    * @param storageFormat underlying storage format such as parquet
    * @param columnSet     The Columns (StructType)
    * @param partitionKey  The columnset (StructType)
    * @return Hive Table Schema that can be executed in sqlContext
    *
    */

  def generateTableDDL(tableType: String, databaseName: String, tableName: String, tableLocation: String, storageFormat: String,
                       columnSet: StructType, partitionKey: StructType): String = {
    val tableParams: Map[String, String] = Map(
      "varTableType" -> tableType,
      "varDatabaseName" -> databaseName,
      "varTableName" -> tableName,
      "varColumnSet" -> getColumnSet(columnSet),
      "varStorageFormat" -> storageFormat,
      "varLocation" -> tableLocation,
      "varPartitionColumnSet" -> getColumnSet(partitionKey)
    )

    generateTableDDL("PartitionedTable", tableParams)

  }

  /**
    * This function generates a Hive Table Schema
    *
    * @param tableType            indicates where table an External Table or Managed Table
    * @param inputTableParameters A Map of every parameter that is necessary to build a Table DDL from Templates
    * @return Hive Table Schema that can be executed in sqlContext
    *
    */

  def generateTableDDL(tableType: String, inputTableParameters: Map[String, String]): String = {
    val tableParams = buildTableParams(inputTableParameters)
    val templateDDLType = "template" + tableType
    var tableDDL = HiveSchemaTemplates.tableTemplates.getOrElse(templateDDLType, "")
    tableParams.foreach { tableParameter =>
      tableDDL = tableDDL.replaceAllLiterally("${" + tableParameter._1 + "}", tableParameter._2)
    }
    tableDDL
  }

  /**
    * This function builds all the parameters that are required to resolve the variables in a table
    * DDL Template.
    *
    * @param inputParams
    * incoming list of parameters that will used to generate the holistic list
    * @return tableParams
    *         All the parameters that are necessary to generate a table's DDL from templates.
    *
    */

  def buildTableParams(inputParams: Map[String, String]): Map[String, String] = {
    val serdeFromat = inputParams.getOrElse("varSparkSQLSerde", "parquet")
    Map(
      "varTableType" -> inputParams.getOrElse("varTableType", "EXTERNAL"),
      "varDatabaseName" -> inputParams.getOrElse("varDatabaseName", "default"),
      "varTableName" -> inputParams.getOrElse("varTableName", "default"),
      "varColumnSet" -> inputParams.getOrElse("varColumnSet", "default"),
      "varStorageFormat" -> inputParams.getOrElse("varStorageFormat", "default"),
      "varLocation" -> inputParams.getOrElse("varLocation", "default"),
      "varSparkSQLSerde" -> s"org.apache.spark.sql.$serdeFromat",
      "varHBaseTableName" -> inputParams.getOrElse("varHBaseTableName", ""),
      "varHBaseNamespace" -> inputParams.getOrElse("varHBaseNamespace", ""),
      "varHBaseTableMapping" -> inputParams.getOrElse("varHBaseTableMapping", ""),
      // "varHbaseSiteXML" -> inputParams.getOrElse("varHbaseSiteXML", ""),
      "varClusterKey" -> inputParams.getOrElse("varClusterKey", "default"),
      "varClusterCount" -> inputParams.getOrElse("varClusterCount", "default"),
      "varPartitionColumnSet" -> inputParams.getOrElse("varPartitionColumnSet", "default"),
      "varFieldDelimiter" -> inputParams.getOrElse("varFieldDelimiter", "\\010"),
      "varRowDelimiter" -> inputParams.getOrElse("varRowDelimiter", "\\020")
    )
  }

  /**
    * This function builds columnset list of a table using an input of Array of ColumnNames
    *
    * @param columnSet An Array of ColumnNames . Example : Array("id", "name", "age")
    * @return FieldSet that is an Array of Maps, each Map representing 1 column & the map contains columnName, columnType, columnNullability
    *
    */

  def getListOfFieldSpec(columnSet: Array[String]): Array[Map[String, String]] = {
    columnSet.map { column =>
      Map(
        "fieldName" -> column,
        "fieldType" -> "string"
        // "fieldNullable" -> "null"
      )
    }
  }

  /**
    * This function builds an array of Maps, each map representing a column with columnName, columnType, columnNullability
    *
    * @param fieldSet An array of Maps, each map representing a column with columnName, columnType, columnNullability
    * @return A string that can be used to replace the columnset in a table's DDL Template.
    *         Example : "id string null, name string null"
    *
    */

  def getColumnSet(fieldSet: Array[Map[String, String]]): String = {
    fieldSet.map { field =>
      s"${field.getOrElse("fieldName", "unknown")} string"
    }.mkString("", ",\n", "\n")
  }


  /**
    * This function builds an array of Maps, each map representing a column with columnName, columnType, columnNullability
    *
    * @param fieldSet sparkSQL StructType
    * @return A string that can be used to replace the columnset in a table's DDL Template.
    *         Example : "id string null ,name string null"
    *
    */

  def getColumnSet(fieldSet: StructType): String = {
    fieldSet.map { field =>
      s"${field.name} ${field.dataType.simpleString}"
    }.mkString("", ",\n", "\n")
  }
}
