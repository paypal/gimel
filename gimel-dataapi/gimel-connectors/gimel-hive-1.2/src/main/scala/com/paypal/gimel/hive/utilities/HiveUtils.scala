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

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.immutable.Map

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructField

import com.paypal.gimel.common.catalog.{DataSetProperties, Field}
import com.paypal.gimel.common.conf.{CatalogProviderConfigs, CatalogProviderConstants, GimelConstants}
import com.paypal.gimel.hdfs.conf.HdfsConfigs
import com.paypal.gimel.hive.conf.{HiveConfigs, HiveConstants}
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.parser.utilities.QueryConstants

class HiveUtils {

  val logger = Logger()
  logger.info(s"Initiated --> ${this.getClass.getName}")

  /**
    * write to hive table
    *
    * @param dataSetProps is options coming from the user
    * @param dataSet      - the hive table name
    * @param dataFrame    - dataframe that need to be inserted into the hive table
    * @param sparkSession : SparkSession
    * @return dataFrame
    */

  def write(dataSet: String, dataFrame: DataFrame, sparkSession: SparkSession, dataSetProps: Map[String, Any]): DataFrame = {

    val datasetProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val saveMode = dataSetProps.getOrElse("saveMode", "").toString.toLowerCase
    val apiType = dataSetProps.getOrElse("apiType", "").toString.toLowerCase
    val location = datasetProps.props(CatalogProviderConstants.PROPS_LOCATION)
    val partitionColumns = datasetProps.partitionFields.map(_.fieldName)
    val partKeys = partitionColumns.mkString(",")
    val fieldNames = datasetProps.fields.map(_.fieldName).mkString(",")

    try {

      logger.info("Registering temp table")
      dataFrame.registerTempTable("tempTable")
      logger.info("Register temp table completed")

      val insertPrefix = saveMode.toUpperCase match {
        case "APPEND" =>
          "insert into"
        case "OVERWRITE" =>
          "insert overwrite table"
        case _ =>
          "insert into"
      }

      logger.info(s"insertPrefix:$insertPrefix")
      logger.info(s"saveMode:$saveMode, apiType:$apiType,location:$location, partKeys:$partKeys, partitionColumns:$partitionColumns, fieldNames:$fieldNames")

      val fnlInsert = {
        if (partKeys.isEmpty) {
          s"""
             |$insertPrefix $dataSet
             |select
             |$fieldNames
             | from tempTable
    """.stripMargin
        } else {
          s"""
             |$insertPrefix $dataSet
             | partition ($partKeys)
             |select
             |$fieldNames,$partKeys
             | from tempTable
            """.stripMargin
        }
      }

      logger.info(s"final insert: $fnlInsert")
      logger.info("Executing final insert statement")

      sparkSession.conf.set(HdfsConfigs.dynamicPartitionKey, "true")
      sparkSession.conf.set(HdfsConfigs.dynamicPartitionModeKey, "nonstrict")
      sparkSession.sql(fnlInsert)

      dataFrame
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

  /**
    *
    * @param dataset : Name of the UDC Data Set
    * @return true if its a UDC or Pcatalog table
    *
    */
  def checkIfCatalogTable(dataset: String): Boolean = {
    if (dataset.startsWith(GimelConstants.PCATALOG_STRING) || dataset.startsWith(GimelConstants.UDC_STRING) || (dataset.split("\\.").length > 2)) {
      return true
    }
    false
  }

  /**
    * Create hive table by preparing the CREATE sql statement
    *
    * @param dataset      - dataset that need to be created as object in HIVE
    * @param dataSetProps - set of dataset properties
    * @param sparkSession - sparkSession
    * @return - whether the table creation is success or failure
    */
  def create(dataset: String, dataSetProps: Map[String, Any], sparkSession: SparkSession): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    val catalogProvider = dataSetProps.get(CatalogProviderConfigs.CATALOG_PROVIDER).get.toString
    val sql = dataSetProps.get(GimelConstants.TABLE_SQL).get.toString
    val sparkSchema: Array[StructField] = dataSetProps.keySet.contains(GimelConstants.TABLE_FILEDS) match {
      case true => dataSetProps(GimelConstants.TABLE_FILEDS).asInstanceOf[Array[StructField]]
      case false => Array[StructField]()
    }
    val partitionFields: Array[Field] = dataSetProps.get(GimelConstants.HIVE_DDL_PARTITIONS_STR) match {
      case None => Array[Field]()
      case _ => dataSetProps.get("PARTITIONS").get.asInstanceOf[Array[Field]]
    }
    val createTableStatement: Any = catalogProvider.toUpperCase() match {
      case CatalogProviderConstants.PCATALOG_PROVIDER | CatalogProviderConstants.UDC_PROVIDER => {
        dataSetProps.get(GimelConstants.CREATE_STATEMENT_IS_PROVIDED).get match {
          case "true" =>
            dataSetProps.get(GimelConstants.TABLE_SQL).get.toString
          case _ =>
            prepareCreateStatement(sql, sparkSchema, partitionFields)
        }
      }
      case CatalogProviderConstants.HIVE_PROVIDER =>
        throw new Exception(s"HIVE Provider is NOT currently Supported")
    }
    logger.info("CREATE TABLE STATEMENT => " + createTableStatement)
    sparkSession.sql(createTableStatement.toString)
    logger.info("TABLE CREATED SUCCESSFULLY ")
    true
  }

  /**
    * Prepare the CREATE table statement
    *
    * @param sql         - Given SQL statement
    * @param sparkSchema - sparkSchema gotten from the dataframe of the select statement
    * @param partitions  - Partition information
    * @return - The createTable statement
    */
  def prepareCreateStatement(sql: String, sparkSchema: Array[StructField], partitions: Array[Field]): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    // This is the pattern to get the content of first paranthesis which is partition columns
    // Sample create statement is - CREATE TABLE <NEW_TABLE> PARTITIONED BY(YEAR, MONTH) AS SELECT * FROM <SRC TABLE>
    // In below section we get the partitioned columns and add the data type as string and create column1 String, Column2 String
    // which will be injected back into the PARTITIONED BY clause as the statement will not have the data types.
    // But for creating the table we need to give PARTITIONED BY with column names and data types.

    val pattern = """\((.*?)\)""".r
    var partitionReplaced = ""
    partitions.foreach(parts => partitionReplaced = partitionReplaced + parts.fieldName + " String" + ",")
    val typedPartitions = s"""(${partitionReplaced.dropRight(1)})"""
    val partitionsFixedSql = pattern.replaceFirstIn(sql, typedPartitions.toLowerCase())

    // Here we remove the SELECT portion and have only the CREATE portion of the DDL supplied so that we can use that to create the table
    val sqlParts = partitionsFixedSql.split(" ")
    val lenPartKeys = partitions.length
    val index = sqlParts.indexWhere(_.toUpperCase() == "SELECT")
    val createOnly: String = sqlParts.slice(0, index - 1).mkString(" ")

    // Here we remove the UDC prefix => we replace udc.storagetype.storagesystem.DB.Table with DB.Table
    val createParts = createOnly.split(" ")
    val pcatSQL = createParts.map(element => {
      if (element.toLowerCase().contains(GimelConstants.UDC_STRING)) {
        element.split('.').tail.mkString(".").split('.').tail.mkString(".").split('.').tail.mkString(".")
      }
      else {
        element
      }
    }
    ).mkString(" ")

    // Here we formulate the column and their column data types from dataframes schema
    val newSchema = sparkSchema.take(sparkSchema.length - lenPartKeys)
    val colList: Array[String] = newSchema.map(x => (x.name + " " + (x.dataType.simpleString) + ","))
    val conCatColumns = colList.mkString("").dropRight(1)
    val colQulifier = s"""(${conCatColumns})"""

    // Here we inject back the columns with data types back in the SQL statemnt
    val newSqlParts = pcatSQL.split(" ")
    val pcatIndex = newSqlParts.indexWhere(_.toUpperCase().contains("TABLE"))
    val catPrefix = newSqlParts.slice(0, pcatIndex + 2).mkString(" ")
    val catSuffix = newSqlParts.slice(pcatIndex + 2, newSqlParts.length).mkString(" ")
    val fullStatement = s"""${catPrefix} ${colQulifier} ${catSuffix}"""
    logger.info("the Create statement" + fullStatement)
    fullStatement

  }

  /**
    * Truncate utility to truncate the table
    *
    * Managed hive Table:
    * a) Rename the table in hive to original name + Unique ID (DELETE_ + current time YYYYMMDDHHSS + User)
    * b) recreate the table with the original name
    *
    * External Table:
    * a) Rename the table in hive to original name + Unique ID (DELETE_ + current time YYYYMMDDHHSS + User)
    * b) Rename the location to original name + Unique ID (current time YYYYMMDDHHSS + User)
    * c) recreate the table with the original name
    *
    * @param dataset      - Input data set
    * @param dataSetProps - dataset properties holding attributes
    * @param sparkSession - spark session
    * @return - returns boolean based on whether the table got truncated or not
    */
  def truncate(dataset: String, dataSetProps: Map[String, Any], sparkSession: SparkSession): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    val actualProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val catalogProvider = dataSetProps.get(CatalogProviderConfigs.CATALOG_PROVIDER).get.toString
    val hiveDataSetName = actualProps.props(GimelConstants.HIVE_DATABASE_NAME) + "." + actualProps.props(GimelConstants.HIVE_TABLE_NAME)
    val uniqueID = getUniqueID(sparkSession.sparkContext.sparkUser, QueryConstants.DDL_DELETE_STRING, hiveDataSetName)
    val tableLocation = com.paypal.gimel.common.catalog.CatalogProvider.getHiveTable(hiveDataSetName).getSd.getLocation
    val dfShowCreate = sparkSession.sql(s"""show create table ${hiveDataSetName}""")
    val originalCreateStatement = dfShowCreate.select("createtab_stmt").rdd.map(r => r(0)).collect()(0).toString

    val isManagedTable = com.paypal.gimel.common.catalog.CatalogProvider.getHiveTable(hiveDataSetName).getTableType match {
      case "MANAGED_TABLE" => true
      case _ => false
    }
    catalogProvider match {
      case CatalogProviderConstants.PCATALOG_PROVIDER | CatalogProviderConstants.UDC_PROVIDER => {
        // Rename the table
        val renameTableCommand = s"""ALTER TABLE ${hiveDataSetName} RENAME TO ${hiveDataSetName}_${uniqueID}"""
        logger.info("ALTER TABLE COMMAND =>  " + renameTableCommand)
        sparkSession.sql(renameTableCommand)

        // If it is an EXTERNAL table move the data files to the new location
        if (!isManagedTable) {
          val oldPath = new Path(tableLocation)
          val newPath = new Path(s"""${tableLocation}_${uniqueID}""")
          val conf = new org.apache.hadoop.conf.Configuration()
          val fs = FileSystem.get(conf)
          fs.rename(oldPath, newPath)

          // We need to set the new location that we moved the files in the Meta strore
          val renamePathCommand = s"""ALTER TABLE ${hiveDataSetName}_${uniqueID} SET LOCATION '${tableLocation}_${uniqueID}' """
          logger.info("ALTER TABLE COMMAND - LOCATION CHANGE  " + renamePathCommand)
          sparkSession.sql(renamePathCommand)
        }
        // If MANAGED table recreate the table with the original name
        if (isManagedTable) {
          val createStatement = s"""CREATE TABLE ${hiveDataSetName} LIKE ${hiveDataSetName}_${uniqueID}"""
          sparkSession.sql(createStatement)
        }
        else {
          // If EXTERNAL table recreate the table with the original name
          // Also set the location to the original location
          sparkSession.sql(originalCreateStatement)
          val renamePathCommand = s"""ALTER TABLE ${hiveDataSetName} SET LOCATION '${tableLocation}' """
          sparkSession.sql(renamePathCommand)
        }
      }
      case CatalogProviderConstants.HIVE_PROVIDER =>
        throw new Exception(s"HIVE Provider is NOT currently Supported")
    }
    logger.info(s""""${hiveDataSetName} got truncated and a back up table ${hiveDataSetName}_${uniqueID} got created""")
    true
  }

  /**
    * Drops the table with a back up
    * Managed hive Table:
    * a) Rename the table in hive to original name + Unique ID (DROP + current time YYYYMMDDHHSS + User)
    *
    * External Table:
    * a) Rename the table in hive to original name + Unique ID (DROP_ + current time YYYYMMDDHHSS + User)
    * b) Rename the location to original name + Unique ID (DROP_ + current time YYYYMMDDHHSS + User)
    * c) recreate the table with the original name
    *
    * @param dataset      - Input data set
    * @param dataSetProps - dataset properties holding attributes
    * @param sparkSession
    * @return - returns boolean based on whether the table got dropped or not
    */
  def drop(dataset: String, dataSetProps: Map[String, Any], sparkSession: SparkSession): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    val actualProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val catalogProvider = dataSetProps.get(CatalogProviderConfigs.CATALOG_PROVIDER).get.toString
    val hiveDataSetName = actualProps.props(GimelConstants.HIVE_DATABASE_NAME) + "." + actualProps.props(GimelConstants.HIVE_TABLE_NAME)
    val uniqueID = getUniqueID(sparkSession.sparkContext.sparkUser, QueryConstants.DDL_DROP_STRING, hiveDataSetName)
    val tableLocation = com.paypal.gimel.common.catalog.CatalogProvider.getHiveTable(hiveDataSetName).getSd.getLocation
    val isManagedTable = com.paypal.gimel.common.catalog.CatalogProvider.getHiveTable(hiveDataSetName).getTableType match {
      case "MANAGED_TABLE" => true
      case _ => false
    }

    val dropTableStatement = catalogProvider match {
      case CatalogProviderConstants.PCATALOG_PROVIDER | CatalogProviderConstants.UDC_PROVIDER =>
        s"""ALTER TABLE ${hiveDataSetName} RENAME TO ${hiveDataSetName}_${uniqueID}"""
      case CatalogProviderConstants.HIVE_PROVIDER =>
        throw new Exception(s"HIVE Provider is NOT currently Supported")
    }
    sparkSession.sql(dropTableStatement.toString)

    if (!isManagedTable) {
      // If it is an EXTERNAL table move the data files to the new location
      val oldPath = new Path(tableLocation)
      val newPath = new Path(s"""${tableLocation}_${uniqueID}""")
      val conf = new org.apache.hadoop.conf.Configuration()
      val fs = FileSystem.get(conf)
      fs.rename(oldPath, newPath)
      // We need to set the new location that we moved the files in the Meta strore
      val renamePathCommand =
        s"""ALTER TABLE ${hiveDataSetName}_${uniqueID} SET LOCATION '${tableLocation}_${uniqueID}' """
      logger.debug("renameTableCommand  " + renamePathCommand)
      sparkSession.sql(renamePathCommand)
    }
    logger.info(s""""${hiveDataSetName} got dropped and a back up table ${hiveDataSetName}_${uniqueID} got created""")
    true
  }

  /**
    * This function creates a new back up table name with the following standards
    * We will create unique ID =  ACTION +  '_' + YYYYMMDDHHSS + spark user
    * new table name = original_name + '_' + unique_id
    * we will strip off characters beyond hive table max size (128)
    *
    * @param action          - action will tell whether it is a DROP or TRUNCATE
    * @param hiveDataSetName - input table name
    * @return - output backup tablename
    */
  def getUniqueID(sparkUser: String, action: String, hiveDataSetName: String): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    val calendar = Calendar.getInstance()
    val dtf = new SimpleDateFormat("YYYYMMddHHmmss")
    val currentTime: String = dtf.format(calendar.getTime())
    val uniqueID = s"""${action}_${currentTime}_${sparkUser}"""
    val hiveTableLen = hiveDataSetName.length
    uniqueID.take(HiveConstants.HIVE_MAX_TABLE_NAME_SIZE - hiveTableLen)
  }

  /**
    * Returns true if cross cluster is detected
    *
    * @param datasetProps - set of attributes for a dataset
    */
  def isCrossCluster(datasetProps: DataSetProperties) : Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    val hadoopConfiguration = new Configuration()
    val clusterUrl = hadoopConfiguration.get(GimelConstants.FS_DEFAULT_NAME)
    val clusterName = new java.net.URI(clusterUrl).getHost
    (clusterName.toUpperCase() != datasetProps.props(HiveConfigs.hdfsStorageNameKey).toUpperCase())
  }

}
