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

package com.paypal.gimel.common.storageadmin

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.security.UserGroupInformation

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.logger.Logger


object HBaseAdminClient {

  val logger = Logger()

  /**
    * Creates HBASE Connection
    *
    * @param hbaseSiteXml HBASE Site XML provided
    */
  def createConnection(hbaseSiteXml: String): Connection = {
    logger.info("Hbase site xml :" + hbaseSiteXml)
    // HBASE Conf
    val conf: Configuration = HBaseConfiguration.create()
    conf.addResource(new Path(hbaseSiteXml))
    // HBASE Connection
    ConnectionFactory.createConnection(conf)
  }

  /**
    * Creates HBASE Table with multiple column families if it does not exists
    *
    * @param hbaseNameSpace        HBASE NameSpace
    * @param hbaseTable            HBASE Table
    * @param hbaseColumnFamilyName HBASE Column Family
    * @param hbaseSiteXmlHdfs      HBASE Site XML provided in HDFS
    */
  def createHbaseTableIfNotExists(hbaseNameSpace: String, hbaseTable: String, hbaseColumnFamilyName: Array[String], hbaseSiteXmlHdfs: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val connection = createConnection(hbaseSiteXmlHdfs)
    try {
      createHbaseTableIfNotExists(connection, hbaseNameSpace, hbaseTable, hbaseColumnFamilyName)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to create HBase table.")
        throw ex
    } finally {
      connection.close()
    }
  }

  /**
    * Creates HBASE Table with multiple column families if it does not exists with provided hbase connection
    *
    * @param connection            HBase Connection
    * @param hbaseNameSpace        HBASE NameSpace
    * @param hbaseTable            HBASE Table
    * @param hbaseColumnFamilyName HBASE Column Family
    */
  def createHbaseTableIfNotExists(connection: Connection, hbaseNameSpace: String, hbaseTable: String, hbaseColumnFamilyName: Array[String]): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    // HBASE Admin
    val admin: Admin = connection.getAdmin
    try {
      // Build Table Name
      val tableName: TableName = TableName.valueOf(hbaseNameSpace, s"$hbaseTable")
      // Build Table Name Description
      val tableDesc: HTableDescriptor = new HTableDescriptor(tableName)
      hbaseColumnFamilyName.foreach { columnFamily =>
        // Build Cf & Cols
        val colDef: HColumnDescriptor = new HColumnDescriptor(Bytes.toBytes(columnFamily))
        tableDesc.addFamily(colDef)
      }
      // DeployTable
      if (!admin.isTableAvailable(tableName)) {
        logger.warning(s"Schema:Table Does Not Exists --> $hbaseNameSpace:$hbaseTable. Creating it ...")
        admin.createTable(tableDesc)
        logger.info(s"Created --> $hbaseNameSpace:$hbaseTable.")
      } else {
        logger.warning(s"Schema:Table Already Exists --> $hbaseNameSpace:$hbaseTable")
      }
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to create HBase table.")
        throw ex
    } finally {
      admin.close()
    }
  }

  /**
    * Creates HBASE Table with one column family if it does not exists
    *
    * @param hbaseNameSpace        HBASE NameSpace
    * @param hbaseTable            HBASE Table
    * @param hbaseColumnFamilyName HBASE Column Family
    * @param hbaseSiteXmlHdfs      HBASE Site XML provided in HDFS
    */
  def createHbaseTableIfNotExists(hbaseNameSpace: String, hbaseTable: String, hbaseColumnFamilyName: String, hbaseSiteXmlHdfs: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    try {
      createHbaseTableIfNotExists(hbaseNameSpace, hbaseTable, Array(hbaseColumnFamilyName), hbaseSiteXmlHdfs)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to create HBase table.")
        throw ex
    }
  }

  /**
    * Creates HBASE Table with one column family if it does not exists
    *
    * @param connection            HBase Connection
    * @param hbaseNameSpace        HBASE NameSpace
    * @param hbaseTable            HBASE Table
    * @param hbaseColumnFamilyName HBASE Column Family
    */
  def createHbaseTableIfNotExists(connection: Connection, hbaseNameSpace: String, hbaseTable: String, hbaseColumnFamilyName: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    try {
      createHbaseTableIfNotExists(connection, hbaseNameSpace, hbaseTable, Array(hbaseColumnFamilyName))
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to create HBase table.")
        throw ex
    }
  }

  /**
    * Deletes HBASE Table if it one exists
    *
    * @param connection     HBase Connection
    * @param hbaseNameSpace HBASE NameSpace
    * @param hbaseTable     HBASE Table
    */
  def deleteHbaseTable(connection: Connection, hbaseNameSpace: String, hbaseTable: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    logger.info("Dropping HBASE table --> ")
    // HBASE Admin
    val admin: Admin = connection.getAdmin
    try {
      // Build Table Name
      val tableName = TableName.valueOf(hbaseNameSpace, s"$hbaseTable")
      // DeleteTable
      if (admin.isTableAvailable(tableName)) {
        logger.info(s"Schema:Table  Found : $hbaseNameSpace:$hbaseTable. Deleting it ...")
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
        logger.info(s"Deleted --> $hbaseNameSpace:$hbaseTable.")
      } else {
        logger.warning(s"Schema:Table Not Found --> $hbaseNameSpace:$hbaseTable")
      }
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to delete HBase table.")
        throw ex
    } finally {
      admin.close()
    }
  }

  /**
    * Deletes HBASE Table if it one exists
    *
    * @param hbaseNameSpace   HBASE NameSpace
    * @param hbaseTable       HBASE Table
    * @param hbaseSiteXmlHdfs HBASE Site XMl provided in HDFS
    */
  def deleteHbaseTable(hbaseNameSpace: String, hbaseTable: String, hbaseSiteXmlHdfs: String): Unit = {

    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    logger.info("Dropping HBASE table --> ")

    val connection = createConnection(hbaseSiteXmlHdfs)
    try {
      deleteHbaseTable(connection, hbaseNameSpace, hbaseTable)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to delete HBase table.")
        throw ex
    } finally {
      connection.close()
    }
  }

  /**
    * Creates HBASE Name Space if one does not exists, this works only with Create privileges on NameSpaces.
    *
    * @param hbaseNameSpace HBASE NameSpace
    * @param clusterName    HBASE Cluster Name : optional
    */
  def createHbaseNameSpaceIfNotExists(hbaseNameSpace: String, clusterName: String, hbaseSiteXmlHdfs: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val connection = createConnection(hbaseSiteXmlHdfs)
    try {
      createHbaseNameSpaceIfNotExists(connection, hbaseNameSpace, clusterName)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to create HBase namespace.")
        throw ex
    } finally {
      connection.close()
    }
  }

  /**
    * Creates HBASE Name Space if one does not exists, this works only with Create privileges on NameSpaces.
    *
    * @param connection     HBase Connection
    * @param hbaseNameSpace HBASE NameSpace
    * @param clusterName    HBASE Cluster Name : optional
    */
  def createHbaseNameSpaceIfNotExists(connection: Connection, hbaseNameSpace: String, clusterName: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    // HBASE Admin
    val admin: Admin = connection.getAdmin
    try {
      // Name Space Creation
      val namespace: NamespaceDescriptor = NamespaceDescriptor.create(hbaseNameSpace).build()
      val nameSpaces = admin.listNamespaceDescriptors().map(_.getName)

      // DeployNameSpace
      if (!nameSpaces.contains(hbaseNameSpace)) {
        logger.info(s"NameSpace Does Not Exists --> $hbaseNameSpace, creating...")
        admin.createNamespace(namespace)
        logger.info(s"NameSpace Created --> $hbaseNameSpace.")
      } else {
        logger.warning(s"NameSpace Already Exists --> $hbaseNameSpace")
      }
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to create HBase namespace.")
        throw ex
    } finally {
      admin.close()
    }
  }


  /**
    * Get a list of namespaces
    *
    * @param connection HBase Connection
    */
  def getAllNamespaces(connection: Connection): Array[String] = {

    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    logger.info("Getting all namespaces --> ")

    // HBASE Admin
    val admin: Admin = connection.getAdmin
    try {
      admin.listNamespaceDescriptors().map(x => x.getName)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to list namespaces")
        throw ex
    } finally {
      admin.close()
    }
  }

  /**
    * Get a list of namespaces
    *
    * @param hbaseSiteXmlHdfs HBASE Site XMl provided in HDFS
    */
  def getAllNamespaces(hbaseSiteXmlHdfs: String): Array[String] = {

    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    logger.info("Hbase Site XML --> " + hbaseSiteXmlHdfs)
    logger.info("Getting all namespaces --> ")
    val connection = createConnection(hbaseSiteXmlHdfs)
    try {
      val namespaces = getAllNamespaces(connection)
      namespaces
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to list namespaces")
        throw ex
    } finally {
      connection.close()
    }
  }

  /**
    * Get a list of tables from a namespace
    *
    * @param namespace        Namespace name
    * @param hbaseSiteXmlHdfs HBASE Site XMl provided in HDFS
    */
  def getTablesFromNamespace(namespace: String, hbaseSiteXmlHdfs: String): Array[String] = {

    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    logger.info("Getting all tables from namespaces " + namespace + "--> ")
    val connection = createConnection(hbaseSiteXmlHdfs)
    // HBASE Admin
    val admin: Admin = connection.getAdmin
    try {
      val tables = getTablesFromNamespace(connection, namespace)
      tables
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to list tables from the namespace")
        throw ex
    } finally {
      admin.close()
      connection.close()
    }
  }

  /**
    * Get a list of tables from a namespace
    *
    * @param connection HBase Connection
    * @param namespace  Namespace name
    */
  def getTablesFromNamespace(connection: Connection, namespace: String): Array[String] = {

    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    logger.info("Getting all tables from namespaces " + namespace + "--> ")
    // HBASE Admin
    val admin: Admin = connection.getAdmin
    try {
      val namespacesTables = admin.listTableNamesByNamespace(namespace).map(tableDesc => tableDesc.getNameAsString)
      logger.info(namespacesTables.toString)
      val tables = if (namespace != "default") namespacesTables.map(nsTable => nsTable.split(":")(1)) else namespacesTables
      tables
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to list tables from the namespace")
        throw ex
    } finally {
      admin.close()
    }
  }

  /**
    * Copies Distributed XML file to local Path
    *
    * @param hbaseSiteXMLHDFS @param hbaseConfigFileLocation The HBASE Configuration File : hbase-site.xml
    * @return hbaseSiteXML LocalPath
    */

  def getHbaseSiteXml(hbaseSiteXMLHDFS: String): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val hbaseConfigFileLocation = hbaseSiteXMLHDFS match {
      case "NONE" =>
        logger.warning("THERE IS NO HBASE SITE XML SPECIFIED, WILL TRY TO USE WHATEVER IS VISIBLE TO THE SESSION !")
        "/etc/hbase/conf/hive-site.xml"
      case _ =>
        val hdfsPath = new Path(hbaseSiteXMLHDFS)
        val siteXMLName = "hbase-site.xml"
        val user = sys.env("USER")
        val targetFileString = s"/tmp/$user/gimel/conf/$siteXMLName"
        val targetFile = new Path(targetFileString)
        val conf = new org.apache.hadoop.conf.Configuration()
        val hadoop_conf_dir = System.getenv("HADOOP_CONF_DIR")
        conf.addResource(new Path(hadoop_conf_dir + "/" + "core-site.xml"))
        conf.addResource(new Path(hadoop_conf_dir + "/" + "hdfs-site.xml"))
        conf.set(GimelConstants.HDFS_IMPL, GimelConstants.DISTRIBUTED_FS)
        conf.set(GimelConstants.FILE_IMPL, GimelConstants.LOCAL_FS)
        conf.set(GimelConstants.SECURITY_AUTH, "kerberos")
        UserGroupInformation.setConfiguration(conf)
        val fileSystem = FileSystem.get(conf)
        logger.info(s"Picked Site HBASE XML from HDFS PATH --> $hbaseSiteXMLHDFS | Copied to Local Path --> $targetFileString")
        fileSystem.copyToLocalFile(hdfsPath, targetFile)
        targetFileString
    }
    hbaseConfigFileLocation
  }
}
