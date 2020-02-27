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

package com.paypal.gimel.sql

import java.sql.SQLException
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.immutable.Map
import scala.util.{Failure, Success, Try}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.Time

import com.paypal.gimel.common.catalog.{CatalogProvider, DataSetProperties}
import com.paypal.gimel.common.conf.{GimelConstants, _}
import com.paypal.gimel.common.utilities.DataSetUtils.resolveDataSetName
import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.elasticsearch.conf.ElasticSearchConfigs
import com.paypal.gimel.hbase.conf.HbaseConfigs
import com.paypal.gimel.hive.conf.HiveConfigs
import com.paypal.gimel.jdbc.conf.{JdbcConfigs, JdbcConstants}
import com.paypal.gimel.jdbc.utilities._
import com.paypal.gimel.kafka.conf.KafkaConfigs
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.logging.GimelStreamingListener
import com.paypal.gimel.parser.utilities.{QueryParserUtils, SearchCriteria, SearchSchemaUtils, SQLNonANSIJoinParser}

object GimelQueryUtils {

  val logger: Logger = Logger(this.getClass.getName)

  // Holder for Run-Time Catalog Provider. Mutable at every SQL execution
  private var catalogProvider: String = CatalogProviderConstants.UDC_PROVIDER
  // Holder for Run-Time Catalog Provider Name. Mutable at every SQL execution
  // Provider Name Space is like a hive DB name when Catalog Provider = HIVE
  private var catalogProviderNameSpace: String = CatalogProviderConstants.UDC_PROVIDER

  /**
    * Gets all Source Tables from a Query String
    *
    * @param sql        SQL String
    * @param searchList List[String] ->  inorder to get from all types of SQL pass searchlist
    *                   to be List("into", "view", "table", "from", "join")
    * @return Seq[Tables]
    */

  def getAllTableSources(sql: String,
                         searchList: Seq[SearchCriteria] = SearchSchemaUtils.ALL_TABLES_SEARCH_CRITERIA): Seq[String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)
    val finalList = QueryParserUtils.getAllSourceTables(sql, searchList)
    logger.info(s"Final List of Tables --> ${finalList.mkString("[", " , ", "]")}")
    finalList
  }

  /**
    * Sets the Catalog Provider
    *
    * @param provider Catalog Provider, say - UDC , PCATALOG , HIVE , USER
    */
  def setCatalogProvider(provider: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    logger.info(s"Supplied catalog provider --> [$provider]")
    provider.toUpperCase() match {
      case CatalogProviderConstants.HIVE_PROVIDER | CatalogProviderConstants.USER_PROVIDER =>
        catalogProvider = provider
      case CatalogProviderConstants.PCATALOG_PROVIDER =>
        logger.warning(" ************************* WARNING ************************* ")
        logger.warning(s"DEPRECATED Catalog Provider --> [${CatalogProviderConstants.PCATALOG_PROVIDER}]")
        logger.warning(s"Please migrate to Catalog Provider --> [${CatalogProviderConstants.UDC_PROVIDER}]")
        logger.warning(" ************************* WARNING ************************* ")
        catalogProvider = provider
        logger.info(s"Auto-Setting catalog provider Namespace to --> [${provider.toUpperCase}]")
        setCatalogProviderName(provider.toUpperCase)
      case CatalogProviderConstants.UDC_PROVIDER =>
        logger.info(s"Auto-Setting catalog provider Namespace to --> [${provider.toUpperCase}]")
        catalogProvider = provider
        setCatalogProviderName(provider.toUpperCase)
      case _ => logger.warning(
        s"""
           |Invalid Catalog Provider --> [${provider}]
           |Valid Options --> [ ${CatalogProviderConstants.HIVE_PROVIDER}| ${CatalogProviderConstants.UDC_PROVIDER}| ${CatalogProviderConstants.PCATALOG_PROVIDER}| ${CatalogProviderConstants.USER_PROVIDER} ]
         """.stripMargin
      )
    }
  }

  /**
    * Client Function to Get Catalog Provider
    *
    * @return The Catalog Provider
    */
  def getCatalogProvider(): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    catalogProvider
  }

  /**
    * Sets the Catalog Provider Name
    *
    * @param providerNameSpace Catalog Provider, say - default, pcatalog, udc, any_other_hive_db_name
    */

  def setCatalogProviderName(providerNameSpace: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val catalogProvider = getCatalogProvider
    if (catalogProvider.equalsIgnoreCase(CatalogProviderConstants.HIVE_PROVIDER) |
      catalogProvider.equalsIgnoreCase(CatalogProviderConstants.USER_PROVIDER)) {
      logger.info(s"setting catalog provider Name to --> [$providerNameSpace]")
      catalogProviderNameSpace = providerNameSpace
    }
    else catalogProviderNameSpace = catalogProvider.toLowerCase()
  }

  /**
    * Client Function to Get Catalog Provider Name
    *
    * @return The Catalog Provider Name Space, say the hive DB name
    */

  def getCatalogProviderName(): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    catalogProviderNameSpace
  }

  /**
    * Returns the individual works from the SQL as tokens
    *
    * @param sql SqlString
    * @return String tokens
    */
  def tokenizeSql(sql: String): Array[String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)
    QueryParserUtils.tokenize(sql)
  }

  /**
    * This search will return true if the hive query has a partitioning criteria in it.
    *
    * @param sql SqlString
    * @return true, if query contains ; insert into partitions of target table.
    */
  def isQueryContainingPartitioning(sql: String): Boolean = {
    val tokens = tokenizeSql(sql.toLowerCase)
    var isHaving = false
    var tmp = ""
    tokens.foreach { token =>
      if ((tmp == "partition" & token == "(") || tmp.contains("partition(")) isHaving = true
      tmp = token
    }
    isHaving
  }

  /**
    * Gets the Tables List from SQL
    *
    * @param sql SQL String
    * @return List of Tables
    */
  def getTablesFrom(sql: String): Array[String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    val otherCatalogProvider = getCatalogProviderName().toLowerCase match {
      case GimelConstants.UDC_STRING => GimelConstants.PCATALOG_STRING
      case GimelConstants.PCATALOG_STRING => GimelConstants.UDC_STRING
      case _ => "hive"

    }
    val allTables = getAllTableSources(sql)
    val finalList = allTables.filter(
      token =>
        token.toLowerCase.contains(s"${getCatalogProviderName().toLowerCase}.") ||
          token.toLowerCase.contains(s"$otherCatalogProvider.")
    )
    logger.info(s"Source Catalog [udc/pcatalog] Tables from entire SQL --> ${finalList.mkString("[", " , ", "]")}")
    finalList.toArray
  }

  /**
    * Gets the Tables List from SQL
    *
    * @param sql SQL String
    * @return List of Tables
    */
  @deprecated
  def getTablesFrom1(sql: String): Array[String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    val sqlLower = sql.toLowerCase
    val searchList = List("insert", "select", "from", "join", "where")
    var lastKey = if (searchList.contains(tokenizeSql(sqlLower).head)) {
      tokenizeSql(sqlLower).head
    } else {
      ""
    }
    var currentKey = ""
    val catalogProviderNameSpace = getCatalogProviderName.toLowerCase
    //    logger.info(s"catalogProviderNameSpace is --> [${catalogProviderNameSpace}]")
    var catalogTables = List[String]()
    val otherCatalogProvider = getCatalogProviderName.toLowerCase match {
      case GimelConstants.UDC_STRING => GimelConstants.PCATALOG_STRING
      case GimelConstants.PCATALOG_STRING => GimelConstants.UDC_STRING
      case _ => "hive"

    }
    // Pick each catalog.table only if its appearing at specific places in the SQL String
    // This guard necessary if someone uses "catalog" as an alias, example - udc or pcatalog
    tokenizeSql(sqlLower).tail.foreach {
      token =>

        currentKey = if (searchList.contains(token)) token else currentKey
        val pickCriteriaMet = token.toLowerCase.contains(s"${getCatalogProviderName.toLowerCase}.") ||
          token.toLowerCase.contains(s"${otherCatalogProvider}.")

        if (pickCriteriaMet) {
          if (lastKey == "from" & !(currentKey == "select")) catalogTables ++= List(token)
          if (lastKey == "join" & !(currentKey == "select")) catalogTables ++= List(token)
        }
        lastKey = if (searchList.contains(token)) currentKey else lastKey
        currentKey = ""
    }

    val nonANSIJoinTables: Seq[String] = SQLNonANSIJoinParser.getSourceTablesFromNonAnsi(sql)
    val nonANSIJoinTablesCatalog = nonANSIJoinTables.filter(
      token =>
        token.toLowerCase.contains(s"${getCatalogProviderName.toLowerCase}.") ||
          token.toLowerCase.contains(s"${otherCatalogProvider}.")
    )
    val finalList = (catalogTables.toArray ++ nonANSIJoinTablesCatalog).distinct
    logger.info(s"Source Tables from Non-ANSI Join --> ${nonANSIJoinTables.mkString("[", " , ", "]")}")
    logger.info(s"Source Catalog Tables from Non-ANSI Join --> ${nonANSIJoinTablesCatalog.mkString("[", " , ", "]")}")
    logger.info(s"Source Catalog Tables from ANSI Join --> ${catalogTables.mkString("[", " , ", "]")}")
    logger.info(s"Source Catalog Tables from entire SQL --> ${finalList.mkString("[", " , ", "]")}")
    finalList
  }


  /**
    * Prints Stats for Streaming Batch Window
    *
    * @param time     Time Object - Spark Streaming
    * @param listener GIMEL Streaming Listener
    */
  def printStats(time: Time, listener: GimelStreamingListener): Unit = {
    val batchTimeMS = time.milliseconds.toString
    val batchDate = new Date(batchTimeMS.toLong)
    val df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val batchTime = df.format(batchDate)
    logger.info(s"Current Batch ID --> $time | $batchTime | $batchDate")
    logger.info(
      s"""|-----------------------------------------------------------------------
          |Batch ID -->
          |-----------------------------------------------------------------------
          |time               : $time
          |batchTimeMS        : $batchTimeMS
          |batchTime          : $batchTime
          |-----------------------------------------------------------------------
          |Listener Metrics -->
          |-----------------------------------------------------------------------
          |appProcessingDelay : ${listener.appProcessingDelay}
          |appSchedulingDelay : ${listener.appSchedulingDelay}
          |appTotalDelay      : ${listener.appTotalDelay}
          |processingDelay    : ${listener.processingDelay}
          |schedulingDelay    : ${listener.schedulingDelay}
          |totalDelay         : ${listener.totalDelay}
          |-----------------------------------------------------------------------
          |""".stripMargin)
  }

  /**
    * Cache the DataSet (lazily) if its configured to be cached - by user in properties.
    *
    * @param df          DataFrame
    * @param dataSetName DataSetName representing the DataFrame
    * @param options     Props
    */
  def cacheIfRequested(df: DataFrame, dataSetName: String, options: Map[String, String]): Unit = {
    val isCachingEnabled = (
      options.getOrElse(GimelConstants.DATA_CACHE_IS_ENABLED, "false").toBoolean
        && (
        options.getOrElse(s"${GimelConstants.DATA_CACHE_IS_ENABLED}.for.$dataSetName", "false").toBoolean
          || options.getOrElse(s"${GimelConstants.DATA_CACHE_IS_ENABLED_FOR_ALL}", "false").toBoolean
        )
      )
    if (isCachingEnabled) df.cache()
  }

  /**
    * Push downs the SELECT query to JDBC data source and executes using JDBC read.
    *
    * @param selectSQL    SELECT SQL string
    * @param sparkSession : SparkSession
    * @return DataFrame
    */

  def executePushdownQuery(selectSQL: String, sparkSession: SparkSession): DataFrame = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    val dataSetProps = sparkSession.conf.getAll

    // get jdbc Password Strategy
    val jdbcPasswordStrategy = dataSetProps.getOrElse(JdbcConfigs.jdbcPasswordStrategy, JdbcConstants.JDBC_DEFAULT_PASSWORD_STRATEGY).toString
    val jdbcUrl = sparkSession.conf.get(JdbcConfigs.jdbcUrl)

    val jdbcConnectionUtility: JDBCConnectionUtility = JDBCConnectionUtility(sparkSession, dataSetProps)

    // get authUtilities object
    val authUtilities: JDBCAuthUtilities = JDBCAuthUtilities(sparkSession)

    // get real user of JDBC
    val realUser: String = JDBCCommons.getDefaultUser(sparkSession).toString

    // get connection
    val dbConnection = new DbConnection(jdbcConnectionUtility)

    logger.info(s"Final SQL for Query Push Down --> ${selectSQL}")
    try {

      // setting default values for ExtendedJdbcRDD

      // set the local property to pass to executor tasks
      logger.info(s"Setting jdbcPushDownFlag to TRUE in TaskContext for JDBCRdd")

      sparkSession.sparkContext.setLocalProperty(JdbcConfigs.jdbcPushDownEnabled, "true")

      // NOTE: The number of partitions are set to 1. The pushdown query result will always be obtained through one executor. Further optimizations to be explored.
      val jdbcRDD: ExtendedJdbcRDD[Array[Object]] = new ExtendedJdbcRDD(sparkSession.sparkContext, dbConnection, selectSQL, JdbcConstants.DEFAULT_LOWER_BOUND, JdbcConstants.DEFAULT_UPPER_BOUND, 1, JdbcConstants.DEFAULT_READ_FETCH_SIZE, realUser, jdbcPasswordStrategy)

      // get connection
      val conn = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()

      // getting table schema to build final dataframe
      val pushDownSqlAsTempTable = s"( ${selectSQL} ) as pushDownTempTable"
      val tableSchema = JdbcReadUtility.resolveTable(jdbcUrl, pushDownSqlAsTempTable, conn)
      val rowRDD: RDD[Row] = jdbcRDD.map(v => Row(v: _*))
      sparkSession.createDataFrame(rowRDD, tableSchema)
    }
    catch {
      case exec: SQLException =>
        var ex: SQLException = exec
        while (ex != null) {
          ex.printStackTrace()
          ex = ex.getNextException
        }
        throw exec
    }
  }

  /**
    * Returns the flag whether the query has to be pushed down to dataset or not based on dataset provided and user supplied flag for pushdown
    *
    * @param originalSQL  SQLString
    * @param selectSQL    SQLString
    * @param sparkSession : SparkSession
    * @param dataSet      Dataset Object
    * @return STring flag to show whether to push down query or not
    */
  def getQueryPushDownFlag(originalSQL: String, selectSQL: String, sparkSession: SparkSession, dataSet: com.paypal.gimel.DataSet): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val userSuppliedPushDownFlag = sparkSession.conf.get(JdbcConfigs.jdbcPushDownEnabled, "false").toBoolean

    var kafkaDataSets: List[com.paypal.gimel.datasetfactory.GimelDataSet] = List()
    var sqlTmpString = selectSQL
    var sqlOriginalString = originalSQL

    val pCatalogTablesToReplaceAsTmpTable: Map[String, String] = getTablesFrom(selectSQL).map {
      eachSource =>
        val options = getOptions(sparkSession)._2
        if (dataSet.latestKafkaDataSetReader.isDefined) {
          logger.info(s"@$MethodName | Added Kafka Reader for Source --> $eachSource")
          kafkaDataSets = kafkaDataSets ++ List(dataSet.latestKafkaDataSetReader.get)
        }
        val tabNames = eachSource.split("\\.")
        val tmpTableName = "tmp_" + tabNames(1)
        (eachSource, tmpTableName)
    }.toMap

    val queryPushDownFlag: String = pCatalogTablesToReplaceAsTmpTable.foldLeft(userSuppliedPushDownFlag) { (userFlag, kv) =>
      val resolvedSourceTable: String = resolveDataSetName(kv._1)
      val formattedProps: scala.collection.immutable.Map[String, Any] = sparkSession.conf.getAll ++ Map(CatalogProviderConfigs.CATALOG_PROVIDER ->
        sparkSession.conf.get(CatalogProviderConfigs.CATALOG_PROVIDER,
          CatalogProviderConstants.PRIMARY_CATALOG_PROVIDER))
      val dataSetProperties: DataSetProperties =
        CatalogProvider.getDataSetProperties(resolvedSourceTable, formattedProps)
      dataSetProperties.datasetType == GimelConstants.STORAGE_TYPE_JDBC & userFlag
    }.toString
    logger.info(s"queryPushDownFlag for dataset: ${queryPushDownFlag.toLowerCase}")
    queryPushDownFlag.toLowerCase
  }

  private def mergeAllConfs(sparkSession: SparkSession): Map[String, String] = {
    sparkSession.conf.getAll ++ Map(CatalogProviderConfigs.CATALOG_PROVIDER -> sparkSession.conf.get(
      CatalogProviderConfigs.CATALOG_PROVIDER, CatalogProviderConstants.PRIMARY_CATALOG_PROVIDER)
    )
  }

  /**
    * Resolves the Query by replacing Tmp Tables in the Query String
    * For Each Tmp Table placed in the Query String - a DataSet.read is initiated
    * For each Tmp Table - if the dataset is a Kafka DataSet - then each KafkaDataSet object is accumulated
    * Accumulated KafkaDataSet Object will be used towards the end of the Query (on success) -
    * to call check pointing for each topic consumed
    *
    * @param originalSQL  SQLString
    * @param selectSQL    SQLString
    * @param sparkSession : SparkSession
    * @param dataSet      Dataset Object
    * @return Tuple of (Resolved Original SQL, Resolved Select SQL, List of (KafkaDataSet)
    */
  def resolveSQLWithTmpTables(originalSQL: String, selectSQL: String, sparkSession: SparkSession,
                              dataSet: com.paypal.gimel.DataSet): (String, String, List[GimelDataSet], String) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    // get queryPushDown flag
    val queryPushDownFlag = getQueryPushDownFlag(originalSQL, selectSQL, sparkSession, dataSet)

    var kafkaDataSets: List[GimelDataSet] = List()
    var sqlTmpString = selectSQL
    var sqlOriginalString = originalSQL
    val pCatalogTablesToReplaceAsTmpTable: Map[String, String] = getTablesFrom(selectSQL).map {
      eachSource =>
        val options = getOptions(sparkSession)._2
        val df = dataSet.read(eachSource, options)
        cacheIfRequested(df, eachSource, options)

        val tabNames = eachSource.split("\\.")
        val tmpTableName = "tmp_" + tabNames(1)

        // do DataSet.read() only if queryPushDownFlag is set to "false"
        queryPushDownFlag match {
          case "false" =>
            logger.info(s"Setting transformation dataset.read for ${eachSource}")
            logger.info("printing all options during read" + options.toString())
            val df = dataSet.read(eachSource, options)
            cacheIfRequested(df, eachSource, options)
            df.createOrReplaceTempView(tmpTableName)
          case _ =>
          // do nothing if query pushdown is true. No need to do dataset.read
        }

        if (dataSet.latestKafkaDataSetReader.isDefined) {
          logger.info(s"@$MethodName | Added Kafka Reader for Source --> $eachSource")
          kafkaDataSets = kafkaDataSets ++ List(dataSet.latestKafkaDataSetReader.get)
        }
        (eachSource, tmpTableName)
    }.toMap

    // replacing the dataset names with original tables names if queryPushDown is "true"
    queryPushDownFlag match {
      case "true" =>
        logger.info("PATH IS -> QUERY PUSH DOWN")
        pCatalogTablesToReplaceAsTmpTable.foreach { kv =>
          val resolvedSourceTable = resolveDataSetName(kv._1)
          val dataSetProperties: DataSetProperties =
            CatalogProvider.getDataSetProperties(resolvedSourceTable, mergeAllConfs(sparkSession))
          val hiveTableParams = dataSetProperties.props
          val jdbcTableName: String = hiveTableParams(JdbcConfigs.jdbcInputTableNameKey)
          logger.info(s"JDBC input table name : ${jdbcTableName}")
          logger.info(s"Setting JDBC URL : ${hiveTableParams(JdbcConfigs.jdbcUrl)}")
          sparkSession.conf.set(JdbcConfigs.jdbcUrl, hiveTableParams(JdbcConfigs.jdbcUrl))
          logger.info(s"Setting JDBC driver Class : ${hiveTableParams(JdbcConfigs.jdbcDriverClassKey)}")
          sparkSession.conf.set(JdbcConfigs.jdbcDriverClassKey, hiveTableParams(JdbcConfigs.jdbcDriverClassKey))
          sqlTmpString = sqlTmpString.replaceAll(s"(?i)${kv._1}", jdbcTableName)
          sqlOriginalString = sqlOriginalString.replaceAll(s"(?i)${kv._1}", jdbcTableName)
        }
      case _ =>
        logger.info("PATH IS --> DEFAULT")
        pCatalogTablesToReplaceAsTmpTable.foreach { kv =>
          sqlTmpString = sqlTmpString.replaceAll(s"(?i)${kv._1}", kv._2)
          sqlOriginalString = sqlOriginalString.replaceAll(s"(?i)${kv._1}", kv._2)
        }
    }

    logger.info(s"incoming SQL --> $selectSQL")
    logger.info(s"resolved SQL with Temp Table(s) --> $sqlTmpString")
    (sqlOriginalString, sqlTmpString, kafkaDataSets, queryPushDownFlag)
  }

  /**
    * Checks if a Query has Insert or if its just a select
    *
    * @param sql SQL String
    * @return true - if there is an "insert" clause, else false
    */
  def isHavingInsert(sql: String): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)
    QueryParserUtils.isHavingInsert(sql)
  }

  /**
    * Checks whether the sql is of drop table/view pattern and checks whether the table/view is a temp table
    * This will help to take a path to whether to go in livy session or normal gsql session
    *
    * @param sql          - incoming sql
    * @param sparkSession - current spark session
    * @return - true or false based on whether the dropped table/view is a temp (cached) table.
    */
  def isDropTableATempTable(sql: String, sparkSession: SparkSession): Boolean = {
    val dropTableIfExistsPattern = s"DROP TABLE IF EXISTS .(.*)".r
    val dropViewIfExistsPattern = s"DROP VIEW IF EXISTS .(.*)".r
    val dropTablePattern = s"DROP TABLE .(.*)".r
    val dropViewPattern = s"DROP VIEW .(.*)".r
    val uniformSQL = sql.replace("\n", " ")
    val sqlParts: Array[String] = uniformSQL.split(" ")
    val newSql = sqlParts.filter(x => !x.isEmpty).mkString(" ")
    val tableName = newSql.toUpperCase() match {
      case dropTableIfExistsPattern(_) | dropViewIfExistsPattern(_) =>
        newSql.split(" ")(newSql.split(" ").indexWhere(_.toUpperCase() == "EXISTS") + 1)

      case dropTablePattern(_) =>
        newSql.split(" ")(newSql.split(" ").indexWhere(_.toUpperCase() == "TABLE") + 1)

      case dropViewPattern(_) =>
        newSql.split(" ")(newSql.split(" ").indexWhere(_.toUpperCase() == "VIEW") + 1)

      case _ => "."
    }
    if (tableName.contains(".")) {
      false
    } else {
      isSparkCachedTable(tableName, sparkSession)
    }
  }

  /**
    * This function call will check SQL is a DDL
    *
    * @param sql          - Incoming SQL
    * @param sparkSession - Spark Session object
    */

  def isDDL(sql: String, sparkSession: SparkSession): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    QueryParserUtils.isDDL(sql, isDropTableATempTable(sql, sparkSession))
  }

  /**
    * This function call will check whether SQL is setting conf, say - "SET key=val"
    *
    * @param sql          - Incoming SQL
    * @param sparkSession - Spark Session object
    */

  def isSetConf(sql: String, sparkSession: SparkSession): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    val tokenized = GimelQueryUtils.tokenizeSql(sql)
    val nonEmptyStrTokenized = tokenized.filter(x => !x.isEmpty)
    nonEmptyStrTokenized.head.toUpperCase.equals("SET")
  }

  /**
    * isDataDefinition - will find whether we need to take to the Data definition path or select/insert DML path
    *
    * @param sql SQL String from client
    * @return Resulting Boolean
    */

  def isUDCDataDefinition(sql: String): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    // Add alter table
    val catalogName = getCatalogProvider.toUpperCase
    val createTablePattern = s"CREATE TABLE ${catalogName}.(.*)".r
    val createExternalTablePattern = s"CREATE EXTERNAL TABLE ${catalogName}.(.*)".r
    val multisetPattern = s"CREATE MULTISET TABLE ${catalogName}.(.*)".r
    val setPattern = s"CREATE SET TABLE ${catalogName}.(.*)".r
    val dropTablePattern = s"DROP TABLE ${catalogName}.(.*)".r
    val truncateTablePattern = s"TRUNCATE TABLE ${catalogName}.(.*)".r
    val deleteFromPattern = s"DELETE FROM ${catalogName}.(.*)".r
    val deletePattern = s"DELETE ${catalogName}.(.*)".r

    val uniformSQL = sql.replace("\n", " ")
    val sqlParts: Array[String] = uniformSQL.split(" ")
    // remove all additional white spaces in the DDL statment
    val newSql = sqlParts.filter(x => !x.isEmpty).mkString(" ")
    newSql.toUpperCase() match {
      case createTablePattern(_) | createExternalTablePattern(_) | multisetPattern(_) | setPattern(_) | dropTablePattern(_) | truncateTablePattern(_) | deleteFromPattern(_) | deletePattern(_) => {
        true
      }
      case _ => {
        false
      }
    }
  }

  /**
    * Parse the SQL and get the entire select clause
    *
    * @param sql SQL String
    * @return SQL String - that has just the select clause
    */
  def getSelectClause(sql: String): String = {
    QueryParserUtils.getSelectClause(sql)
  }

  /**
    * Parse the SQL and get the entire select clause
    *
    * @param sql SQL String
    * @return SQL String - that has just the select clause
    */
  def getPlainSelectClause(sql: String): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val uniformSQL = sql.replace("\n", " ")
    val sqlParts: Array[String] = uniformSQL.split(" ")
    val index = sqlParts.indexWhere(_.toUpperCase() == "SELECT")
    val selectClauseOnly = sqlParts.slice(index, sqlParts.length).mkString(" ")
    selectClauseOnly
  }

  /**
    * Gets the target table
    *
    * @param sql SQL String
    * @return Table Name
    */
  def getTargetTables(sql: String): Option[String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)
    SQLParser.getTargetTables(sql)
  }

  /**
    * getOptions - read the SparkSession options that was set by the user else add the default values
    *
    * @param sparkSession : SparkSession
    * @return Tuple ( String with concatenated options read from the SparkSession , Same Props as a Map[String, String] )
    */

  def getOptions(sparkSession: SparkSession): (String, Map[String, String]) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val hiveConf: Map[String, String] = sparkSession.conf.getAll
    val optionsToCheck: Map[String, String] = Map(
      KafkaConfigs.rowCountOnFirstRunKey -> "250"
      , KafkaConfigs.batchFetchSize -> "250"
      , KafkaConfigs.maxRecordsPerPartition -> "25000000"
      , GimelConstants.LOG_LEVEL -> "ERROR"
      , KafkaConfigs.kafkaConsumerReadCheckpointKey -> "true"
      , KafkaConfigs.kafkaConsumerClearCheckpointKey -> "false"
      , KafkaConfigs.maxRatePerPartitionKey -> "3600"
      , KafkaConfigs.streamParallelKey -> "10"
      , KafkaConfigs.defaultBatchInterval -> "30"
      , KafkaConfigs.isStreamParallelKey -> "true"
      , KafkaConfigs.streamaWaitTerminationOrTimeoutKey -> "-1"
      , KafkaConfigs.isBackPressureEnabledKey -> "true"
      , JdbcConfigs.teradataReadType -> ""
      , HbaseConfigs.hbaseOperation -> "scan"
      , HbaseConfigs.hbaseFilter -> ""
      , GimelConstants.DATA_CACHE_IS_ENABLED -> "false"
      , GimelConstants.DATA_CACHE_IS_ENABLED_FOR_ALL -> "true"
      , KafkaConfigs.isStreamBatchSwitchEnabledKey -> "false"
      , KafkaConfigs.streamFailureThresholdPerSecondKey -> "1500"
      , ElasticSearchConfigs.esIsDailyIndex -> "false"
      , CatalogProviderConfigs.CATALOG_PROVIDER -> CatalogProviderConstants.PRIMARY_CATALOG_PROVIDER
      , GimelConstants.SPARK_APP_ID -> sparkSession.conf.get(GimelConstants.SPARK_APP_ID)
      , GimelConstants.SPARK_APP_NAME -> sparkSession.conf.get(GimelConstants.SPARK_APP_NAME)
      , GimelConstants.APP_TAG -> com.paypal.gimel.common.utilities.DataSetUtils.getAppTag(sparkSession.sparkContext)
    )
    val resolvedOptions: Map[String, String] = optionsToCheck.map { kvPair =>
      (kvPair._1, hiveConf.getOrElse(kvPair._1, kvPair._2))
    }
    resolvedOptions.foreach(conf => sparkSession.conf.set(conf._1, conf._2))
    (resolvedOptions.map(x => x._1 + "=" + x._2).mkString(":"), hiveConf ++ resolvedOptions)
  }


  /**
    * Executes the SQL and Returns DataFrame
    *
    * @param selectSQL    The Select SQL
    * @param sparkSession : SparkSession
    * @return DataFrame
    */
  def executeSelectClause(selectSQL: String, sparkSession: SparkSession, queryPushDownFlag: String): DataFrame = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    val selectDF: DataFrame = queryPushDownFlag match {
      case "true" =>

        // set the SparkContext as well as TaskContext property for JdbcPushDown flag to "false"
        //        logger.info(s"Setting jdbcPushDownFlag to false in SparkContext")
        //        sparkSession.conf.set(JdbcConfigs.jdbcPushDownEnabled, "false")

        logger.info(s"Executing Pushdown Query: ${selectSQL}")
        val df = executePushdownQuery(selectSQL, sparkSession)

        df
      case _ =>
        sparkSession.sql(selectSQL)
    }
    selectDF
  }

  /**
    * Executes the Resolved SQL Query by calling the DataSet code that has been generated
    *
    * @param clientSQL    Original SQL String submitted by Client
    * @param dest         Target Table
    * @param selectSQL    SQl String for Select Clause alone
    * @param sparkSession :SparkSession
    * @param dataset      DataSet
    * @return Result String
    */
  def executeResolvedQuery(clientSQL: String, dest: Option[String], selectSQL: String, sparkSession: SparkSession,
                           dataset: com.paypal.gimel.DataSet, queryPushDownFlag: String): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    logger.info(s"Client SQL is --> $clientSQL")
    logger.info(s"Select SQL is --> $selectSQL")
    var resultString = ""
    if (dest.isDefined) {
      logger.info(s"EXECUTION PATH ====== DATASET WRITE ======")
      if (clientSQL.toLowerCase.contains("partition")) {
        sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      }
      Try {
        val options = getOptions(sparkSession)._2
        val selectDF = executeSelectClause(selectSQL, sparkSession, queryPushDownFlag)
        // --- EXISTING LOGIC
        // dataset.write(dest.get, selectDF, options)
        // --- NEW LOGIC
        // Get the DataSet Properties

        val tgt = dest.get
        (tgt.split(",").length > 2) match {
          case true =>
          case _ =>
        }
        val dataSetProperties: DataSetProperties = CatalogProvider.getDataSetProperties(dest.get)
        //        val dataSetProperties = GimelServiceUtilities().getDataSetProperties(dest.get)
        dataSetProperties.datasetType.toString match {
          case "HIVE" | "NONE" =>
            // If Hive
            val sqlToInsertIntoHive = queryPushDownFlag.toLowerCase match {
              case "true" =>
                logger.info(s"Invoking write API in gimel with queryPushDownFlag=${queryPushDownFlag}...")

                // create a temp view for pushdown dataframe.
                val jdbcPushDownTempTable = "jdbcPushDownTempTable"
                logger.info(s"Creating temp view for pushdown query dataframe as ${jdbcPushDownTempTable}")
                selectDF.createOrReplaceTempView(jdbcPushDownTempTable)

                val pushDownSelectQuery = s"SELECT * FROM ${jdbcPushDownTempTable}"

                // replace selectSQL in clientSQL with pushDownSelectQuery
                logger.info(s"Replacing ${selectSQL} in ${clientSQL} with ${pushDownSelectQuery}")
                val pushDownSparkSql = clientSQL.replace(selectSQL, pushDownSelectQuery)
                // dataset.write(dest.get, selectDF, options)
                logger.info(s"Spark SQL after Pushdown Query: ${pushDownSparkSql}")
                pushDownSparkSql
              case _ =>
                logger.info(s"Invoking sparkSession.sql for write with queryPushDownFlag=${queryPushDownFlag}...")
                // Get the DB.TBL from UDC
                clientSQL
            }
            // execute on hive
            val db = dataSetProperties.props(HiveConfigs.hiveDBName)
            val tbl = dataSetProperties.props(HiveConfigs.hiveTableName)
            val actual_db_tbl = s"${db}.${tbl}"
            // Replace the SQL with DB.TBL
            logger.info(s"Replacing ${dest.get} with ${actual_db_tbl}")
            val sqlToExecute = sqlToInsertIntoHive.replaceAll(s"(?i)${dest.get}", actual_db_tbl)
            logger.info(s"Passing through SQL to Spark for write since target [${actual_db_tbl}] is of data set type - HIVE ...")
            logger.info(s"Final SQL to Run --> \n ${sqlToExecute}")
            sparkSession.sql(sqlToExecute)
          case _ =>
            // If Non-HIVE
            logger.info(s"Invoking write API in gimel with queryPushDownFlag=${queryPushDownFlag}...")
            dataset.write(dest.get, selectDF, options)
        }

      } match {
        case Success(_) =>
          resultString = "Query Completed."
          logger.info(resultString)
        case Failure(e) =>
          // e.printStackTrace()
          resultString =
            s"""Query Failed in function : $MethodName via path dataset.write. Error -->
               |
               |${e.toString}""".stripMargin
          // logger.error(resultString)
          throw e
      }
    } else {
      logger.info(s"EXECUTION PATH ====== DATASET SELECT ======")
      val selectDF: DataFrame = queryPushDownFlag match {
        case "true" =>
          //          logger.info(s"Setting jdbcPushDownFlag to false in SparkContext")
          //          sparkSession.conf.set(JdbcConfigs.jdbcPushDownEnabled, "false")
          val df = executePushdownQuery(selectSQL, sparkSession)
          df
        case _ =>
          sparkSession.sql(selectSQL)
      }
      val count = selectDF.cache.count
      val rowsToShow = sparkSession.conf.get(GimelConstants.MAX_RESULTS_TO_SHOW, "250").toInt
      val showRowsOnly = sparkSession.conf.get(GimelConstants.SHOW_ROWS_ENABLED, "false").toBoolean
      val resultSet = selectDF.take(rowsToShow).mkString("\n")
      val marginString = "-------------------------------------------------------------------------------------------------------"
      val extraMessage =
        s"""
           |$marginString
           |Total Rows Returned from original Query --> $count
           |Displaying Rows ${scala.math.min(rowsToShow, count)} of $count
           |
           |$userInfoString
        """.stripMargin
      resultString =
        s"""${if (!showRowsOnly) extraMessage else ""}
           |$marginString
           |$resultSet
           |$marginString""".stripMargin
    }
    resultString
  }

  /**
    * Executes the Resolved SQL Query by calling the DataSet code that has been generated
    *
    * @param clientSQL    Original SQL String submitted by Client
    * @param dest         Target Table
    * @param selectSQL    SQl String for Select Clause alone
    * @param sparkSession : SparkSession
    * @param dataset      DataSet
    * @return RDD[Result JSON String]
    */
  //  def executeResolvedQuerySparkMagic(clientSQL: String, dest: Option[String], selectSQL: String, hiveContext: HiveContext, dataset: DataSet): RDD[String] = {
  //    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
  //
  //    logger.info(" @Begin --> " + MethodName)
  //
  //    logger.info(s"Client SQL is --> $clientSQL")
  //    logger.info(s"Select SQL is --> $selectSQL")
  //    logger.silence
  //    val selectDF = hiveContext.sql(selectSQL)
  //    selectDF.toJSON
  //  }

  def executeResolvedQuerySparkMagic(clientSQL: String, dest: Option[String], selectSQL: String, sparkSession: SparkSession, dataset: com.paypal.gimel.DataSet, queryPushDownFlag: String): RDD[String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    logger.info(s"Client SQL is --> $clientSQL")
    logger.info(s"Select SQL is --> $selectSQL")
    var resultString = ""
    if (dest.isDefined) {
      logger.info(s"EXECUTION PATH ====== DATASET WRITE ======")
      if (clientSQL.toLowerCase.contains("partition")) {
        sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      }
      Try {
        val (_, options) = getOptions(sparkSession)
        val selectDF: DataFrame = queryPushDownFlag match {
          case "true" =>
            //            logger.info(s"Setting jdbcPushDownFlag to false in SparkContext")
            //            sparkSession.conf.set(JdbcConfigs.jdbcPushDownEnabled, "false")
            val df = executePushdownQuery(selectSQL, sparkSession)
            df
          case _ =>
            sparkSession.sql(selectSQL)
        }
        dataset.write(dest.get, selectDF, options)
      } match {
        case Success(_) =>
          resultString = """{"Query Execution":"Success"}"""
          logger.info(resultString)
          sparkSession.read.json(sparkSession.sparkContext.parallelize(Seq(resultString))).toJSON.rdd
        case Failure(e) =>
          // e.printStackTrace()
          resultString =
            s"""{"Query Execution Failed":${e.toString}}"""
          // logger.error(resultString)
          sparkSession.read.json(sparkSession.sparkContext.parallelize(Seq(resultString))).toJSON.rdd
        //          throw e
      }
    } else {
      logger.info(s"EXECUTION PATH ====== DATASET SELECT ======")
      val selectDF: DataFrame = queryPushDownFlag match {
        case "true" =>
          //          logger.info(s"Setting jdbcPushDownFlag to false in SparkContext")
          //          sparkSession.conf.set(JdbcConfigs.jdbcPushDownEnabled, "false")
          val df = executePushdownQuery(selectSQL, sparkSession)
          df
        case _ =>
          sparkSession.sql(selectSQL)
      }
      //      val count = selectDF.cache.count
      val rowsToShow = sparkSession.conf.get(GimelConstants.MAX_RESULTS_TO_SHOW, "250").toInt
      selectDF.registerTempTable("tmp_table_spark_magic")
      val resultSet = sparkSession.sql(s"select * from tmp_table_spark_magic limit ${rowsToShow}").toJSON.rdd
      resultSet
    }
  }

  /**
    * Checks whether a table is cached in spark Catalog
    *
    * @param tableName    - incoming table name
    * @param sparkSession - spark session
    * @return - A boolean value to tell whether the table is cached or not
    */

  def isSparkCachedTable(tableName: String, sparkSession: SparkSession): Boolean = {

    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    val isCached = Try {
      sparkSession.catalog.isCached(tableName)
    }
    match {
      case Success(result) => {
        result match {
          case true => true
          case _ => false
        }
      }
      case Failure(e) => false
    }
    isCached match {
      case true => logger.info(tableName + "====> a Cached table")
      case _ => logger.info(tableName + "====> NOT a Cached table")
    }
    isCached
  }

  /**
    * Checks whether the dataSet is HIVE by scanning the pcatalog phrase and also expecting to have the db and table
    * names to decide it is a HIVE table
    *
    * @param dataSet DataSet
    * @return Boolean
    */

  def isStorageTypeUnknown
  (dataSet: String): Boolean = {
    dataSet.split('.').head.toLowerCase() != GimelConstants.PCATALOG_STRING && dataSet.split('.').length == 2
  }

  /**
    * Resolves the Query by replacing Tmp Tables in the Query String
    * For Each Tmp Table placed in the Query String - a DataSet.read is initiated
    * For each Tmp Table - if the dataset is a Kafka DataSet - then each KafkaDataSet object is accumulated
    * Accumulated KafkaDataSet Object will be used towards the end of the Query (on success) -
    * to call check pointing for each topic consumed
    *
    * @param sql          SQL String
    * @param sparkSession : SparkSession
    * @param dataSet      DataSet
    * @return Tuple ( Target Table, select SQL String, List(KafkaDataSet)  )
    */
  def resolveSQL(sql: String, sparkSession: SparkSession, dataSet: com.paypal.gimel.DataSet):
  (String, Option[String], String, List[GimelDataSet], String) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info("@Begin --> " + MethodName)

    logger.info(s"incoming SQL --> $sql")
    val uniformSQL = sql.replace("\n", " ")
    val selectClauseOnly = getSelectClause(uniformSQL)
    val (originalSQL, selectClause, kafkaDataSets, queryPushDownFlag) =
      resolveSQLWithTmpTables(sql, selectClauseOnly, sparkSession, dataSet)
    val targetTable = getTargetTables(sql)
    logger.info(s"selectClause --> $selectClause")
    logger.info(s"destination --> $targetTable")
    (originalSQL, targetTable, selectClause, kafkaDataSets, queryPushDownFlag)
  }

  /**
    * Checks whether partitioned by clause is there so that we can pull out the partitions spec
    *
    * @param sql - incoming sql string
    * @return - Boolean value to see whether partitioned clause presents or not
    */
  def existsPartitionedByClause(sql: String): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)
    sql.toUpperCase().contains(GimelConstants.HIVE_DDL_PARTITIONED_BY_CLAUSE)
  }

  private lazy val userInfoString =
    s"""
       |------------------------------
       |User controllable Properties
       |------------------------------
       |
       |Query Results & Helper
       |----------------------
       |${GimelConstants.SHOW_ROWS_ENABLED} --> Set this to "true" to stop getting all these messages. (Default : false)
       |${GimelConstants.MAX_RESULTS_TO_SHOW} --> Number of rows to display in interactive mode (Default : 1000)
       |
       |Data Caching Options
       |----------------------
       |${GimelConstants.DATA_CACHE_IS_ENABLED} --> true indicates dataset caching is enabled (Default : false)
       |${GimelConstants.DATA_CACHE_IS_ENABLED}.for.pcatalog.flights --> if this = true & ${GimelConstants.DATA_CACHE_IS_ENABLED}=true, then only pcatalog.flights from query will be cached. (Default : false)
       |${GimelConstants.DATA_CACHE_IS_ENABLED_FOR_ALL} --> if this = true, then all pcatalog datasets in query will be cached (Default : true)
       |
       |Logging Level
       |----------------------
       |${GimelConstants.LOG_LEVEL} --> set to INFO, DEBUG, WARN, ERROR to get desired level of logging (Default : ERROR)
       |
       |kafka Checkpointing
       |----------------------
       |${KafkaConfigs.kafkaConsumerReadCheckpointKey} --> true indicates check-pointing enabled (Default : true)
       |${KafkaConfigs.kafkaConsumerClearCheckpointKey} --> true indicates checkpoint will be cleared before this run begins (Default : false)
       |
       |kafka Stream Throttle
       |----------------------
       |${KafkaConfigs.maxRatePerPartitionKey} --> Spark Configuration for Streaming Rate (Default : 3600, empirically derived)
       |${KafkaConfigs.isStreamParallelKey} --> true causes ordering to be lost, but performance gain via parallelism factor. (Default : true)
       |${KafkaConfigs.streamParallelKey} --> Number of parallel threads to run while processing data after fetching from kafka (Default : 10)
       |${KafkaConfigs.defaultBatchInterval} --> Streaming Window Seconds (Default : 30)
       |
       |kafka Batch Throttle
       |----------------------
       |${KafkaConfigs.rowCountOnFirstRunKey} --> Fetches Only Supplied number of rows from Kafka (Default : 25 Million)
       |${KafkaConfigs.maxRecordsPerPartition} --> Advanced options to further restrict how many messages we can read from each partition - in batch mode Kafka Read (Default 25 Million rows, for this to be effective, value should be <= throttle.batch.fetchRowsOnFirstRun)
       |${KafkaConfigs.batchFetchSize} --> Advanced options to parallelize in batch mode Kafka Read (Default 250) --> This will parallelize 25 Million into 250 threads
       |
       |HBase
       |-----------------------
       |${HbaseConfigs.hbaseOperation} -> Type of operation to be performed on HBase. Can be scan for reading all data or get for lookup
       |${HbaseConfigs.hbaseFilter} -> Filter condition for HBase lookup. Example: rowKey=1:toGet=cf1-c1,c2|cf2-c3
       |
       |Elastic
       |-----------------------
       |${ElasticSearchConfigs.esIsPartitioned}-> Is the index partitioned or not ?
       |${ElasticSearchConfigs.esDelimiter}-> What is the delimiter which separates the index name with the partition
       |${ElasticSearchConfigs.esPartition}-> "*" -> wild card to include all the specific partitions
       |${ElasticSearchConfigs.esDefaultReadForAllPartitions}-> flag which indicates whether to read all partitions or not
       |${ElasticSearchConfigs.esMapping}-> flag which gets the schema from the user
     """.stripMargin
}
