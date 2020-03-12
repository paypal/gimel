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

import java.nio.charset.StandardCharsets
import java.sql.SQLException
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import com.google.common.hash.Hashing
import org.apache.commons.lang3.ArrayUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.Time

import com.paypal.gimel.common.catalog.{CatalogProvider, DataSetProperties}
import com.paypal.gimel.common.conf.{GimelConstants, _}
import com.paypal.gimel.common.gimelserde.GimelSerdeUtils
import com.paypal.gimel.common.utilities.{DataSetType, GenericUtils, RandomGenerator}
import com.paypal.gimel.common.utilities.DataSetUtils._
import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.elasticsearch.conf.ElasticSearchConfigs
import com.paypal.gimel.hbase.conf.HbaseConfigs
import com.paypal.gimel.hive.conf.HiveConfigs
import com.paypal.gimel.jdbc.conf.{JdbcConfigs, JdbcConstants}
import com.paypal.gimel.jdbc.utilities._
import com.paypal.gimel.jdbc.utilities.JdbcAuxiliaryUtilities._
import com.paypal.gimel.jdbc.utilities.PartitionUtils.ConnectionDetails
import com.paypal.gimel.kafka.conf.{KafkaConfigs, KafkaConstants}
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.logging.GimelStreamingListener
import com.paypal.gimel.parser.utilities.{QueryParserUtils, SearchCriteria, SearchSchemaUtils, SQLNonANSIJoinParser}

object GimelQueryUtils {

  val logger: Logger = Logger(this.getClass.getName)
  /*
   * Regex for substituting tmp table in a sql.
   * This regex matches if key is preceded by any whitespace character - new line, tab, space
   * and followed by (new line, tab, space, round brackets, semi colon and comma) or is at end of line ($$)
   * ?<! negative loohbehind operator is used for checking the character preceding
   * ?i is used for ignore case
   * ?= positive lookahead operator is used for checking the characters following
   * $$ is used to check if it is at the end of line
   */
  val regexTmpTable = s"(?<!\\S)(?i)key(?=[\\n\\t \\(\\);,]|$$)"

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
        // Create a random string with random length for tmp table
        val randomString = RandomGenerator.getRandomString(
          RandomGenerator.getRandomInt(GimelConstants.GSQL_TMP_TABLE_RANDOM_GENERATOR_MIN,
            GimelConstants.GSQL_TMP_TABLE_RANDOM_GENERATOR_MAX))
        val tmpTableName = "tmp_" + eachSource.replaceAll("[^\\w\\s]", "_") + "_" + randomString

        // do DataSet.read() only if queryPushDownFlag is set to "false"
        queryPushDownFlag match {
          case "false" =>
            logger.info(s"Setting transformation dataset.read for ${eachSource}")
            logger.info("printing all options during read" + options.toString())
            val datasetProps = CatalogProvider.getDataSetProperties(eachSource, options)
            /*
             * Sets the appropriate deserializer class based on the kafka.message.value.type and value.serializer properties
             * This is mainly required for backward compatibility for KAFKA datasets
             */
            val newOptions = GimelSerdeUtils.setGimelDeserializer(sparkSession, datasetProps, options)
            val df = dataSet.read(eachSource, newOptions)
            cacheIfRequested(df, eachSource, newOptions)
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
          sqlTmpString = getSQLWithTmpTable(sqlTmpString, kv._1, jdbcTableName)
          sqlOriginalString = getSQLWithTmpTable(sqlOriginalString, kv._1, jdbcTableName)
        }
      case _ =>
        logger.info("PATH IS --> DEFAULT")
        pCatalogTablesToReplaceAsTmpTable.foreach { kv =>
          sqlTmpString = getSQLWithTmpTable(sqlTmpString, kv._1, kv._2)
          sqlOriginalString = getSQLWithTmpTable(sqlOriginalString, kv._1, kv._2)
        }
    }

    logger.info(s"incoming SQL --> $selectSQL")
    logger.info(s"resolved SQL with Temp Table(s) --> $sqlTmpString")
    (sqlOriginalString, sqlTmpString, kafkaDataSets, queryPushDownFlag)
  }

  /*
   * Substitutes dataset name with tmp table in sql using regex
   *
   * @param sql
   * @param datasetName : Mame of dataset to substitute
   * @param tmpTableName : Temp table name to substitute
   *
   * Example:
   * sql = select * from udc.hive.test.flights
   * key = udc.hive.test.flights
   * This should match udc.hive.test.flights in the sql string.
   *
   * sql = select * fromudc.hive.test.flights
   * key = udc.hive.test.flights
   * This should not match udc.hive.test.flights in the sql string.
   *
   * sql = select * from udc.hive.test.flights_schedule
   * key = udc.hive.test.flights
   * This should not match udc.hive.test.flights in the sql string.
   */
  def getSQLWithTmpTable(sql: String, datasetName: String, tmpTableName: String): String = {
    sql.replaceAll(regexTmpTable.replace("key", datasetName), tmpTableName)
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
      , GimelConstants.APP_TAG -> getAppTag(sparkSession.sparkContext)
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
        val dataSetProperties: DataSetProperties = CatalogProvider.getDataSetProperties(dest.get, options)
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
            /*
               * Sets the appropriate serializer class based on the kafka.message.value.type and value.serializer properties
               * This is mainly required for backward compatibility for KAFKA datasets
              */
            val newOptions = GimelSerdeUtils.setGimelSerializer(sparkSession, dataSetProperties, options)
            dataset.write(dest.get, selectDF, newOptions)
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
    * Checks if a Query has Cache statemnt
    *
    * @param sql SQL String
    * @return true - if there is an "Cache" clause, else false
    */
  def isHavingCache(sql: String): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    GimelQueryUtils.tokenizeSql(sql).head.equalsIgnoreCase("cache")
  }

  /**
    * This function tokenizes the incoming sql and parses it using GSQL parser and identify whether the query is of Select type
    * If it is a select query, it checks whether it is of HBase and has limit clause.
    *
    * @param sql          - Incoming SQL
    * @param options      - set of Options from the user
    * @param sparkSession - spark session
    * @return
    */
  def setLimitForHBase(sql: String, options: Map[String, String],
                       sparkSession: SparkSession): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    Try {
      val nonEmptyStrTokenized = GimelQueryUtils.tokenizeSql(sql)
      nonEmptyStrTokenized.head.toLowerCase match {
        case "select" =>
          val selectTables = getAllTableSources(sql)
          // Checks if there is more than 1 source tables
          if (selectTables.isEmpty || selectTables.length > 1) return
          selectTables.map(eachTable => getSystemType(eachTable, sparkSession, options) match {
            case DataSetType.HBASE =>
              logger.info("Sql contains limit clause, setting the HBase Page Size.")
              val limit = Try(QueryParserUtils.getLimit(sql)).get
              sparkSession.conf.set(GimelConstants.HBASE_PAGE_SIZE, limit)
            case _ =>
              return
          })
        case _ =>
          return
      }
    } match {
      case Success(_) =>
      case Failure(exception) =>
        logger.error(s"Exeception occurred while setting the limit for HBase -> ${exception.getMessage}")
        throw exception
    }
  }

  /**
    * Parse the SQL and get cache Query & select statement
    *
    * @param sql SQL String
    */
  def splitCacheQuery(sql: String): (Option[String], String) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val uniformSQL = sql.replace("\n", " ")
    val sqlParts: Array[String] = uniformSQL.split(" ")
    if (isHavingCache(sql)) {
      logger.info("Splitting sql since it contains cache table")
      val index = sqlParts.indexWhere(_.toUpperCase() == "SELECT")
      (Some(sqlParts.slice(0, index).mkString(" ")), sqlParts.slice(index, sqlParts.length).mkString(" "))
    } else {
      (None, sqlParts.mkString(" "))
    }
  }

  /**
    * This method will execute the ' cache table t as...'  query
    *
    * @param cacheStatment cache table statement
    * @param dataFrame     pushdown dataframe
    * @param sparkSession  sparksesssion
    * @return dataframe
    */
  def cachePushDownQuery(cacheStatment: String, dataFrame: DataFrame, sparkSession: SparkSession): DataFrame = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    // create a temp view for pushdown dataframe.
    val pushDownCacheTempTable = "pushDownCacheTempTable"
    logger.info(s"Creating temp view for pushdown query dataframe as ${pushDownCacheTempTable}")
    dataFrame.createOrReplaceTempView(pushDownCacheTempTable)

    val sql =
      s"""
         | ${cacheStatment} SELECT * FROM ${pushDownCacheTempTable}
      """.stripMargin

    // execute the cached statement
    logger.info(s"Now caching dataframe for pushdown query: ${sql}")
    sparkSession.sql(sql)
  }

  /**
    * Push downs the SELECT query to JDBC data source and executes using JDBC read.
    *
    * @param inputSQL     SELECT SQL string
    * @param sparkSession : SparkSession
    * @return DataFrame
    */

  def executePushdownQuery(inputSQL: String, sparkSession: SparkSession): DataFrame = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    // check if SQL contains cache query
    val (cacheStatement, selectSQL) = splitCacheQuery(inputSQL)

    val dataSetProps = sparkSession.conf.getAll
    val jdbcOptions: Map[String, String] = JdbcAuxiliaryUtilities.getJDBCOptions(dataSetProps)

    if (!jdbcOptions.contains(JdbcConfigs.jdbcUrl)) {
      throw new IllegalArgumentException("No JDBC url found. Please verify the dataset name in query")
    }

    val userSpecifiedFetchSize = dataSetProps.getOrElse("fetchSize", JdbcConstants.DEFAULT_READ_FETCH_SIZE).toString.toInt

    try {
      val jdbcSystem = getJDBCSystem(jdbcOptions(JdbcConfigs.jdbcUrl))
      val pushDownDf = jdbcSystem match {
        case JdbcConstants.TERADATA =>
          executeTeradataSelectPushDownQuery(sparkSession, selectSQL, dataSetProps, jdbcOptions, userSpecifiedFetchSize)
        case _ =>
          val pushDownSqlAsTempTable = s"( $selectSQL ) as pushDownTempTable"
          logger.info(s"Final SQL for Query Push Down --> $pushDownSqlAsTempTable")
          val jdbcConnectionUtility: JDBCConnectionUtility = JDBCConnectionUtility(sparkSession, dataSetProps)
          JdbcAuxiliaryUtilities.sparkJdbcRead(sparkSession, jdbcOptions(JdbcConfigs.jdbcUrl), pushDownSqlAsTempTable,
            None, JdbcConstants.DEFAULT_LOWER_BOUND, JdbcConstants.DEFAULT_UPPER_BOUND,
            1, userSpecifiedFetchSize, jdbcConnectionUtility.getConnectionProperties)
      }

      // cache query if inputSql contains cache query
      cacheStatement match {
        case Some(cacheTable) =>
          // cache the query results from pushdown
          logger.info(s"Now caching the dataframe for ->  $selectSQL")
          cachePushDownQuery(cacheTable, pushDownDf, sparkSession)
        case _ =>
          pushDownDf
      }
    }
    catch {
      case exec: SQLException =>
        val errors = new mutable.StringBuilder()
        var ex: SQLException = exec
        var lastException: SQLException = exec
        while (ex != null) {
          if (errors.nonEmpty) {
            errors.append(s"${GimelConstants.COMMA} ")
          }
          errors.append(s = ex.getErrorCode().toString)
          lastException = ex
          ex = ex.getNextException
        }
        if (lastException != null) {
          lastException.printStackTrace()
        }
        logger.error(s"SQLException: Error codes ${errors.toString()}")
        throw exec
      case e: Throwable =>
        throw e
    }
    finally {
      // re-setting all configs for JDBC
      JDBCCommons.resetPushDownConfigs(sparkSession)
    }
  }


  def executeTeradataSelectPushDownQuery(sparkSession: SparkSession, selectSQL: String,
                                         dataSetProps: Map[String, String], jdbcOptions: Map[String, String],
                                         userSpecifiedFetchSize: Int): DataFrame = {
    logger.info(s" @Begin --> ${new Exception().getStackTrace.apply(1).getMethodName}")
    val jdbcConnectionUtility: JDBCConnectionUtility = JDBCConnectionUtility(sparkSession, dataSetProps)
    import JDBCUtilities._
    val loggerOption = Some(logger)
    val mutableJdbcOptions: mutable.Map[String, String] = scala.collection.mutable.Map(jdbcOptions.toSeq: _*)
    var sqlToBeExecutedInJdbcRDD: String = selectSQL
    logger.info(s"In query pushdown SQL to be executed --> $sqlToBeExecutedInJdbcRDD")

    // Get connection details per the explain plan of the incomingSql
    import JDBCConnectionUtility.withResources
    var (connectionDetails, connectionUtilityPerIncomingSQL): (ConnectionDetails, JDBCConnectionUtility) =
      (null, jdbcConnectionUtility)
    var partitionColumns: Seq[String] = Seq.empty
    withResources(getOrCreateConnection(connectionUtilityPerIncomingSQL, logger = loggerOption)) {
      connection =>
        // get the partition columns
        partitionColumns = JdbcAuxiliaryUtilities.getAndSetPartitionParameters(
          sparkSession, dataSetProps, userSpecifiedFetchSize, mutableJdbcOptions, connection)
        val tuple = JdbcAuxiliaryUtilities.getConnectionInfo(sparkSession,
          jdbcConnectionUtility, dataSetProps, sqlToBeExecutedInJdbcRDD, loggerOption, partitionColumns)
        connectionDetails = tuple._1
        connectionUtilityPerIncomingSQL = tuple._2
    }

    // Create a new connection as per the new config
    withResources(getOrCreateConnection(connectionUtilityPerIncomingSQL, logger = loggerOption)) {
      connection =>
        // if partitions greater than 1
        if (connectionDetails.numOfPartitions > 1) {
          // if sql having analytical functions
          if (QueryParserUtils.isHavingAnalyticalFunction(selectSQL)) {
            require(dataSetProps.contains(JdbcConfigs.jdbcTempDatabase),
              s"Expecting CONF: ${JdbcConfigs.jdbcTempDatabase} to be available")
            val tableName =
              s"${dataSetProps(JdbcConfigs.jdbcTempDatabase)}.gimel_push_down_${
                Hashing.sha256().hashString(selectSQL, StandardCharsets.UTF_8)
                  .toString.substring(0, 7)
              }"
            logger.info(s"Resolved temp table name: $tableName")
            // delete the temp table if it exists
            JdbcAuxiliaryUtilities.dropTable(tableName, connection, logger = loggerOption)
            // create volatile table as select with data
            // Recording the time taken for the query execution
            val createTableStatement: String = s"CREATE TABLE $tableName AS ( ${selectSQL.trim} ) WITH DATA "
            logger.info(s"Proceeding to execute: $createTableStatement")
            JdbcAuxiliaryUtilities.executeQueryStatement(createTableStatement, connection,
              incomingLogger = loggerOption, recordTimeTakenToExecute = true)
            // rewrite the selectSql with `select * from temp_table` and set JdbcConfigs.jdbcDbTable => temp_table
            sqlToBeExecutedInJdbcRDD = s"SELECT * from $tableName"
            mutableJdbcOptions += (JdbcConfigs.jdbcTempTable -> tableName)
            mutableJdbcOptions += (JdbcConfigs.jdbcDbTable -> tableName)
            // Set the first column name as partition column if data split is needed
            if (!dataSetProps.contains(JdbcConfigs.jdbcPartitionColumns)) {
              val tempTableSchema = JdbcReadUtility.resolveTable(
                mutableJdbcOptions(JdbcConfigs.jdbcUrl),
                sqlToBeExecutedInJdbcRDD, connection
              )
              mutableJdbcOptions += (JdbcConfigs.jdbcPartitionColumns -> tempTableSchema.head.name)
            }
          }
        }

        if (!selectSQL.equals(sqlToBeExecutedInJdbcRDD)) {
          logger.info("Re-calculating the connection info as the SQL to be executed is changed ")
          val tuple = JdbcAuxiliaryUtilities.getConnectionInfo(sparkSession,
            jdbcConnectionUtility, dataSetProps, sqlToBeExecutedInJdbcRDD, loggerOption, partitionColumns)
          // below syntax to override compilation error
          connectionDetails = tuple._1
          connectionUtilityPerIncomingSQL = tuple._2
        }

        // create JDBC rdd

        logger.info(s"Final SQL for Query Push Down --> $sqlToBeExecutedInJdbcRDD")
        val tableSchema = JdbcReadUtility.resolveTable(
          mutableJdbcOptions(JdbcConfigs.jdbcUrl),
          sqlToBeExecutedInJdbcRDD,
          connection
        )

        JdbcAuxiliaryUtilities.createJdbcDataFrame(sparkSession, sqlToBeExecutedInJdbcRDD,
          connectionDetails.fetchSize, connectionDetails.numOfPartitions,
          connectionUtilityPerIncomingSQL, partitionColumns, tableSchema)
    }
  }

  def validateAllTablesAreFromSameJdbcSystem(sparkSession: SparkSession,
                                             tables: Seq[String],
                                             sqlToBeExecuted: String): (Boolean, Option[Map[String, String]]) = {
    val dataSetPropertiesForAllTables: Iterable[Option[DataSetProperties]] = tables.map {
      tableName =>
        Try(CatalogProvider.getDataSetProperties(tableName, mergeAllConfs(sparkSession))).toOption
    }
    if (dataSetPropertiesForAllTables.nonEmpty && dataSetPropertiesForAllTables.head.isDefined) {
      var queryPushDownFlag: Boolean = false
      val headJdbcUrl = dataSetPropertiesForAllTables.head.get.props.get(JdbcConfigs.jdbcUrl)
      if (headJdbcUrl.isDefined) {
        queryPushDownFlag = dataSetPropertiesForAllTables.forall {
          dataSetProperty =>
            dataSetProperty.isDefined && dataSetProperty.get.datasetType == GimelConstants.STORAGE_TYPE_JDBC &&
              dataSetProperty.get.props.contains(JdbcConfigs.jdbcUrl) &&
              headJdbcUrl.get.equalsIgnoreCase(dataSetProperty.get.props(JdbcConfigs.jdbcUrl))
        }
      }
      if (queryPushDownFlag && JdbcAuxiliaryUtilities.validatePushDownQuery(sparkSession,
        tables.head, sqlToBeExecuted)) {
        // Getting connection info from dataset properties else from the incoming properties
        (queryPushDownFlag, Some(JdbcAuxiliaryUtilities.getJDBCOptions(
          Map(GimelConstants.DATASET_PROPS -> dataSetPropertiesForAllTables.head.get)
        )))
      } else {
        (false, None)
      }
    } else {
      (false, None)
    }
  }

  def validateAllDatasetsAreFromSameJdbcSystem(datasets: Seq[String]): Boolean = {
    var areAllDatasetFromSameJdbcSystem: Boolean = false
    if (datasets.nonEmpty) {
      import com.paypal.gimel.parser.utilities.QueryParserUtils._
      val storageSystemName = Try(extractSystemFromDatasetName(datasets.head)).toOption
      if (storageSystemName.isDefined &&
        CatalogProvider.getStorageSystemProperties(
          storageSystemName.get
        )(GimelConstants.STORAGE_TYPE) == GimelConstants.STORAGE_TYPE_JDBC) {
        areAllDatasetFromSameJdbcSystem = datasets.forall {
          dataset =>
            Try {
              val storageSystemProperties =
                CatalogProvider.getStorageSystemProperties(extractSystemFromDatasetName(dataset))
              storageSystemProperties(GimelConstants.STORAGE_TYPE) == GimelConstants
                .STORAGE_TYPE_JDBC && dataset.contains(storageSystemName.get)
            }.getOrElse(false)
        }
      }
    }
    areAllDatasetFromSameJdbcSystem
  }

  /**
    * Returns the flag whether the query has to be pushed down to dataset or not based on dataset provided
    * and user supplied flag for pushdown, this method is primarily called for select only clause
    *
    * @param originalSQL  SQLString
    * @param selectSQL    SQLString
    * @param sparkSession : SparkSession
    * @param dataSet      Dataset Object
    * @return String flag to show whether to push down query or not
    */
  def getQueryPushDownFlag(originalSQL: String, selectSQL: String, sparkSession: SparkSession,
                           dataSet: com.paypal.gimel.DataSet): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val tables = getTablesFrom(selectSQL)
    val userSuppliedPushDownFlag: Boolean = getQueryPushDownFlagFromConf(sparkSession)

    var queryPushDownFlag: Boolean = false
    if (userSuppliedPushDownFlag && tables.nonEmpty) {
      val (queryPushDownFlagR, jdbcOptions) = validateAllTablesAreFromSameJdbcSystem(sparkSession, tables, selectSQL)
      if (queryPushDownFlagR) {
        // if all the tables are from the same JDBC system then set query pushdown flag to be true
        queryPushDownFlag = queryPushDownFlagR
        logger.info(s"Since all the datasets are from same JDBC system overriding " +
          s"User specified flag: $userSuppliedPushDownFlag -> true " +
          s"with JDBC options: $jdbcOptions")
      } else {
        logger.info(s"Atleast one dataset is from an alternate JDBC system overriding " +
          s"User specified flag: $userSuppliedPushDownFlag -> false")
      }
    }

    logger.info(s"queryPushDownFlag for data sets${ArrayUtils.toString(tables)}:" +
      s" ${queryPushDownFlag.toString}")
    queryPushDownFlag.toString
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

  /**
    * Checks the config to see if complete pushdown enabled,
    * if enabled returns the transformed SQL and the JDBC options
    *
    * @param sparkSession -> Created SparkSession
    * @param sql -> Incoming SQL to be executed
    * @return
    */
  def isJdbcCompletePushDownEnabled(sparkSession: SparkSession,
                                    sql: String): (Boolean, Option[String], Option[Map[String, String]]) = {
    logger.info(s"@Begin --> ${new Exception().getStackTrace.apply(1).getMethodName}")
    val userSuppliedPushDownFlag: Boolean = getQueryPushDownFlagFromConf(sparkSession)
    val isSelectQuery = QueryParserUtils.isSelectQuery(sql)
    logger.info(s"Is select query: $isSelectQuery")
    var resultTuple: (Boolean, Option[String], Option[Map[String, String]]) = (false, None, None)
    if (userSuppliedPushDownFlag && !isSelectQuery) {
      val tables = getAllTableSources(sql)
      //      val datasets = SQLDataTypesUtils.getDatasets(sql)
      logger.info(s"Received tables: $tables for the query: $sql")
      // if sql's target tables are of the same JDBC system
      if (validateAllTablesAreFromSameJdbcSystem(sparkSession, tables, sqlToBeExecuted = sql)._1) {
        logger.info("All datasets are from the same JDBC system")
        // As tables emptiness is checked on the validateAllDatasetsAreFromSameJdbcSystem, getting the tables.head

        val transformedSQL = QueryParserUtils.transformUdcSQLtoJdbcSQL(sql, tables)
        import com.paypal.gimel.common.utilities.DataSetUtils._
        val systemName = QueryParserUtils.extractSystemFromDatasetName(tables.head)
        resultTuple = (true, Some(transformedSQL), Some(getJdbcConnectionOptions(systemName, sparkSession.conf.getAll)))
      } else {
        logger.info("Not all the datasets are from the same JDBC system")
      }
    } else if (userSuppliedPushDownFlag && isSelectQuery) {
      // Set partitioning to be 1
      // sparkSession.conf.set(JdbcConfigs.jdbcCompletePushdownSelectEnabled, value = true)
      logger.info(s"As we received a select query with pushdown flag enabled: $userSuppliedPushDownFlag," +
        s" we redirect the output to dataset reader -> Query: $sql")
    }
    resultTuple
  }

  private def getQueryPushDownFlagFromConf(sparkSession: SparkSession): Boolean = {
    // User supplied push down flag  will be overridden if all the datasets are from the same JDBC system
    val userSuppliedPushDownFlag = Try(
      sparkSession.conf.get(JdbcConfigs.jdbcPushDownEnabled, "true").toBoolean
    ).getOrElse(true)
    logger.info(s"User specified pushdown flag: $userSuppliedPushDownFlag")
    userSuppliedPushDownFlag
  }

  /**
    * Utility for executing push down queries on the respective JDBC system, based on the incoming dataset's property
    *
    * @param sparkSession
    * @param sql
    * @param jdbcOptions
    * @return
    */
  def pushDownQueryAndReturnResult(sparkSession: SparkSession,
                                   sql: String,
                                   jdbcOptions: Map[String, String]): String = {
    val jdbcConnectionUtility: JDBCConnectionUtility = validateAndGetJdbcConnectionUtility(sparkSession, jdbcOptions)
    val functionName = s"[QueryHash: ${sql.hashCode}]"
    logger.info(s"Proceeding to execute JDBC[System: ${jdbcConnectionUtility.jdbcSystem}," +
      s" User: ${jdbcConnectionUtility.jdbcUser}] pushdown query$functionName: $sql")
    GenericUtils.time(functionName, Some(logger)) {
      val queryResult: String =
        JDBCConnectionUtility.withResources(
          JDBCUtilities.getOrCreateConnection(jdbcConnectionUtility, logger = Some(logger))
        ) {
          connection => JdbcAuxiliaryUtilities.executeQueryAndReturnResultString(sql, connection)
        }
      queryResult
    }
  }

  private def validateAndGetJdbcConnectionUtility(sparkSession: SparkSession,
                                                  jdbcOptions: Map[String, String]): JDBCConnectionUtility = {
    logger.info(s" @Begin --> ${new Exception().getStackTrace.apply(1).getMethodName}")
    logger.info(s"Received JDBC options: $jdbcOptions")
    if (!jdbcOptions.contains(JdbcConfigs.jdbcUrl)) {
      throw new IllegalArgumentException("No JDBC url found. Please verify the dataset name in query")
    }

    JDBCConnectionUtility(sparkSession, jdbcOptions)
  }

  def createPushDownQueryDataframe(sparkSession: SparkSession,
                                   sql: String,
                                   jdbcOptions: Map[String, String]): DataFrame = {
    val jdbcConnectionUtility: JDBCConnectionUtility = validateAndGetJdbcConnectionUtility(sparkSession, jdbcOptions)
    val pushDownJdbcRDD =
      new PushDownJdbcRDD(sparkSession.sparkContext, new DbConnection(jdbcConnectionUtility), sql)
    sparkSession.createDataFrame(pushDownJdbcRDD, JdbcConstants.DEF_JDBC_PUSH_DOWN_SCHEMA)
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
