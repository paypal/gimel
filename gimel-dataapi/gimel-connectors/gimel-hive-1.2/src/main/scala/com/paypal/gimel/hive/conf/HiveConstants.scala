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

package com.paypal.gimel.hive.conf

object HiveConstants {
  val sqlPollingKind = "sql"
  val hivePollingKind = "hive"
  val defaultPollingKind = sqlPollingKind
  val defaultPollingKindKey = "defaultPollingKind"
  val driver = "com.mysql.jdbc.Driver"
  val groupConcatLength = "4294967295"
  val hivePollingQueryForAllDbs = "select DBS.NAME DB_NAME,TBLS.TBL_NAME TBL_NAME, TBLS.TBL_TYPE TBL_TYPE, FROM_UNIXTIME(TBLS.CREATE_TIME) CREATE_TIME, " +
    "TBLS.OWNER OWNER, SDS_TEMP.INPUT_FORMAT INPUT_FORMAT, SDS_TEMP.OUTPUT_FORMAT OUTPUT_FORMAT, SDS_TEMP.LOCATION LOCATION, " +
    "CONCAT(\"[\",GROUP_CONCAT(CONCAT('{\"columnName\":\"', COLUMNS_V2.COLUMN_NAME,'\", \"columnFamily\":\"NA', '\", \"columnType\":\"',COLUMNS_V2.TYPE_NAME,'\"}')),\"]\") TBL_SCHEMA " +
    "from DBS join TBLS on DBS.DB_ID = TBLS.DB_ID join COLUMNS_V2 on TBLS.TBL_ID = COLUMNS_V2.CD_ID join " +
    "(select * from SDS where SD_ID in (select min(SDS.SD_ID) as SD_ID from SDS where SDS.CD_ID in (select TBLS.TBL_ID from TBLS) group by SDS.CD_ID)) SDS_TEMP " +
    "on TBLS.TBL_ID = SDS_TEMP.CD_ID "
  val groupByClause = "group by DBS.NAME,TBLS.TBL_NAME, TBLS.TBL_TYPE, TBLS.CREATE_TIME, TBLS.OWNER,SDS_TEMP.INPUT_FORMAT, SDS_TEMP.OUTPUT_FORMAT, SDS_TEMP.LOCATION"
  val groupConcatQuery = s"SET SESSION group_concat_max_len=${groupConcatLength}"
  val dbName = "DB_NAME"
  val tblName = "TBL_NAME"
  val tblSchema = "TBL_SCHEMA"
  val inputFormat = "INPUT_FORMAT"
  val location = "LOCATION"
  val hiveSchemaMap = Map("org.apache.hadoop.mapred.SequenceFileInputFormat" -> "sequence",
    "org.apache.hadoop.mapred.TextInputFormat" -> "text",
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" -> "parquet",
    "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat" -> "orc",
    "com.teradata.querygrid.qgc.hive.QGRemoteInputFormat" -> "querygrid")
}
