-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements. See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License. You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE DATABASE IF NOT EXISTS pcatalog;

drop table if exists pcatalog.flights_lookup_cancellation_code_hbase;

CREATE EXTERNAL TABLE `pcatalog.flights_lookup_cancellation_code_hbase`(
  `Code` string,
  `Description` string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
  "gimel.hbase.rowkey"="Code",
  "gimel.hbase.table.name"="flights:flights_lookup_cancellation_code",
  "gimel.hbase.namespace.name"="flights",
  "gimel.hbase.columns.mapping"=":key,flights:Description",
  'gimel.storage.type'='HBASE');

drop table if exists pcatalog.flights_lookup_carrier_code_hbase;

CREATE EXTERNAL TABLE `pcatalog.flights_lookup_carrier_code_hbase`(
  `code` string,
  `description` string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
  "gimel.hbase.rowkey"="Code",
  "gimel.hbase.table.name"="flights:lights_lookup_carrier_code",
  "gimel.hbase.namespace.name"="flights",
  "gimel.hbase.columns.mapping"=":key,flights:Description",
  'gimel.storage.type'='HBASE');

drop table if exists pcatalog.flights_lookup_airline_id_hbase;

CREATE EXTERNAL TABLE `pcatalog.flights_lookup_airline_id_hbase`(
  `code` string,
  `description` string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
  "gimel.hbase.rowkey"="Code",
  "gimel.hbase.table.name"="flights:flights_lookup_airline_id",
  "gimel.hbase.namespace.name"="flights",
  "gimel.hbase.columns.mapping"=":key,flights:Description",
  'gimel.storage.type'='HBASE');
