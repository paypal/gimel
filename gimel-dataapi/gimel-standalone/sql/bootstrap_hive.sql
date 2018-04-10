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

drop table if exists pcatalog.flights_lookup_carrier_code_hdfs;

CREATE external TABLE if not exists pcatalog.flights_lookup_carrier_code_hdfs(
  payload string)
LOCATION 'hdfs://namenode:8020/flights/lkp/carrier_code'
TBLPROPERTIES (
  'gimel.storage.type'='HDFS',
  'gimel.hdfs.data.format'='csv'
);

drop table if exists pcatalog.flights_lookup_airline_id_hdfs;

CREATE external TABLE if not exists pcatalog.flights_lookup_airline_id_hdfs(
  payload string)
LOCATION 'hdfs://namenode:8020/flights/lkp/airline_id'
TBLPROPERTIES (
  'gimel.storage.type'='HDFS',
  'gimel.hdfs.data.format'='csv'
);

drop table if exists pcatalog.flights_lookup_cancellation_code_hdfs;

CREATE external TABLE if not exists pcatalog.flights_lookup_cancellation_code_hdfs(
  payload string)
LOCATION 'hdfs://namenode:8020/flights/lkp/cancellation_code'
TBLPROPERTIES (
  'gimel.storage.type'='HDFS',
  'gimel.hdfs.data.format'='csv'
);

drop table if exists pcatalog.flights_lookup_airports_hdfs;

CREATE external TABLE if not exists pcatalog.flights_lookup_airports_hdfs(
  payload string)
LOCATION 'hdfs://namenode:8020/flights/lkp/airports'
TBLPROPERTIES (
  'gimel.storage.type'='HDFS',
  'gimel.hdfs.data.format'='csv'
);

drop table if exists pcatalog.flights_hdfs;
CREATE external TABLE if not exists pcatalog.flights_hdfs(
  payload string)
PARTITIONED BY (
year string,
month string
)
LOCATION 'hdfs://namenode:8020/flights/data'
TBLPROPERTIES (
  'gimel.storage.type'='HDFS',
  'gimel.hdfs.data.format'='csv'
);

drop table if exists pcatalog.flights_hdfs_2017_10;

CREATE external TABLE if not exists pcatalog.flights_hdfs_2017_10(
  payload string)
PARTITIONED BY (
year string,
month string
)
LOCATION 'hdfs://namenode:8020/flights/data/year'
TBLPROPERTIES (
  'gimel.storage.type'='HDFS',
  'gimel.hdfs.data.format'='csv'
);
