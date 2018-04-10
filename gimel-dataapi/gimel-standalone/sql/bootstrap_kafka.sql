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

drop table if exists pcatalog.flights_kafka_json;

CREATE EXTERNAL TABLE `pcatalog.flights_kafka_json`(
  `payload` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://namenode:8020/tmp/flights'
TBLPROPERTIES (
  'auto.offset.reset'='earliest',
  'bootstrap.servers'='kafka:9092',
  'gimel.kafka.checkpoint.zookeeper.host'='zookeeper:2181',
  'gimel.kafka.checkpoint.zookeeper.path'='/pcatalog/kafka_consumer/checkpoint/flights_hive',
  'gimel.kafka.message.value.type'='json',
  'gimel.kafka.throttle.batch.fetchRowsOnFirstRun'='10000000',
  'gimel.kafka.throttle.batch.maxRecordsPerPartition'='10000000',
  'gimel.kafka.whitelist.topics'='gimel.demo.flights.json',
  'gimel.kafka.zookeeper.connection.timeout.ms'='10000',
  'gimel.storage.type'='kafka',
  'key.deserializer'='org.apache.kafka.common.serialization.StringDeserializer',
  'key.serializer'='org.apache.kafka.common.serialization.StringSerializer',
  'numFiles'='0',
  'totalSize'='0',
  'transient_lastDdlTime'='1521993577',
  'value.deserializer'='org.apache.kafka.common.serialization.StringDeserializer',
  'value.serializer'='org.apache.kafka.common.serialization.StringSerializer');
