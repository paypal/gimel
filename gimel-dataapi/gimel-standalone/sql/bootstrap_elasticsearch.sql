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

drop table if exists pcatalog.gimel_flights_elastic;

CREATE EXTERNAL TABLE `pcatalog.gimel_flights_elastic`(
`payload` string
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES(
"es.mapping.date.rich"="true",
  "es.nodes"="http://elasticsearch",
  "es.port"="9200",
  "es.resource"="flights/data",
  "es.index.auto.create"="true",
  "gimel.es.schema.mapping"="{\"location\": { \"type\": \"geo_point\" } }",
  "gimel.es.index.partition.delimiter"="-",
  "gimel.es.index.partition.isEnabled"="true",
  "gimel.es.index.read.all.partitions.isEnabled"="true",
  "gimel.es.index.partition.suffix"="20180205",
  "gimel.es.schema.mapping"="{\"executionStartTime\": {\"format\": \"strict_date_optional_time||epoch_millis\", \"type\": \"date\" }, \"createdTime\": {\"format\": \"strict_date_optional_time||epoch_millis\", \"type\": \"date\"},\"endTime\": {\"format\": \"strict_date_optional_time||epoch_millis\", \"type\": \"date\"}}",
  "gimel.storage.type"="ELASTIC_SEARCH");
