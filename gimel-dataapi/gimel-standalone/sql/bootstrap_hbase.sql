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