CREATE DATABASE IF NOT EXISTS pcatalog;

drop table pcatalog.flights_lookup_carrier_code_hdfs;

CREATE external TABLE if not exists pcatalog.flights_lookup_carrier_code_hdfs(
  payload string)
LOCATION 'hdfs://namenode:8020/flights/lkp/carrier_code'
TBLPROPERTIES (
  'gimel.storage.type'='HDFS',
  'gimel.hdfs.data.format'='csv'
);

drop table pcatalog.flights_lookup_airline_id_hdfs;

CREATE external TABLE if not exists pcatalog.flights_lookup_airline_id_hdfs(
  payload string)
LOCATION 'hdfs://namenode:8020/flights/lkp/airline_id'
TBLPROPERTIES (
  'gimel.storage.type'='HDFS',
  'gimel.hdfs.data.format'='csv'
);

drop table pcatalog.flights_lookup_cancellation_code_hdfs;

CREATE external TABLE if not exists pcatalog.flights_lookup_cancellation_code_hdfs(
  payload string)
LOCATION 'hdfs://namenode:8020/flights/lkp/cancellation_code'
TBLPROPERTIES (
  'gimel.storage.type'='HDFS',
  'gimel.hdfs.data.format'='csv'
);

drop table pcatalog.flights_lookup_airports_hdfs;

CREATE external TABLE if not exists pcatalog.flights_lookup_airports_hdfs(
  payload string)
LOCATION 'hdfs://namenode:8020/flights/lkp/airports'
TBLPROPERTIES (
  'gimel.storage.type'='HDFS',
  'gimel.hdfs.data.format'='csv'
);

drop table pcatalog.flights_hdfs;
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