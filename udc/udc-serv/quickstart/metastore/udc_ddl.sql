# ************************************************************
# Sequel Pro SQL dump
# Version 4541
#
# http://www.sequelpro.com/
# https://github.com/sequelpro/sequelpro
#
# Host: 10.176.63.206 (MySQL 5.7.19)
# Database: udc
# Generation Time: 2018-10-02 06:32:14 +0000
# ************************************************************


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table pc_object_schema_map
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_object_schema_map`;

CREATE TABLE `pc_object_schema_map` (
  `object_id` int(11) NOT NULL AUTO_INCREMENT,
  `object_name` varchar(500) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT '',
  `container_name` varchar(500) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT '',
  `storage_system_id` int(11) NOT NULL,
  `object_schema` json DEFAULT NULL,
  `is_self_discovered` enum('Y','N') NOT NULL DEFAULT 'N',
  `is_registered` enum('Y','N','P') NOT NULL,
  `is_active_y_n` enum('Y','N') NOT NULL DEFAULT 'Y',
  `query` text,
  `created_timestamp_on_store` varchar(100) NOT NULL,
  `created_user_on_store` varchar(100) NOT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`object_id`),
  UNIQUE KEY `uniq_idx` (`object_name`,`container_name`,`storage_system_id`),
  KEY `system_fk_idx` (`storage_system_id`),
  CONSTRAINT `system_fk` FOREIGN KEY (`storage_system_id`) REFERENCES `pc_storage_system` (`storage_system_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

LOCK TABLES `pc_object_schema_map` WRITE;
/*!40000 ALTER TABLE `pc_object_schema_map` DISABLE KEYS */;

INSERT INTO `pc_object_schema_map` (`object_id`, `object_name`, `container_name`, `storage_system_id`, `object_schema`, `is_self_discovered`, `is_registered`, `is_active_y_n`, `query`, `created_timestamp_on_store`, `created_user_on_store`, `cre_user`, `cre_ts`, `upd_user`, `upd_ts`)
VALUES
	(39,'flights_log1234','flights',69,'[{"columnName": "payload", "columnType": "dynamic", "columnClass": "", "columnIndex": 0, "partitionStatus": false, "restrictionStatus": false}]','Y','Y','Y','','','gimeldev','gimeladmin','2018-03-26 14:28:29','gimeladmin','2018-08-20 14:33:55'),
	(43,'flights_data12','flights',69,'[{"columnName": "payload", "columnType": "dynamic", "columnFamily": "NA"}]','Y','N','Y','','2018-08-16 17:03:09','gimeldev','gimeladmin','2018-03-26 15:20:05','gimeladmin','2018-06-23 16:55:49'),
	(44,'flights_enriched','flights',68,'[{"columnName": "payload", "columnType": "dynamic"}]','Y','N','Y','','2018-08-16 17:03:09','gimeldev','gimeladmin','2018-03-26 15:41:22','gimeladmin','2018-06-27 14:41:50'),
	(45,'flights_lkp_airport_details','flights_db',68,'[{"columnName": "payload", "columnType": "dynamic"}]','Y','N','Y','','2018-08-16 17:03:09','gimeldev','gimeladmin','2018-03-26 15:56:55','gimeladmin','2018-06-23 15:21:59'),
	(46,'flights_lkp_airport_details1','flights_db',70,'[{"columnName": "payload", "columnType": "dynamic"}]','Y','N','Y','','2018-08-16 17:03:09','gimeldev','gimeladmin','2018-03-26 16:36:08','gimeladmin','2018-06-22 13:23:04'),
	(47,'flights_lkp_cancellation_code','flights_db',66,'[{"columnName": "payload", "columnType": "dynamic", "columnFamily": "NA"}]','N','N','Y','','2018-08-16 17:03:09','gimeldev','gimeladmin','2018-03-26 16:49:10','gimeladmin','2018-03-26 16:49:10'),
	(48,'flights_lkp_cancellation','flights_db',66,'[{"columnName": "code", "columnType": "string", "columnClass": "", "columnIndex": 0, "columnFamily": "NA", "partitionStatus": false, "restrictionStatus": false}, {"columnName": "description", "columnType": "string", "columnClass": "", "columnIndex": 0, "columnFamily": "NA", "partitionStatus": false, "restrictionStatus": false}]','Y','N','Y','','2018-08-16 17:03:09','gimeldev','gimeladmin','2018-03-26 16:50:39','gimeladmin','2018-08-17 11:00:29'),
	(49,'flights_lkp_airline','flights_db',66,'[{"columnName": "code", "columnType": "string", "columnFamily": "NA"}, {"columnName": "description", "columnType": "string", "columnFamily": "NA"}]','N','N','Y',NULL,'2018-08-16 17:03:09','gimeldev','gimeladmin','2018-03-26 16:51:49','gimeladmin','2018-05-01 09:45:58'),
	(50,'flights_lkp_carrier','flights_db',66,'[{"columnName": "code", "columnType": "string", "columnFamily": "NA"}, {"columnName": "description", "columnType": "string", "columnFamily": "NA"}]','Y','N','Y','','2018-08-16 17:03:09','gimeldev','gimeladmin','2018-03-26 16:51:57','gimeladmin','2018-06-27 14:38:21');

/*!40000 ALTER TABLE `pc_object_schema_map` ENABLE KEYS */;
UNLOCK TABLES;


# Dump of table pc_ranger_policy
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_ranger_policy`;

CREATE TABLE `pc_ranger_policy` (
  `derived_policy_id` int(11) NOT NULL AUTO_INCREMENT,
  `policy_id` int(11) NOT NULL,
  `cluster_id` int(11) NOT NULL,
  `policy_name` varchar(1000) CHARACTER SET utf8 NOT NULL DEFAULT '',
  `type_name` varchar(50) CHARACTER SET utf8 NOT NULL DEFAULT '',
  `policy_locations` longtext CHARACTER SET utf8 NOT NULL,
  `is_active_y_n` enum('Y','N') CHARACTER SET utf8 NOT NULL,
  `cre_user` varchar(45) CHARACTER SET utf8 DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) CHARACTER SET utf8 DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`derived_policy_id`),
  UNIQUE KEY `policy_name_id_cluster_UNIQUE` (`policy_id`,`cluster_id`,`policy_name`),
  KEY `cluster_id_fk` (`cluster_id`),
  CONSTRAINT `cluster_id_fk` FOREIGN KEY (`cluster_id`) REFERENCES `pc_storage_clusters` (`cluster_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



# Dump of table pc_ranger_policy_user_group
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_ranger_policy_user_group`;

CREATE TABLE `pc_ranger_policy_user_group` (
  `derived_policy_user_group_id` int(11) NOT NULL AUTO_INCREMENT,
  `derived_policy_id` int(11) NOT NULL,
  `access_types` longtext NOT NULL,
  `users` mediumtext NOT NULL,
  `groups` mediumtext NOT NULL,
  `is_active_y_n` enum('Y','N') NOT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`derived_policy_user_group_id`),
  KEY `derived_policy_id_fk` (`derived_policy_id`),
  CONSTRAINT `derived_policy_id_fk` FOREIGN KEY (`derived_policy_id`) REFERENCES `pc_ranger_policy` (`derived_policy_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table pc_storage
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_storage`;

CREATE TABLE `pc_storage` (
  `storage_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_name` varchar(45) CHARACTER SET utf8 DEFAULT NULL,
  `storage_desc` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
  `is_active_y_n` enum('Y','N') COLLATE utf8_unicode_ci NOT NULL,
  `cre_user` varchar(45) CHARACTER SET utf8 DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`storage_id`),
  UNIQUE KEY `storage_name_UNIQUE` (`storage_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

LOCK TABLES `pc_storage` WRITE;
/*!40000 ALTER TABLE `pc_storage` DISABLE KEYS */;

INSERT INTO `pc_storage` (`storage_id`, `storage_name`, `storage_desc`, `is_active_y_n`, `cre_user`, `cre_ts`, `upd_user`, `upd_ts`)
VALUES
	(2,'Nosql','Nosql Databases - 1','Y','gimeladmin','2017-03-27 19:42:51','gimeladmin','2018-10-01 22:36:05'),
	(3,'Document','Document based Datastores','Y','gimeladmin','2017-03-27 19:42:51','gimeladmin','2018-10-01 22:36:05'),
	(5,'Streaming','Streaming Platform Storage','Y','gimeladmin','2017-03-27 19:42:51','gimeladmin','2018-10-01 22:36:05'),
	(6,'RDBMS','Relational Databases','Y','gimeladmin','2017-03-27 19:42:51','gimeladmin','2018-10-01 22:36:05'),
	(7,'InMemory','Memory Cache Systems','Y','gimeladmin','2018-02-21 14:22:48','gimeladmin','2018-10-01 22:36:05');

/*!40000 ALTER TABLE `pc_storage` ENABLE KEYS */;
UNLOCK TABLES;


# Dump of table pc_storage_clusters
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_storage_clusters`;

CREATE TABLE `pc_storage_clusters` (
  `cluster_id` int(11) NOT NULL AUTO_INCREMENT,
  `cluster_name` varchar(45) NOT NULL,
  `cluster_description` varchar(1000) DEFAULT NULL,
  `livy_end_point` varchar(100) NOT NULL DEFAULT '',
  `livy_port` int(11) NOT NULL,
  `is_active_y_n` enum('Y','N') NOT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`cluster_id`),
  UNIQUE KEY `cluster_name_UNIQUE` (`cluster_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

LOCK TABLES `pc_storage_clusters` WRITE;
/*!40000 ALTER TABLE `pc_storage_clusters` DISABLE KEYS */;

INSERT INTO `pc_storage_clusters` (`cluster_id`, `cluster_name`, `cluster_description`, `livy_end_point`, `livy_port`, `is_active_y_n`, `cre_user`, `cre_ts`, `upd_user`, `upd_ts`)
VALUES
	(4,'Cluster10','Cluster10 is a POC Cluster - Make / Break at will','a.b.c.d',8989,'Y','gimeladmin','2017-08-01 11:43:37','gimeladmin','2018-10-01 22:34:39'),
	(5,'Cluster1','Cluster1 is a production cluster','e.f.g.h',8989,'Y','gimeladmin','2017-08-01 11:43:37','gimeladmin','2018-10-01 22:34:47'),
	(6,'Cluster2','Cluster2 is a production cluster for XYZ','i.j.k.l',8989,'Y','gimeladmin','2017-08-01 11:43:37','gimeladmin','2018-10-01 22:34:56');

/*!40000 ALTER TABLE `pc_storage_clusters` ENABLE KEYS */;
UNLOCK TABLES;


# Dump of table pc_storage_dataset
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_storage_dataset`;

CREATE TABLE `pc_storage_dataset` (
  `storage_dataset_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_dataset_name` varchar(200) CHARACTER SET utf8 NOT NULL,
  `storage_dataset_alias_name` varchar(500) CHARACTER SET utf8 NOT NULL,
  `storage_database_name` varchar(500) CHARACTER SET utf8 DEFAULT NULL,
  `storage_dataset_desc` varchar(500) CHARACTER SET utf8 NOT NULL,
  `storage_system_id` int(11) DEFAULT NULL,
  `is_active_y_n` enum('Y','N') CHARACTER SET utf8 DEFAULT NULL,
  `object_schema_map_id` int(11) NOT NULL,
  `is_auto_registered` enum('Y','N') CHARACTER SET utf8 NOT NULL,
  `user_id` int(11) NOT NULL,
  `cre_user` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`storage_dataset_id`),
  UNIQUE KEY `storage_dataset_uniq_cnst` (`storage_dataset_name`,`storage_dataset_alias_name`,`object_schema_map_id`,`is_auto_registered`),
  KEY `dataset_user_fk_idx` (`user_id`),
  KEY `dataset_object_fk_idx` (`object_schema_map_id`),
  CONSTRAINT `dataset_object_fk` FOREIGN KEY (`object_schema_map_id`) REFERENCES `pc_object_schema_map` (`object_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `dataset_user_fk` FOREIGN KEY (`user_id`) REFERENCES `pc_users` (`user_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

LOCK TABLES `pc_storage_dataset` WRITE;
/*!40000 ALTER TABLE `pc_storage_dataset` DISABLE KEYS */;

INSERT INTO `pc_storage_dataset` (`storage_dataset_id`, `storage_dataset_name`, `storage_dataset_alias_name`, `storage_database_name`, `storage_dataset_desc`, `storage_system_id`, `is_active_y_n`, `object_schema_map_id`, `is_auto_registered`, `user_id`, `cre_user`, `cre_ts`, `upd_user`, `upd_ts`)
VALUES
	(883,'flights_log_test','flights_log_test','udc','flights_log',69,'Y',39,'Y',22,'gimeldev','2018-03-26 14:37:09','gimeldev','2018-10-01 22:42:21'),
	(885,'flights_cancelled','flights_cancelled','udc','Elastic Search Data with Enriched Details on Cancelled flights',68,'Y',45,'N',22,'gimeldev','2018-03-26 15:58:19','gimeldev','2018-10-01 22:42:21'),
	(886,'flights_lkp_airport','flights_lkp_airport','udc','Airport Details',70,'Y',46,'N',22,'gimeldev','2018-03-26 16:37:03','gimeldev','2018-10-01 22:42:21'),
	(887,'flights_lookup_airline_id_TD','flights_lookup_airline_id_TD','udc','Airline Lookup',66,'Y',49,'N',22,'gimeldev','2018-03-26 16:53:41','gimeldev','2018-10-01 22:42:21'),
	(888,'flights_lookup_cancellation_code_TD4','flights_lookup_cancellation_code_TD4','udc','flights_lookup_cancellation_code_TD4',66,'Y',48,'N',22,'gimeldev','2018-03-26 16:54:30','gimeldev','2018-10-01 22:42:21'),
	(889,'flights_lookup_carrier_code_td_2','flights_lookup_carrier_code_td','udc','Lookup Carrier',66,'Y',50,'N',22,'gimeldev','2018-03-26 16:55:12','gimeldev','2018-10-01 22:42:21'),
	(890,'flights_dataset123','flights_dataset','udc','Demo',69,'Y',39,'N',22,'gimeldev','2018-03-27 13:38:22','gimeldev','2018-10-01 22:42:21'),
	(892,'flights_123','flights_123','udc','Demo',69,'Y',39,'N',22,'gimeldev','2018-03-28 19:07:57','gimeldev','2018-10-01 22:42:21');

/*!40000 ALTER TABLE `pc_storage_dataset` ENABLE KEYS */;
UNLOCK TABLES;


# Dump of table pc_storage_dataset_change_log_registered
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_storage_dataset_change_log_registered`;

CREATE TABLE `pc_storage_dataset_change_log_registered` (
  `storage_dataset_change_log_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_dataset_id` int(11) NOT NULL,
  `storage_dataset_name` varchar(200) CHARACTER SET utf8 NOT NULL,
  `storage_dataset_alias_name` varchar(10000) COLLATE utf8_unicode_ci DEFAULT NULL,
  `storage_container_name` varchar(1000) COLLATE utf8_unicode_ci DEFAULT NULL,
  `storage_database_name` varchar(500) CHARACTER SET utf8 DEFAULT NULL,
  `storage_dataset_desc` varchar(500) CHARACTER SET utf8 DEFAULT NULL,
  `storage_system_id` int(11) NOT NULL,
  `storage_cluster_id` int(11) DEFAULT NULL,
  `user_id` int(11) NOT NULL,
  `storage_dataset_query` longtext COLLATE utf8_unicode_ci,
  `storage_dataset_schema` text CHARACTER SET utf8,
  `storage_deployment_status` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
  `storage_dataset_change_type` enum('C','M','D') CHARACTER SET utf8 DEFAULT NULL,
  `cre_user` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`storage_dataset_change_log_id`),
  KEY `data_set_change_log_fk_idx` (`storage_dataset_id`),
  KEY `cluster_change_log_fk_idx` (`storage_cluster_id`),
  KEY `user_change_log_fk_idx` (`user_id`),
  KEY `system_dataset_change_log_fk_idx` (`storage_system_id`),
  CONSTRAINT `cluster_change_log_fk` FOREIGN KEY (`storage_cluster_id`) REFERENCES `pc_storage_clusters` (`cluster_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `data_set_change_log_fk` FOREIGN KEY (`storage_dataset_id`) REFERENCES `pc_storage_dataset` (`storage_dataset_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `system_dataset_change_log_fk` FOREIGN KEY (`storage_system_id`) REFERENCES `pc_storage_system` (`storage_system_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `user_change_log_fk` FOREIGN KEY (`user_id`) REFERENCES `pc_users` (`user_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



# Dump of table pc_storage_dataset_system
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_storage_dataset_system`;

CREATE TABLE `pc_storage_dataset_system` (
  `storage_system_dataset_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_system_id` int(11) NOT NULL,
  `storage_dataset_id` int(11) NOT NULL,
  `is_active_y_n` enum('Y','N') DEFAULT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`storage_system_dataset_id`),
  KEY `system_id_ws_fk_idx` (`storage_system_id`),
  KEY `dataset_id_ds_fk_idx` (`storage_dataset_id`),
  CONSTRAINT `dataset_id_ds_fk` FOREIGN KEY (`storage_dataset_id`) REFERENCES `pc_storage_dataset` (`storage_dataset_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `system_id_ds_fk` FOREIGN KEY (`storage_system_id`) REFERENCES `pc_storage_system` (`storage_system_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `pc_storage_dataset_system` WRITE;
/*!40000 ALTER TABLE `pc_storage_dataset_system` DISABLE KEYS */;

INSERT INTO `pc_storage_dataset_system` (`storage_system_dataset_id`, `storage_system_id`, `storage_dataset_id`, `is_active_y_n`, `cre_user`, `cre_ts`, `upd_user`, `upd_ts`)
VALUES
	(956,69,883,'Y','gimeladmin','2018-03-26 14:37:09','gimeladmin','2018-10-01 22:55:00'),
	(958,68,885,'Y','gimeladmin','2018-03-26 15:58:19','gimeladmin','2018-10-01 22:55:00'),
	(959,70,886,'Y','gimeladmin','2018-03-26 16:37:03','gimeladmin','2018-10-01 22:55:00'),
	(960,66,887,'Y','gimeladmin','2018-03-26 16:53:41','gimeladmin','2018-10-01 22:55:00'),
	(961,66,888,'Y','gimeladmin','2018-03-26 16:54:30','gimeladmin','2018-10-01 22:55:00'),
	(962,66,889,'Y','gimeladmin','2018-03-26 16:55:12','gimeladmin','2018-10-01 22:55:00'),
	(963,69,890,'Y','gimeladmin','2018-03-27 13:38:22','gimeladmin','2018-10-01 22:55:00'),
	(965,69,892,'Y','gimeladmin','2018-03-28 19:07:57','gimeladmin','2018-10-01 22:55:00');

/*!40000 ALTER TABLE `pc_storage_dataset_system` ENABLE KEYS */;
UNLOCK TABLES;


# Dump of table pc_storage_object_attribute_value
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_storage_object_attribute_value`;

CREATE TABLE `pc_storage_object_attribute_value` (
  `object_attribute_value_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_ds_attribute_key_id` int(11) NOT NULL,
  `object_id` int(11) NOT NULL,
  `object_attribute_value` longtext NOT NULL,
  `is_customized` enum('Y','N') NOT NULL DEFAULT 'N',
  `is_active_y_n` enum('Y','N') DEFAULT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`object_attribute_value_id`),
  UNIQUE KEY `object_type_value_uniq_idx` (`object_attribute_value`(1000),`storage_ds_attribute_key_id`,`object_id`),
  KEY `type_object_attribute_fk_idx` (`storage_ds_attribute_key_id`),
  KEY `object_attribute_fk` (`object_id`),
  CONSTRAINT `object_attribute_fk` FOREIGN KEY (`object_id`) REFERENCES `pc_object_schema_map` (`object_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `type_object_attribute_fk` FOREIGN KEY (`storage_ds_attribute_key_id`) REFERENCES `pc_storage_type_attribute_key` (`storage_ds_attribute_key_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

LOCK TABLES `pc_storage_object_attribute_value` WRITE;
/*!40000 ALTER TABLE `pc_storage_object_attribute_value` DISABLE KEYS */;

INSERT INTO `pc_storage_object_attribute_value` (`object_attribute_value_id`, `storage_ds_attribute_key_id`, `object_id`, `object_attribute_value`, `is_customized`, `is_active_y_n`, `cre_user`, `cre_ts`, `upd_user`, `upd_ts`)
VALUES
	(18,103,39,'updated_6','N','Y','gimeladmin','2018-03-26 14:28:29','gimeladmin','2018-10-01 22:53:57'),
	(23,78,43,'updated','N','Y','gimeladmin','2018-03-26 15:20:05','gimeladmin','2018-10-01 22:53:57'),
	(24,79,43,'updated again','N','Y','gimeladmin','2018-03-26 15:20:05','gimeladmin','2018-10-01 22:53:57'),
	(25,80,43,'flights.flights_log','N','Y','gimeladmin','2018-03-26 15:20:05','gimeladmin','2018-10-01 22:53:57'),
	(26,82,43,'flights.flights_log','N','Y','gimeladmin','2018-03-26 15:20:05','gimeladmin','2018-10-01 22:53:57'),
	(27,101,43,'org.apache.kafka.common.serialization.StringDeserializer','N','Y','gimeladmin','2018-03-26 15:20:05','gimeladmin','2018-10-01 22:53:57'),
	(28,103,43,'org.apache.kafka.common.serialization.StringDeserializer','N','Y','gimeladmin','2018-03-26 15:20:05','gimeladmin','2018-10-01 22:53:57'),
	(29,63,44,'Delimiter_1','N','Y','gimeladmin','2018-03-26 15:41:22','gimeladmin','2018-10-01 22:53:57'),
	(30,64,44,'N','N','Y','gimeladmin','2018-03-26 15:41:22','gimeladmin','2018-10-01 22:53:57'),
	(31,65,44,'flights/data','N','Y','gimeladmin','2018-03-26 15:41:22','gimeladmin','2018-10-01 22:53:57'),
	(32,63,45,'_','N','Y','gimeladmin','2018-03-26 15:56:55','gimeladmin','2018-10-01 22:53:57'),
	(33,64,45,'N','Y','Y','gimeladmin','2018-03-26 15:56:55','gimeladmin','2018-10-01 22:53:57'),
	(34,65,45,'flights/data','Y','Y','gimeladmin','2018-03-26 15:56:55','gimeladmin','2018-10-01 22:53:57'),
	(35,91,46,'csv','Y','Y','gimeladmin','2018-03-26 16:36:08','gimeladmin','2018-10-01 22:53:57'),
	(36,92,46,'/user/flights/lkp/flights_lkp_airport_details','Y','Y','gimeladmin','2018-03-26 16:36:08','gimeladmin','2018-10-01 22:53:57'),
	(37,86,47,'flights_db.flights_lkp_cancellation_code','N','Y','gimeladmin','2018-03-26 16:49:10','gimeladmin','2018-03-26 16:49:10'),
	(38,86,48,'tables.abcd','Y','Y','gimeladmin','2018-03-26 16:50:39','gimeladmin','2018-10-01 22:53:57'),
	(39,86,49,'flights_db.flights_lkp_airline11','Y','Y','gimeladmin','2018-03-26 16:51:49','gimeladmin','2018-10-01 22:53:57'),
	(40,86,50,'updated_2','Y','Y','gimeladmin','2018-03-26 16:51:57','gimeladmin','2018-10-01 22:53:57'),
	(41,72,43,'/tmp/zookeeper/checkpoint/flights_log','N','Y','gimeladmin','2018-04-23 13:51:54','gimeladmin','2018-10-01 22:53:57'),
	(43,72,39,'checkpoint','N','Y','gimeladmin','2018-05-02 15:53:38','gimeladmin','2018-10-01 22:53:57'),
	(44,78,39,'kryo','N','Y','gimeladmin','2018-05-02 15:53:38','gimeladmin','2018-10-01 22:53:57'),
	(45,79,39,'key','N','Y','gimeladmin','2018-05-02 15:53:38','gimeladmin','2018-10-01 22:53:57'),
	(46,80,39,'hello','N','Y','gimeladmin','2018-05-02 15:53:38','gimeladmin','2018-10-01 22:53:57'),
	(47,82,39,'doctor','Y','Y','gimeladmin','2018-05-02 15:53:38','gimeladmin','2018-10-01 22:53:57'),
	(117,101,39,'heart','N','Y','gimeladmin','2018-07-31 17:49:30','gimeladmin','2018-10-01 22:53:57'),
	(124,123,39,'Defaults','Y','Y','gimeladmin','2018-09-28 18:28:13','gimeladmin','2018-10-01 22:53:57'),
	(125,123,43,'Defaults','Y','Y','gimeladmin','2018-09-28 18:28:13','gimeladmin','2018-10-01 22:53:57');

/*!40000 ALTER TABLE `pc_storage_object_attribute_value` ENABLE KEYS */;
UNLOCK TABLES;


# Dump of table pc_storage_system
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_storage_system`;

CREATE TABLE `pc_storage_system` (
  `storage_system_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_system_name` varchar(1000) CHARACTER SET utf8 NOT NULL DEFAULT '',
  `storage_system_desc` varchar(1000) COLLATE utf8_unicode_ci DEFAULT NULL,
  `storage_type_id` int(11) NOT NULL,
  `running_cluster_id` int(11) NOT NULL,
  `zone_id` int(11) NOT NULL,
  `admin_user` int(11) DEFAULT NULL,
  `is_active_y_n` enum('Y','N') COLLATE utf8_unicode_ci NOT NULL,
  `is_gimel_compatible` enum('Y','N') COLLATE utf8_unicode_ci DEFAULT 'Y',
  `is_read_compatible` enum('Y','N') COLLATE utf8_unicode_ci DEFAULT 'Y',
  `cre_user` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`storage_system_id`),
  UNIQUE KEY `storage_system_name_UNIQUE` (`storage_system_name`),
  KEY `pc_storage_system_ibfk_1_idx` (`storage_type_id`),
  KEY `admin_user_system_fk` (`admin_user`),
  KEY `running_cluster` (`running_cluster_id`),
  KEY `zone_id` (`zone_id`),
  CONSTRAINT `admin_user_system_fk` FOREIGN KEY (`admin_user`) REFERENCES `pc_users` (`user_id`),
  CONSTRAINT `pc_storage_system_ibfk_1` FOREIGN KEY (`zone_id`) REFERENCES `pc_zones` (`zone_id`),
  CONSTRAINT `running_cluster` FOREIGN KEY (`running_cluster_id`) REFERENCES `pc_storage_clusters` (`cluster_id`),
  CONSTRAINT `system_type_fk` FOREIGN KEY (`storage_type_id`) REFERENCES `pc_storage_type` (`storage_type_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

LOCK TABLES `pc_storage_system` WRITE;
/*!40000 ALTER TABLE `pc_storage_system` DISABLE KEYS */;

INSERT INTO `pc_storage_system` (`storage_system_id`, `storage_system_name`, `storage_system_desc`, `storage_type_id`, `running_cluster_id`, `zone_id`, `admin_user`, `is_active_y_n`, `is_gimel_compatible`, `is_read_compatible`, `cre_user`, `cre_ts`, `upd_user`, `upd_ts`)
VALUES
	(64,'Hbase.Cluster10','QA HBase operations',32,5,1,34,'Y','Y','Y','gimeladmin','2018-02-15 16:56:08','gimeladmin','2018-10-01 23:06:30'),
	(65,'Hbase.Cluster1','HBASE Development',32,4,1,34,'Y','Y','Y','gimeladmin','2018-02-15 16:56:42','gimeladmin','2018-10-01 23:06:30'),
	(66,'Teradata.Dev','Development teradata cluster-1',35,4,1,34,'Y','Y','Y','gimeladmin','2018-02-15 16:58:16','gimeladmin','2018-10-01 23:06:30'),
	(67,'Teradata.Prod','Prod teradata cluster',35,4,1,34,'Y','Y','Y','gimeladmin','2018-02-15 17:00:21','gimeladmin','2018-10-01 23:06:30'),
	(68,'Elastic.Cluster-Ops','Elastic Search operational logs',33,4,2,34,'Y','N','N','gimeladmin','2018-02-15 17:02:47','gimeladmin','2018-10-01 23:06:30'),
	(69,'Kafka.Transactional','Transactional Cluster High Speed + Hive Volume Low Rentention ',34,4,1,34,'Y','Y','Y','gimeladmin','2018-02-15 17:06:16','gimeladmin','2018-10-01 23:06:30'),
	(70,'Hive.Prod-1','Production 1 Hive',36,4,1,34,'Y','Y','Y','gimeladmin','2018-02-15 17:07:05','gimeladmin','2018-10-01 23:06:30'),
	(71,'Hive.Prod-2','Production 2 Hive',36,5,1,34,'Y','Y','Y','gimeladmin','2018-02-15 17:07:27','gimeladmin','2018-10-01 23:06:30'),
	(72,'Hive.QA-1','Hadoop-Hive-QA-1',36,6,1,34,'Y','Y','Y','gimeladmin','2018-02-15 17:07:49','gimeladmin','2018-10-01 23:06:30'),
	(73,'Teradata.QA','This is a cluster for Site Integration for Analytics Results',35,4,1,34,'Y','Y','Y','gimeladmin','2018-02-15 17:09:06','gimeladmin','2018-10-01 23:06:30');

/*!40000 ALTER TABLE `pc_storage_system` ENABLE KEYS */;
UNLOCK TABLES;


# Dump of table pc_storage_system_attribute_value
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_storage_system_attribute_value`;

CREATE TABLE `pc_storage_system_attribute_value` (
  `storage_system_attribute_value_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_system_attribute_value` varchar(1000) NOT NULL,
  `storage_ds_attribute_key_id` int(11) DEFAULT NULL,
  `storage_system_id` int(11) DEFAULT NULL,
  `is_active_y_n` enum('Y','N') NOT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`storage_system_attribute_value_id`),
  UNIQUE KEY `storage_system_attribute_unique_idx` (`storage_system_id`,`storage_ds_attribute_key_id`,`storage_system_attribute_value`),
  KEY `pc_system_id_fk_idx` (`storage_system_id`),
  KEY `pc_data_attribute_key_fk_idx` (`storage_ds_attribute_key_id`),
  CONSTRAINT `pc_data_attribute_key_fk` FOREIGN KEY (`storage_ds_attribute_key_id`) REFERENCES `pc_storage_type_attribute_key` (`storage_ds_attribute_key_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `pc_system_id_fk` FOREIGN KEY (`storage_system_id`) REFERENCES `pc_storage_system` (`storage_system_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `pc_storage_system_attribute_value` WRITE;
/*!40000 ALTER TABLE `pc_storage_system_attribute_value` DISABLE KEYS */;

INSERT INTO `pc_storage_system_attribute_value` (`storage_system_attribute_value_id`, `storage_system_attribute_value`, `storage_ds_attribute_key_id`, `storage_system_id`, `is_active_y_n`, `cre_user`, `cre_ts`, `upd_user`, `upd_ts`)
VALUES
	(32,'HBASE',62,64,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(33,'HBASE',62,65,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(34,'JDBC',83,66,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(35,'jdbc:teradata://prodteradata.com',84,66,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(36,'com.teradata.jdbc.TeraDriver',85,66,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(37,'JDBC',83,67,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(38,'jdbc:teradata://testteradata.com',84,67,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(39,'com.teradata.jdbc.TeraDriver',85,67,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(40,'8080',66,68,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(41,'gcp.host.11,gcp.host.12,gcp.host.13',67,68,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(42,'ELASTIC_SEARCH',68,68,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(43,'KAFKA',69,69,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(44,'earliest',70,69,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(46,'gcp.host.1:2181,gcp.host.2:2181,gcp.host.3:2181,gcp.host.4:2181,gcp.host.5:2181',73,69,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(47,'http://pp-schema-reg:8081',74,69,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(48,'master.schema',75,69,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(49,'NA',76,69,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(50,'10000',77,69,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(51,'broker1:9092,broker2:9092,broker3:9092',81,69,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(52,'JDBC',83,73,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(53,'jdbc:teradata://prod2teradata.com',84,73,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(54,'com.teradata.jdbc.TeraDriver',85,73,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(59,'hdfs://devcluster.com:8020/',88,72,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(60,'devcluster',89,72,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(61,'HDFS',90,72,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(62,'abcd.com',87,72,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(63,'metastore',93,72,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(64,'3116',94,72,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(65,'Defaults',87,70,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(66,'hadoop.cluster.1.nn',88,70,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(67,'Defaults',89,70,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(68,'HIVE',90,70,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(69,'Defaults',93,70,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(70,'Defaults',94,70,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(71,'Defaults',87,71,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(72,'Defaults',88,71,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(73,'Defaults',89,71,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(74,'Defaults',90,71,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(75,'Defaults',93,71,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34'),
	(76,'Defaults',94,71,'Y','gimeladmin',NULL,'gimeladmin','2018-10-01 22:52:34');

/*!40000 ALTER TABLE `pc_storage_system_attribute_value` ENABLE KEYS */;
UNLOCK TABLES;


# Dump of table pc_storage_system_container
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_storage_system_container`;

CREATE TABLE `pc_storage_system_container` (
  `storage_system_container_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_system_id` int(11) NOT NULL,
  `container_name` varchar(45) NOT NULL,
  `cluster_id` int(11) NOT NULL,
  `is_active_y_n` enum('Y','N') NOT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`storage_system_container_id`),
  KEY `storage_system_fk_idx` (`storage_system_id`),
  KEY `cluster_system_fk` (`cluster_id`),
  CONSTRAINT `cluster_system_fk` FOREIGN KEY (`cluster_id`) REFERENCES `pc_storage_clusters` (`cluster_id`),
  CONSTRAINT `storage_system_fk` FOREIGN KEY (`storage_system_id`) REFERENCES `pc_storage_system` (`storage_system_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table pc_storage_type
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_storage_type`;

CREATE TABLE `pc_storage_type` (
  `storage_type_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_type_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
  `storage_type_desc` varchar(500) COLLATE utf8_unicode_ci DEFAULT NULL,
  `storage_id` int(11) NOT NULL,
  `is_active_y_n` enum('Y','N') CHARACTER SET utf8 NOT NULL,
  `cre_user` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`storage_type_id`),
  UNIQUE KEY `storage_type_name_UNIQUE` (`storage_type_name`),
  KEY `storage_id` (`storage_id`),
  CONSTRAINT `pc_storage_type_ibfk_1` FOREIGN KEY (`storage_id`) REFERENCES `pc_storage` (`storage_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

LOCK TABLES `pc_storage_type` WRITE;
/*!40000 ALTER TABLE `pc_storage_type` DISABLE KEYS */;

INSERT INTO `pc_storage_type` (`storage_type_id`, `storage_type_name`, `storage_type_desc`, `storage_id`, `is_active_y_n`, `cre_user`, `cre_ts`, `upd_user`, `upd_ts`)
VALUES
	(32,'Hbase','Hbase NoSQL DB',2,'Y','gimeladmin','2018-02-15 15:55:38','gimeladmin','2018-10-01 22:49:21'),
	(33,'Elastic','Elastic Search Type',3,'Y','gimeladmin','2018-02-15 16:38:46','gimeladmin','2018-10-01 22:49:21'),
	(34,'Kafka','Kafka Storage type used for streaming',5,'Y','gimeladmin','2018-02-15 16:47:40','gimeladmin','2018-10-01 22:49:21'),
	(35,'Teradata','Teradata storage type used for OLAP operations',6,'Y','gimeladmin','2018-02-15 16:52:12','gimeladmin','2018-10-01 22:49:21'),
	(36,'Hive','Hive storage type',6,'N','gimeladmin','2018-02-15 16:53:26','gimeladmin','2018-10-01 22:49:21');

/*!40000 ALTER TABLE `pc_storage_type` ENABLE KEYS */;
UNLOCK TABLES;


# Dump of table pc_storage_type_attribute_key
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_storage_type_attribute_key`;

CREATE TABLE `pc_storage_type_attribute_key` (
  `storage_ds_attribute_key_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_ds_attribute_key_name` varchar(1000) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `storage_ds_attribute_key_desc` varchar(1000) COLLATE utf8_unicode_ci DEFAULT NULL,
  `is_storage_system_level` enum('Y','N') CHARACTER SET utf8 NOT NULL,
  `storage_type_id` int(11) NOT NULL,
  `is_active_y_n` enum('Y','N') COLLATE utf8_unicode_ci NOT NULL,
  `cre_user` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`storage_ds_attribute_key_id`),
  UNIQUE KEY `type_key_idx` (`storage_ds_attribute_key_name`,`storage_type_id`),
  KEY `pc_storage_ds_attribute_key_ibfk_1_idx` (`storage_type_id`),
  CONSTRAINT `pc_storage_type_attribute_key_ibfk_1` FOREIGN KEY (`storage_type_id`) REFERENCES `pc_storage_type` (`storage_type_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

LOCK TABLES `pc_storage_type_attribute_key` WRITE;
/*!40000 ALTER TABLE `pc_storage_type_attribute_key` DISABLE KEYS */;

INSERT INTO `pc_storage_type_attribute_key` (`storage_ds_attribute_key_id`, `storage_ds_attribute_key_name`, `storage_ds_attribute_key_desc`, `is_storage_system_level`, `storage_type_id`, `is_active_y_n`, `cre_user`, `cre_ts`, `upd_user`, `upd_ts`)
VALUES
	(59,'gimel.hbase.columns.mapping','columns mapping','N',32,'Y','gimeladmin','2018-02-15 15:55:38','gimeladmin','2018-10-01 22:48:56'),
	(60,'gimel.hbase.table.name','table name','N',32,'Y','gimeladmin','2018-02-15 15:55:38','gimeladmin','2018-10-01 22:48:56'),
	(61,'gimel.hbase.namespace.name','hbase namespace name','N',32,'Y','gimeladmin','2018-02-15 15:55:38','gimeladmin','2018-10-01 22:48:56'),
	(62,'gimel.storage.type','hbase storage type flag','Y',32,'Y','gimeladmin','2018-02-15 15:55:38','gimeladmin','2018-10-01 22:48:56'),
	(63,'gimel.es.index.partition.delimiter','delimiter','N',33,'Y','gimeladmin','2018-02-15 16:38:46','gimeladmin','2018-10-01 22:48:56'),
	(64,'gimel.es.index.partition.isEnabled','is it partitioned','N',33,'Y','gimeladmin','2018-02-15 16:38:46','gimeladmin','2018-10-01 22:48:56'),
	(65,'es.resource','es index name','N',33,'Y','gimeladmin','2018-02-15 16:38:46','gimeladmin','2018-10-01 22:48:56'),
	(66,'es.port','port number','Y',33,'Y','gimeladmin','2018-02-15 16:38:46','gimeladmin','2018-10-01 22:48:56'),
	(67,'es.nodes','nodes for ES','Y',33,'Y','gimeladmin','2018-02-15 16:38:46','gimeladmin','2018-10-01 22:48:56'),
	(68,'gimel.storage.type','ES storage type flag','Y',33,'Y','gimeladmin','2018-02-15 16:41:50','gimeladmin','2018-10-01 22:48:56'),
	(69,'gimel.storage.type','pcatalog storage type key for kafka','Y',34,'Y','gimeladmin','2018-02-15 16:47:40','gimeladmin','2018-10-01 22:48:56'),
	(70,'auto.offset.reset','earliest offset','Y',34,'Y','gimeladmin','2018-02-15 16:47:40','gimeladmin','2018-10-01 22:48:56'),
	(72,'gimel.kafka.checkpoint.zookeeper.path','zookeeper path','N',34,'Y','gimeladmin','2018-02-15 16:47:40','gimeladmin','2018-10-01 22:48:56'),
	(73,'gimel.kafka.checkpoint.zookeeper.host','zookeeper host','Y',34,'Y','gimeladmin','2018-02-15 16:47:40','gimeladmin','2018-10-01 22:48:56'),
	(74,'gimel.kafka.avro.schema.source.url','schema registry URL','Y',34,'Y','gimeladmin','2018-02-15 16:47:40','gimeladmin','2018-10-01 22:48:56'),
	(75,'gimel.kafka.avro.schema.source.wrapper.key','wrapper key for source schema','Y',34,'Y','gimeladmin','2018-02-15 16:47:40','gimeladmin','2018-10-01 22:48:56'),
	(76,'gimel.kafka.avro.schema.source','source of avro schema','Y',34,'Y','gimeladmin','2018-02-15 16:47:40','gimeladmin','2018-10-01 22:48:56'),
	(77,'zookeeper.connection.timeout.ms','zookeeper time out','Y',34,'Y','gimeladmin','2018-02-15 16:47:40','gimeladmin','2018-10-01 22:48:56'),
	(78,'value.serializer','value serializer for kafka','N',34,'Y','gimeladmin','2018-02-15 16:47:40','gimeladmin','2018-10-01 22:48:56'),
	(79,'key.serializer','key serializer for kafka','N',34,'Y','gimeladmin','2018-02-15 16:47:40','gimeladmin','2018-10-01 22:48:56'),
	(80,'gimel.kafka.avro.schema.source.key','kafka topic name','N',34,'Y','gimeladmin','2018-02-15 16:47:40','gimeladmin','2018-10-01 22:48:56'),
	(81,'bootstrap.servers','broker list for kafka','Y',34,'Y','gimeladmin','2018-02-15 16:47:40','gimeladmin','2018-10-01 22:48:56'),
	(82,'gimel.kafka.whitelist.topics','kafka topic white list','N',34,'Y','gimeladmin','2018-02-15 16:47:40','gimeladmin','2018-10-01 22:48:56'),
	(83,'gimel.storage.type','pcatalog storage type key for teradata','Y',35,'Y','gimeladmin','2018-02-15 16:52:12','gimeladmin','2018-10-01 22:48:56'),
	(84,'gimel.jdbc.url','jdbc teradata cluster URL','Y',35,'Y','gimeladmin','2018-02-15 16:52:12','gimeladmin','2018-10-01 22:48:56'),
	(85,'gimel.jdbc.driver.class','teradata driver class','Y',35,'Y','gimeladmin','2018-02-15 16:52:12','gimeladmin','2018-10-01 22:48:56'),
	(86,'gimel.jdbc.input.table.name','teradata input table name','N',35,'Y','gimeladmin','2018-02-15 16:52:12','gimeladmin','2018-10-01 22:48:56'),
	(87,'gimel.hive.mysql.server','gimel hive mysql server','Y',36,'Y','gimeladmin','2018-03-23 12:16:23','gimeladmin','2018-10-01 22:48:56'),
	(88,'gimel.hdfs.nn','gimel hdfs name node','Y',36,'Y','gimeladmin','2018-03-23 12:17:06','gimeladmin','2018-10-01 22:48:56'),
	(89,'gimel.hdfs.storage.name','gimel storage name','Y',36,'Y','gimeladmin','2018-03-23 12:17:42','gimeladmin','2018-10-01 22:48:56'),
	(90,'gimel.storage.type','gimel storage type','Y',36,'Y','gimeladmin','2018-03-23 12:18:13','gimeladmin','2018-10-01 22:48:56'),
	(91,'gimel.hdfs.data.format','gimel hdfs data format','N',36,'Y','gimeladmin','2018-03-23 12:18:45','gimeladmin','2018-10-01 22:48:56'),
	(92,'gimel.hdfs.data.location','gimel hdfs data format location','N',36,'Y','gimeladmin','2018-03-23 12:19:20','gimeladmin','2018-10-01 22:48:56'),
	(93,'gimel.hive.mysql.db','gimel hive mysql DB','Y',36,'Y','gimeladmin','2018-03-23 17:04:09','gimeladmin','2018-10-01 22:48:56'),
	(94,'gimel.hive.mysql.port','gimel hive mysql port','Y',36,'Y','gimeladmin','2018-03-23 17:52:34','gimeladmin','2018-10-01 22:48:56'),
	(101,'key.deserializer','key deserializer for kafka','N',34,'Y','gimeladmin','2018-03-26 14:15:26','gimeladmin','2018-03-26 14:15:31'),
	(103,'value.deserializer','value deserializer for kafka','N',34,'Y','gimeladmin','2018-03-26 14:16:11','gimeladmin','2018-03-26 14:16:11'),
	(111,'gimel.hbase.rowkey','rowkey description','N',32,'Y','gimeladmin','2018-04-25 14:41:39','gimeladmin','2018-10-01 22:48:56'),
	(123,'key.deserializer.1','key.deserializer.1','N',34,'Y','gimeladmin','2018-09-28 18:28:13','gimeladmin','2018-10-01 22:48:56');

/*!40000 ALTER TABLE `pc_storage_type_attribute_key` ENABLE KEYS */;
UNLOCK TABLES;


# Dump of table pc_teradata_policy
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_teradata_policy`;

CREATE TABLE `pc_teradata_policy` (
  `teradata_policy_id` int(11) NOT NULL AUTO_INCREMENT,
  `database_name` varchar(200) NOT NULL DEFAULT '',
  `iam_role_name` varchar(200) NOT NULL DEFAULT '',
  `storage_system_id` int(11) NOT NULL,
  `is_active_y_n` enum('Y','N') DEFAULT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`teradata_policy_id`),
  UNIQUE KEY `teradata_policy_uniq_idx` (`database_name`,`iam_role_name`,`storage_system_id`),
  KEY `storage_system_tera_policy_fk` (`storage_system_id`),
  CONSTRAINT `storage_system_tera_policy_fk` FOREIGN KEY (`storage_system_id`) REFERENCES `pc_storage_system` (`storage_system_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table pc_users
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_users`;

CREATE TABLE `pc_users` (
  `user_id` int(11) NOT NULL AUTO_INCREMENT,
  `user_name` varchar(45) NOT NULL,
  `user_full_name` varchar(45) DEFAULT NULL,
  `is_active_y_n` enum('Y','N') NOT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`user_id`),
  UNIQUE KEY `user_name_UNIQUE` (`user_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `pc_users` WRITE;
/*!40000 ALTER TABLE `pc_users` DISABLE KEYS */;

INSERT INTO `pc_users` (`user_id`, `user_name`, `user_full_name`, `is_active_y_n`, `cre_user`, `cre_ts`, `upd_user`, `upd_ts`)
VALUES
	(22,'gimeldev','Gimel Administrator','Y',NULL,'2018-03-26 11:17:18',NULL,'2018-08-06 10:16:19'),
	(34,'gimeladmin','gimeladmin','Y',NULL,'2018-06-20 23:59:48',NULL,'2018-06-21 16:12:11');

/*!40000 ALTER TABLE `pc_users` ENABLE KEYS */;
UNLOCK TABLES;


# Dump of table pc_zones
# ------------------------------------------------------------

DROP TABLE IF EXISTS `pc_zones`;

CREATE TABLE `pc_zones` (
  `zone_id` int(11) NOT NULL AUTO_INCREMENT,
  `zone_name` varchar(100) NOT NULL,
  `zone_description` varchar(400) NOT NULL,
  `is_active_y_n` enum('Y','N') NOT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`zone_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

LOCK TABLES `pc_zones` WRITE;
/*!40000 ALTER TABLE `pc_zones` DISABLE KEYS */;

INSERT INTO `pc_zones` (`zone_id`, `zone_name`, `zone_description`, `is_active_y_n`, `cre_user`, `cre_ts`, `upd_user`, `upd_ts`)
VALUES
	(1,'az','Analytics zone','Y','gimeladmin','2018-08-22 13:35:17','gimeladmin','2018-10-01 22:47:55'),
	(2,'rz','restricted zone','Y','gimeladmin','2018-08-22 13:35:43','gimeladmin','2018-10-01 22:47:55'),
	(3,'corp','Corp zone','Y','gimeladmin','2018-08-22 13:36:11','gimeladmin','2018-10-01 22:47:55');

/*!40000 ALTER TABLE `pc_zones` ENABLE KEYS */;
UNLOCK TABLES;



/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
