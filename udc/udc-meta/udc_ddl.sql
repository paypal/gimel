# Dump of table pc_api_usage_metrics
# ------------------------------------------------------------
CREATE DATABASE pcatalog;

USE pcatalog;

CREATE TABLE `pc_api_usage_metrics` (
  `api_usage_metric_id` int(11) NOT NULL AUTO_INCREMENT,
  `request_url` varchar(1000) NOT NULL DEFAULT '',
  `request_id` varchar(100) NOT NULL DEFAULT '',
  `request_method` enum('GET','POST','PUT','DELETE') NOT NULL,
  `response_status` int(11) NOT NULL,
  `request_paradigm` varchar(50) NOT NULL DEFAULT '',
  `host_name` varchar(500) NOT NULL DEFAULT '',
  `app_name` varchar(500) NOT NULL DEFAULT '',
  `user_name` varchar(500) NOT NULL DEFAULT '',
  `params` varchar(1000) DEFAULT '',
  `start_time` bigint(20) DEFAULT NULL,
  `end_time` bigint(20) DEFAULT NULL,
  `total_time` float NOT NULL,
  PRIMARY KEY (`api_usage_metric_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table pc_attribute_discussion_init
# ------------------------------------------------------------

CREATE TABLE `pc_attribute_discussion_init` (
  `discussion_id` int(11) NOT NULL AUTO_INCREMENT,
  `jira_id` varchar(30) NOT NULL DEFAULT '',
  `source_provider_id` varchar(100) NOT NULL DEFAULT '',
  `discussion_tittle` varchar(1000) NOT NULL DEFAULT '',
  `thread_summary` varchar(1000) NOT NULL DEFAULT '',
  `discussion_details` varchar(10000) NOT NULL DEFAULT '',
  `discussion_misc` varchar(1000) NOT NULL DEFAULT '',
  `create_timestamp` bigint(20) NOT NULL,
  `create_user` varchar(100) NOT NULL,
  `update_timestamp` bigint(20) NOT NULL,
  `update_user` varchar(100) NOT NULL,
  PRIMARY KEY (`discussion_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table pc_attribute_discussion_responses
# ------------------------------------------------------------

CREATE TABLE `pc_attribute_discussion_responses` (
  `response_id` int(11) NOT NULL AUTO_INCREMENT,
  `discussion_id` int(11) NOT NULL,
  `source_response_id` varchar(1000) NOT NULL DEFAULT '',
  `response_summary` varchar(100) NOT NULL DEFAULT '',
  `response_details` varchar(10000) NOT NULL DEFAULT '',
  `response_misc` varchar(1000) NOT NULL DEFAULT '',
  `create_timestamp` bigint(20) NOT NULL,
  `create_user` varchar(100) NOT NULL,
  `update_timestamp` bigint(20) NOT NULL,
  `update_user` varchar(100) NOT NULL,
  PRIMARY KEY (`response_id`),
  KEY `discussion_fk_idx` (`discussion_id`),
  CONSTRAINT `discussion_fk` FOREIGN KEY (`discussion_id`) REFERENCES `pc_attribute_discussion_init` (`discussion_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

# Dump of table pc_source_provider
# ------------------------------------------------------------

CREATE TABLE `pc_source_provider` (
  `source_provider_id` int(11) NOT NULL AUTO_INCREMENT,
  `source_provider_name` varchar(100) NOT NULL DEFAULT '',
  `source_provider_description` varchar(400) NOT NULL DEFAULT '',
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`source_provider_id`),
  UNIQUE KEY `source_provider_name` (`source_provider_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

# Dump of table pc_classifications
# ------------------------------------------------------------

CREATE TABLE `pc_classifications` (
  `dataset_classification_id` int(11) NOT NULL AUTO_INCREMENT,
  `data_source_type` varchar(50) NOT NULL DEFAULT '',
  `data_source` varchar(50) NOT NULL DEFAULT '',
  `container_name` varchar(100) NOT NULL DEFAULT '',
  `object_name` varchar(100) NOT NULL DEFAULT '',
  `column_name` varchar(100) NOT NULL DEFAULT '',
  `provider_id` int(11) NOT NULL,
  `classification_id` int(11) NOT NULL,
  `classification_comment` longtext NOT NULL,
  `cre_user` varchar(50) NOT NULL,
  `cre_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT '',
  `upd_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`dataset_classification_id`),
  UNIQUE KEY `type_source_container_object_column_provider_cre_user` (`data_source_type`,`data_source`,`container_name`,`object_name`,`column_name`,`provider_id`,`cre_user`),
  KEY `provider_fk` (`provider_id`),
  CONSTRAINT `provider_fk` FOREIGN KEY (`provider_id`) REFERENCES `pc_source_provider` (`source_provider_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table pc_classifications_attributes
# ------------------------------------------------------------

CREATE TABLE `pc_classifications_attributes` (
  `dataset_classification_attribute_id` int(11) NOT NULL AUTO_INCREMENT,
  `dataset_classification_id` int(11) NOT NULL,
  `sample_size` varchar(50) NOT NULL DEFAULT '',
  `true_positive` varchar(50) NOT NULL DEFAULT '',
  `confidence_score` varchar(50) NOT NULL,
  `alert_create_date` varchar(100) NOT NULL,
  `is_encrypted` varchar(50) NOT NULL,
  `encryption_type` varchar(100) NOT NULL,
  `classifier_model_version` varchar(100) NOT NULL,
  `classifier_model` varchar(100) NOT NULL,
  PRIMARY KEY (`dataset_classification_attribute_id`),
  KEY `classification_fk` (`dataset_classification_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

# Dump of table pc_entities
# ------------------------------------------------------------

CREATE TABLE `pc_entities` (
  `entity_id` int(11) NOT NULL AUTO_INCREMENT,
  `entity_name` varchar(100) NOT NULL,
  `entity_description` varchar(400) NOT NULL,
  `is_active_y_n` enum('Y','N') NOT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`entity_id`),
  UNIQUE KEY `entity_name_unq_idx` (`entity_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


# Dump of table pc_users
# ------------------------------------------------------------

CREATE TABLE `pc_users` (
  `user_id` int(11) NOT NULL AUTO_INCREMENT,
  `user_name` varchar(45) NOT NULL,
  `user_full_name` varchar(45) DEFAULT NULL,
  `qid` varchar(50) DEFAULT NULL,
  `roles` varchar(1000) DEFAULT NULL,
  `manager_name` varchar(100) DEFAULT NULL,
  `organization` varchar(1000) DEFAULT NULL,
  `location` varchar(100) DEFAULT NULL,
  `is_active_y_n` enum('Y','N') NOT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`user_id`),
  UNIQUE KEY `user_name_UNIQUE` (`user_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

# Dump of table pc_zones
# ------------------------------------------------------------

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


# Dump of table pc_storage_cluster
# ------------------------------------------------------------

CREATE TABLE `pc_storage_cluster` (
  `cluster_id` int(11) NOT NULL AUTO_INCREMENT,
  `cluster_name` varchar(45) NOT NULL,
  `cluster_description` varchar(1000) DEFAULT NULL,
  `is_active_y_n` enum('Y','N') NOT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`cluster_id`),
  UNIQUE KEY `cluster_name_UNIQUE` (`cluster_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


# Dump of table pc_storage
# ------------------------------------------------------------

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

# Dump of table pc_storage_type
# ------------------------------------------------------------

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



# Dump of table pc_storage_system
# ------------------------------------------------------------

CREATE TABLE `pc_storage_system` (
  `storage_system_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_system_name` varchar(1000) CHARACTER SET utf8 NOT NULL DEFAULT '',
  `storage_system_desc` varchar(1000) COLLATE utf8_unicode_ci DEFAULT NULL,
  `storage_type_id` int(11) NOT NULL,
  `entity_id` int(11) NOT NULL DEFAULT '1',
  `running_cluster_id` int(11) NOT NULL,
  `discovery_sla` enum('15','60','120','180','240','1440') COLLATE utf8_unicode_ci NOT NULL DEFAULT '60',
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
  KEY `entity_id` (`entity_id`),
  CONSTRAINT `admin_user_system_fk` FOREIGN KEY (`admin_user`) REFERENCES `pc_users` (`user_id`),
  CONSTRAINT `entity_id_fk` FOREIGN KEY (`entity_id`) REFERENCES `pc_entities` (`entity_id`),
  CONSTRAINT `pc_storage_system_ibfk_1` FOREIGN KEY (`zone_id`) REFERENCES `pc_zones` (`zone_id`),
  CONSTRAINT `running_cluster` FOREIGN KEY (`running_cluster_id`) REFERENCES `pc_storage_cluster` (`cluster_id`),
  CONSTRAINT `system_type_fk` FOREIGN KEY (`storage_type_id`) REFERENCES `pc_storage_type` (`storage_type_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;


# Dump of table pc_classifications_map
# ------------------------------------------------------------

CREATE TABLE `pc_classifications_map` (
  `dataset_classification_map_id` int(11) NOT NULL AUTO_INCREMENT,
  `dataset_classification_id` int(11) NOT NULL,
  `storage_dataset_id` int(11) NOT NULL,
  `storage_system_id` int(11) DEFAULT NULL,
  `zone_id` int(11) DEFAULT NULL,
  `entity_id` int(11) DEFAULT NULL,
  `is_active_y_n` enum('Y','N') DEFAULT NULL,
  `cre_user` varchar(50) DEFAULT '',
  `cre_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`dataset_classification_map_id`),
  UNIQUE KEY `dataset_class_system_zone_entity_uniq_key` (`storage_dataset_id`,`dataset_classification_id`,`storage_system_id`,`entity_id`,`zone_id`),
  KEY `classification_fk` (`dataset_classification_id`),
  KEY `zone_fk` (`zone_id`),
  KEY `system_id_fk` (`storage_system_id`),
  KEY `entity_fk` (`entity_id`),
  CONSTRAINT `classification_fk` FOREIGN KEY (`dataset_classification_id`) REFERENCES `pc_classifications` (`dataset_classification_id`),
  CONSTRAINT `entity_fk` FOREIGN KEY (`entity_id`) REFERENCES `pc_entities` (`entity_id`),
  CONSTRAINT `system_id_fk` FOREIGN KEY (`storage_system_id`) REFERENCES `pc_storage_system` (`storage_system_id`),
  CONSTRAINT `zone_fk` FOREIGN KEY (`zone_id`) REFERENCES `pc_zones` (`zone_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


# Dump of table pc_dataset_description_map
# ------------------------------------------------------------

CREATE TABLE `pc_dataset_description_map` (
  `bodhi_dataset_map_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_dataset_id` int(11) NOT NULL,
  `storage_system_id` int(11) NOT NULL,
  `provider_id` int(11) NOT NULL,
  `object_name` varchar(100) NOT NULL,
  `container_name` varchar(100) NOT NULL,
  `object_comment` longtext NOT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`bodhi_dataset_map_id`),
  UNIQUE KEY `system_provider_object_container` (`storage_system_id`,`provider_id`,`object_name`,`container_name`),
  KEY `provider_dataset_desc_fk` (`provider_id`),
  CONSTRAINT `provider_dataset_desc_fk` FOREIGN KEY (`provider_id`) REFERENCES `pc_source_provider` (`source_provider_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table pc_dataset_column_description_map
# ------------------------------------------------------------

CREATE TABLE `pc_dataset_column_description_map` (
  `bodhi_dataset_column_map_id` int(11) NOT NULL AUTO_INCREMENT,
  `bodhi_dataset_map_id` int(11) NOT NULL,
  `column_name` varchar(100) NOT NULL DEFAULT '',
  `column_comment` longtext,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`bodhi_dataset_column_map_id`),
  UNIQUE KEY `column_dataset_uniq` (`column_name`,`bodhi_dataset_map_id`),
  KEY `bodhi_dataset_map_fk` (`bodhi_dataset_map_id`),
  CONSTRAINT `bodhi_dataset_map_fk` FOREIGN KEY (`bodhi_dataset_map_id`) REFERENCES `pc_dataset_description_map` (`bodhi_dataset_map_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


# Dump of table pc_dataset_ownership_map
# ------------------------------------------------------------

CREATE TABLE `pc_dataset_ownership_map` (
  `dataset_ownership_map_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_dataset_id` int(11) NOT NULL,
  `provider_id` int(11) NOT NULL,
  `owner_name` varchar(100) NOT NULL,
  `owner_email` varchar(200) NOT NULL DEFAULT '',
  `email_ilist` varchar(200) NOT NULL,
  `claimed_by` varchar(100) DEFAULT NULL,
  `storage_system_id` int(11) DEFAULT NULL,
  `container_name` varchar(100) NOT NULL,
  `object_name` varchar(100) NOT NULL DEFAULT '',
  `ownership_comment` longtext,
  `is_notified` enum('Y','N') NOT NULL DEFAULT 'N',
  `cre_user` varchar(50) NOT NULL,
  `cre_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`dataset_ownership_map_id`),
  UNIQUE KEY `container_system_object_owner_uniq_idx` (`storage_system_id`,`container_name`,`object_name`,`owner_name`),
  KEY `dataset_ownership_provider_fk` (`provider_id`),
  CONSTRAINT `dataset_ownership_provider_fk` FOREIGN KEY (`provider_id`) REFERENCES `pc_source_provider` (`source_provider_id`),
  CONSTRAINT `dataset_ownership_system_fk` FOREIGN KEY (`storage_system_id`) REFERENCES `pc_storage_system` (`storage_system_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table pc_object_schema_map
# ------------------------------------------------------------

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



# Dump of table pc_storage_dataset
# ------------------------------------------------------------

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

# Dump of table pc_discussion_to_attribute_map
# ------------------------------------------------------------

CREATE TABLE `pc_discussion_to_attribute_map` (
  `map_id` int(11) NOT NULL AUTO_INCREMENT,
  `dataset_id` int(11) NOT NULL,
  `column_name` varchar(100) NOT NULL DEFAULT '',
  `object_name` varchar(100) NOT NULL DEFAULT '',
  `container_name` varchar(100) NOT NULL DEFAULT '',
  `discussion_id` int(11) NOT NULL,
  `source_name` varchar(100) NOT NULL DEFAULT '',
  PRIMARY KEY (`map_id`),
  KEY `dicussion_fk_idx` (`discussion_id`),
  KEY `dataset_fk_idx` (`dataset_id`),
  CONSTRAINT `dataset_fk` FOREIGN KEY (`dataset_id`) REFERENCES `pc_storage_dataset` (`storage_dataset_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `dicussion_fk` FOREIGN KEY (`discussion_id`) REFERENCES `pc_attribute_discussion_init` (`discussion_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table pc_notifications
# ------------------------------------------------------------

CREATE TABLE `pc_notifications` (
  `notification_id` int(11) NOT NULL AUTO_INCREMENT,
  `notification_type` enum('MAINTAINENCE','NEW FEATURE','ISSUE','OTHER') NOT NULL DEFAULT 'ISSUE',
  `notification_content` longtext NOT NULL,
  `notification_priority` enum('0','1','2','3','4') NOT NULL DEFAULT '3',
  `notification_message_page` varchar(100) NOT NULL DEFAULT '',
  `notification_live_time` varchar(20) NOT NULL DEFAULT '',
  `reference_url` varchar(500) DEFAULT NULL,
  `is_active_y_n` enum('Y','N') NOT NULL DEFAULT 'Y',
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) NOT NULL DEFAULT '',
  `upd_ts` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`notification_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table pc_policy_discovery_metric
# ------------------------------------------------------------

CREATE TABLE `pc_policy_discovery_metric` (
  `policy_discovery_metric_id` int(11) NOT NULL AUTO_INCREMENT,
  `policy_discovery_metric_type` enum('TERADATA','RANGER','UNKNOWN') DEFAULT 'UNKNOWN',
  `start_time` timestamp NULL DEFAULT NULL,
  `end_time` timestamp NULL DEFAULT NULL,
  `total_deletes` int(11) DEFAULT NULL,
  `total_upserts` int(11) NOT NULL,
  `total_inserts` int(11) NOT NULL,
  `running_user` varchar(100) NOT NULL DEFAULT '',
  PRIMARY KEY (`policy_discovery_metric_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table pc_ranger_policy
# ------------------------------------------------------------

CREATE TABLE `pc_ranger_policy` (
  `derived_policy_id` int(11) NOT NULL AUTO_INCREMENT,
  `policy_id` int(11) NOT NULL,
  `cluster_id` int(11) NOT NULL,
  `policy_name` varchar(1000) CHARACTER SET utf8 NOT NULL DEFAULT '',
  `type_name` varchar(50) CHARACTER SET utf8 NOT NULL DEFAULT '',
  `policy_locations` longtext CHARACTER SET utf8 NOT NULL,
  `column` longtext COLLATE utf8_unicode_ci,
  `column_family` longtext COLLATE utf8_unicode_ci,
  `table` longtext COLLATE utf8_unicode_ci,
  `database` longtext COLLATE utf8_unicode_ci,
  `queue` longtext COLLATE utf8_unicode_ci,
  `is_active_y_n` enum('Y','N') CHARACTER SET utf8 NOT NULL,
  `cre_user` varchar(45) CHARACTER SET utf8 DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) CHARACTER SET utf8 DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`derived_policy_id`),
  UNIQUE KEY `policy_name_id_cluster_UNIQUE` (`policy_id`,`cluster_id`,`policy_name`),
  KEY `cluster_id_fk` (`cluster_id`),
  CONSTRAINT `cluster_id_fk` FOREIGN KEY (`cluster_id`) REFERENCES `pc_storage_cluster` (`cluster_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



# Dump of table pc_ranger_policy_user_group
# ------------------------------------------------------------

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

# Dump of table pc_schema_dataset_map
# ------------------------------------------------------------

CREATE TABLE `pc_schema_dataset_map` (
  `schema_dataset_map_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_dataset_id` int(11) NOT NULL,
  `storage_system_id` int(11) NOT NULL,
  `provider_id` int(11) DEFAULT NULL,
  `object_type` enum('TABLE','VIEW','UNKNOWN') NOT NULL DEFAULT 'TABLE',
  `object_name` varchar(100) NOT NULL,
  `container_name` varchar(100) NOT NULL,
  `object_comment` longtext NOT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`schema_dataset_map_id`),
  UNIQUE KEY `system_object_container_provider_schema_uniq_idx` (`storage_system_id`,`object_name`,`container_name`,`provider_id`),
  KEY `provider_schema_fk` (`provider_id`),
  CONSTRAINT `provider_schema_fk` FOREIGN KEY (`provider_id`) REFERENCES `pc_source_provider` (`source_provider_id`),
  CONSTRAINT `system_id_schema_fk` FOREIGN KEY (`storage_system_id`) REFERENCES `pc_storage_system` (`storage_system_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

# Dump of table pc_schema_dataset_column_map
# ------------------------------------------------------------

CREATE TABLE `pc_schema_dataset_column_map` (
  `schema_dataset_column_map_id` int(11) NOT NULL AUTO_INCREMENT,
  `schema_dataset_map_id` int(11) NOT NULL,
  `column_code` varchar(100) NOT NULL DEFAULT '',
  `column_name` varchar(100) NOT NULL DEFAULT '',
  `column_datatype` varchar(50) NOT NULL DEFAULT '',
  `column_comment` longtext,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`schema_dataset_column_map_id`),
  UNIQUE KEY `schema_dataset_column_uniq_idx` (`schema_dataset_map_id`,`column_code`,`column_name`,`column_datatype`),
  CONSTRAINT `schema_dataset_map_fk` FOREIGN KEY (`schema_dataset_map_id`) REFERENCES `pc_schema_dataset_map` (`schema_dataset_map_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


# Dump of table pc_storage_cluster_attribute_key
# ------------------------------------------------------------

CREATE TABLE `pc_storage_cluster_attribute_key` (
  `cluster_attribute_key_id` int(11) NOT NULL AUTO_INCREMENT,
  `cluster_attribute_key_name` varchar(45) NOT NULL DEFAULT '',
  `cluster_attribute_key_type` enum('SPARK','GIMEL','LIVY','HIVE','OTHER') DEFAULT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`cluster_attribute_key_id`),
  UNIQUE KEY `key_name_key_type_index` (`cluster_attribute_key_name`,`cluster_attribute_key_type`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table pc_storage_cluster_attribute_value
# ------------------------------------------------------------

CREATE TABLE `pc_storage_cluster_attribute_value` (
  `cluster_attribute_value_id` int(11) NOT NULL AUTO_INCREMENT,
  `cluster_attribute_key_id` int(11) NOT NULL,
  `cluster_id` int(11) NOT NULL,
  `cluster_attribute_value` varchar(1000) NOT NULL DEFAULT '',
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`cluster_attribute_value_id`),
  KEY `cluster_id_conf_fk` (`cluster_id`),
  KEY `cluster_attribute_key_id_fk` (`cluster_attribute_key_id`),
  CONSTRAINT `cluster_attribute_key_id_fk` FOREIGN KEY (`cluster_attribute_key_id`) REFERENCES `pc_storage_cluster_attribute_key` (`cluster_attribute_key_id`),
  CONSTRAINT `cluster_id_conf_fk` FOREIGN KEY (`cluster_id`) REFERENCES `pc_storage_cluster` (`cluster_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table pc_storage_dataset_change_log
# ------------------------------------------------------------

CREATE TABLE `pc_storage_dataset_change_log` (
  `storage_dataset_change_log_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_dataset_id` int(11) NOT NULL,
  `storage_dataset_change_type` enum('C','M','D') CHARACTER SET utf8 DEFAULT NULL,
  `change_column_type` varchar(100) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `change_column_prev_val` json DEFAULT NULL,
  `change_column_curr_val` json DEFAULT NULL,
  `upd_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`storage_dataset_change_log_id`),
  UNIQUE KEY `change_log_dataset_uniq_idx` (`storage_dataset_id`,`storage_dataset_change_log_id`),
  CONSTRAINT `dataset_change_log_fk` FOREIGN KEY (`storage_dataset_id`) REFERENCES `pc_storage_dataset` (`storage_dataset_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



# Dump of table pc_storage_dataset_system
# ------------------------------------------------------------

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


# Dump of table pc_tags
# ------------------------------------------------------------

CREATE TABLE `pc_tags` (
  `tag_id` int(11) NOT NULL AUTO_INCREMENT,
  `tag_name` varchar(100) NOT NULL DEFAULT '',
  `provider_id` int(11) NOT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`tag_id`),
  UNIQUE KEY `name_provider_cre_user` (`tag_name`,`provider_id`,`cre_user`),
  KEY `provider_id_fk` (`provider_id`),
  CONSTRAINT `provider_id_fk` FOREIGN KEY (`provider_id`) REFERENCES `pc_source_provider` (`source_provider_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


# Dump of table pc_storage_dataset_tag_map
# ------------------------------------------------------------

CREATE TABLE `pc_storage_dataset_tag_map` (
  `dataset_tag_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_dataset_id` int(11) NOT NULL,
  `tag_id` int(11) NOT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`dataset_tag_id`),
  UNIQUE KEY `storage_dataset_id` (`storage_dataset_id`,`tag_id`),
  KEY `tag_id_fk` (`tag_id`),
  CONSTRAINT `tag_id_fk` FOREIGN KEY (`tag_id`) REFERENCES `pc_tags` (`tag_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table pc_storage_object_attribute_custom_key_value
# ------------------------------------------------------------

CREATE TABLE `pc_storage_object_attribute_custom_key_value` (
  `object_attribute_key_value_id` int(11) NOT NULL AUTO_INCREMENT,
  `object_id` int(11) NOT NULL,
  `object_attribute_key` varchar(200) NOT NULL,
  `object_attribute_value` longtext NOT NULL,
  `is_active_y_n` enum('Y','N') DEFAULT NULL,
  `cre_user` varchar(45) DEFAULT NULL,
  `cre_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `upd_user` varchar(45) DEFAULT NULL,
  `upd_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`object_attribute_key_value_id`),
  UNIQUE KEY `attribute_key_object_uniq_key` (`object_attribute_key`,`object_id`),
  KEY `object_attribute_fk` (`object_id`),
  CONSTRAINT `pc_storage_object_attribute_custom_key_value_ibfk_1` FOREIGN KEY (`object_id`) REFERENCES `pc_object_schema_map` (`object_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

# Dump of table pc_storage_type_attribute_key
# ------------------------------------------------------------

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


# Dump of table pc_storage_object_attribute_value
# ------------------------------------------------------------

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


# Dump of table pc_storage_system_attribute_value
# ------------------------------------------------------------

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



# Dump of table pc_storage_system_container
# ------------------------------------------------------------

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
  CONSTRAINT `cluster_system_fk` FOREIGN KEY (`cluster_id`) REFERENCES `pc_storage_cluster` (`cluster_id`),
  CONSTRAINT `storage_system_fk` FOREIGN KEY (`storage_system_id`) REFERENCES `pc_storage_system` (`storage_system_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table pc_storage_system_discovery
# ------------------------------------------------------------

CREATE TABLE `pc_storage_system_discovery` (
  `storage_system_discovery_id` int(11) NOT NULL AUTO_INCREMENT,
  `storage_system_id` int(11) NOT NULL,
  `start_time` timestamp NULL DEFAULT NULL,
  `end_time` timestamp NULL DEFAULT NULL,
  `error_log` longtext NOT NULL,
  `total_deletes` int(11) DEFAULT NULL,
  `total_upserts` int(11) NOT NULL,
  `total_inserts` int(11) NOT NULL,
  `discovery_status` enum('SUCCESS','RUNNING') NOT NULL DEFAULT 'SUCCESS',
  `running_user` varchar(100) NOT NULL DEFAULT '',
  PRIMARY KEY (`storage_system_discovery_id`),
  KEY `storage_system_fk_idx` (`storage_system_id`),
  CONSTRAINT `pc_storage_system_discovery_ibfk_2` FOREIGN KEY (`storage_system_id`) REFERENCES `pc_storage_system` (`storage_system_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


# Dump of table pc_teradata_policy
# ------------------------------------------------------------

CREATE TABLE `pc_teradata_policy` (
  `teradata_policy_id` int(11) NOT NULL AUTO_INCREMENT,
  `database_name` varchar(200) NOT NULL DEFAULT '',
  `role_for` varchar(1000) DEFAULT NULL,
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



# Dump of table pc_user_dataset_top_picks
# ------------------------------------------------------------

CREATE TABLE `pc_user_dataset_top_picks` (
  `user_dataset_top_pick_id` int(11) NOT NULL AUTO_INCREMENT,
  `source_provider_id` int(11) NOT NULL,
  `storage_dataset_id` int(11) NOT NULL,
  `comment` text,
  `rank` int(11) NOT NULL,
  PRIMARY KEY (`user_dataset_top_pick_id`),
  KEY `source_provider_id_fk` (`source_provider_id`),
  CONSTRAINT `source_provider_id_fk` FOREIGN KEY (`source_provider_id`) REFERENCES `pc_source_provider` (`source_provider_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table pc_user_storage_dataset_map
# ------------------------------------------------------------

CREATE TABLE `pc_user_storage_dataset_map` (
  `user_dataset_map_id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `storage_dataset_id` int(11) NOT NULL,
  `map_type` enum('UDC_VIEWS','NB_VIEWS','UDC_GA','NB_GA','UDC_TP','NB_TP') CHARACTER SET utf8 NOT NULL,
  `rank` int(11) NOT NULL,
  PRIMARY KEY (`user_dataset_map_id`),
  KEY `user_id_fk` (`user_id`),
  KEY `storage_dataset_id_fk` (`storage_dataset_id`),
  CONSTRAINT `storage_dataset_id_fk` FOREIGN KEY (`storage_dataset_id`) REFERENCES `pc_storage_dataset` (`storage_dataset_id`),
  CONSTRAINT `user_id_fk` FOREIGN KEY (`user_id`) REFERENCES `pc_users` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
