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

package com.paypal.gimel.cassandra.conf

object CassandraConfigs {
  val gimelCassandraConnectionHosts = "spark.cassandra.connection.host"
  val gimelCassandraSparkTtl = "spark.cleaner.ttl"
  val gimelCassandraClusterName = "gimel.cassandra.cluster.name"
  val gimelCassandraKeySpaceName = "gimel.cassandra.keyspace.name"
  val gimelCassandraTableName = "gimel.cassandra.table.name"
  val gimelCassandraPushDownIsEnabled = "gimel.cassandra.pushdown.is.enabled"
  val gimelCassandraTableConfirmTruncate = "gimel.cassandra.table.confirm.truncate"
  val gimelCassandraSparkDriver = "org.apache.spark.sql.cassandra"
  val gimelCassandraInputSize = "spark.cassandra.input.split.size_in_mb"
}

