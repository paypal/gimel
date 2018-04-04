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

package com.paypal.gimel.druid.util

import com.metamx.tranquility.beam.{Beam, ClusteredBeamTuning}
import com.metamx.tranquility.druid.{DruidBeams, DruidLocation, DruidRollup, SpecificDruidDimensions}
import com.metamx.tranquility.spark.BeamFactory
import com.metamx.tranquility.typeclass.Timestamper
import io.druid.data.input.impl.TimestampSpec
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.joda.time.{DateTime, DateTimeZone, Period}

import com.paypal.gimel.druid.conf.DruidClientConfiguration

/**
  * DruidEventBeam object.
  * Given a DruidClientConfiguration, returns a singleton instance of DruidBeam.
  * DruidBeam instance should be a singleton inorder to share the same connection
  */
object DruidEventBeam {
  var druidConfig: DruidClientConfiguration = _

  /**
    * Method to initialize the required params for the DruidEventBeam instance.
    * This method must be call before trying to fetch BeamInstance
    * DruidClientConfiguration is a required param that needs to be set.
    *
    * @param configMgr DruidClientConfiguration object to be set for defining configuration.
    */
  def init(configMgr: DruidClientConfiguration): Unit = {
    druidConfig = configMgr
  }

  /**
    * Timestamper object that defines how to extract a timestamp from any custom object
    */
  implicit val timestamper = new Timestamper[Map[String, Any]]() {

    /**
      * Overriden method to extract timestamp from a given custom object.
      *
      * @param rowMap Map[String, String] representing a single row with
      *               (columnName -> columnValue) format map
      * @return org.joda.time.DateTime by extracting timestamp from the rowMap
      */
    override def timestamp(rowMap: Map[String, Any]): DateTime = {
      new DateTime(rowMap(druidConfig.timestamp_field), DateTimeZone.UTC)
    }
  }

  /**
    * Builds and stores a singleton instance of Beam[T] given the
    * DruidClientConfiguration object for configuration.
    */
  lazy val BeamInstance: Beam[Map[String, Any]] = {

    // Tranquility uses ZooKeeper (through Curator framework) for coordination.
    val curator = CuratorFrameworkFactory.newClient(
      druidConfig.zookeeper,
      new BoundedExponentialBackoffRetry(100, 3000, 5)
    )
    curator.start()

    // Transforms List[DruidDimensions] from the DruidClientConfiguration to List[String]
    val dimensions = druidConfig
      .dimensions
      .map(_.name)

    // Transforms List[DruidMetrics] from the DruidClientConfiguration to List[AggregatorFactory]
    val aggregators = druidConfig
      .metrics
      .map(_.getAggregator)

    // Building a Druid Beam
    DruidBeams
      .builder()
      .curator(curator)
      .discoveryPath(druidConfig.discoveryPath)
      .location(DruidLocation.create(druidConfig.indexService, druidConfig.datasource))
      .rollup(DruidRollup(SpecificDruidDimensions(dimensions),
        aggregators, DruidUtility.fetchQueryGranularity(druidConfig.queryGranularity)))
      .tuning(
        ClusteredBeamTuning (
          segmentGranularity = druidConfig.segmentGranularity,
          windowPeriod = new Period(druidConfig.windowPeriod),
          partitions = druidConfig.numPartitions, replicants = druidConfig.numReplicants
        )
      )
      .timestampSpec(new TimestampSpec(druidConfig.timestamp_field, "iso", null))
      .buildBeam()
  }
}

class DruidEventBeam(config: DruidClientConfiguration) extends BeamFactory[Map[String, Any]] {
  // Return a singleton, so the same connection is shared across all tasks in the same JVM.
  def makeBeam: Beam[Map[String, Any]] = {
    DruidEventBeam.init(config)
    DruidEventBeam.BeamInstance
  }
}

