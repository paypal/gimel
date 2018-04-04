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

package com.paypal.gimel.druid.model

import org.json4s.FieldSerializer
import org.json4s.FieldSerializer.{renameFrom, renameTo}

/**
  * Druid Dimension Object.
  * Class extends Serializable as it is passed to executors.
  *
  * @param name Name for the dimension
  */
@SerialVersionUID(100L)
case class DruidDimension(name: String)  extends Serializable

object DruidDimension {
  object DimensionFieldNames {
    val NAME = "name"
  }

  // Deserializer for Druid Dimension.
  // Rename name -> name
  val drudDimensionSerializer: FieldSerializer[DruidDimension] = FieldSerializer[DruidDimension] (
      renameTo("name", DimensionFieldNames.NAME),
      renameFrom(DimensionFieldNames.NAME, "name")
  )
}
