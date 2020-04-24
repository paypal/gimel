/*
 * Copyright 2017 PayPal Inc.
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

package com.paypal.gimel.common.utilities

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.gimelservices.GimelServiceUtilities
import com.paypal.gimel.common.security.ColumnClassHandler
import org.apache.commons.lang3.ArrayUtils
import org.scalatest.FunSuite

import scala.collection.immutable.Map
import scala.io.Source

class ColumnClassHandlerTest extends FunSuite {

  import spray.json._
  val gimelServiceUtilities = GimelServiceUtilities()

  test("column class handler lookup with catalog provider") {
    val fileContent = Source
      .fromFile(
        "src/test/resources/pp_engineering_views.wtransaction_p2_intra.json"
      )
      .getLines
      .mkString
    val datasetName =
      "Teradata.Simba.PP_ENGINEERING_VIEWS.STG_WTRANSACTION_P2_INTRA"
    val dataSetProperties = gimelServiceUtilities.getDataSetProperties(
      datasetName,
      fileContent.parseJson.asJsObject
    )
    val dspropsUpdated: DataSetProperties = dataSetProperties.copy(
      props = dataSetProperties.props ++ Map(
        GimelConstants.GIMEL_SECURITY_ACCESS_AUDIT_CLASS_PROVIDER -> "TEST"
      )
    )
    val props = Map(
      GimelConstants.DATASET_PROPS -> dspropsUpdated,
      GimelConstants.DATASET -> datasetName
    )
    val columnClassSeq = ColumnClassHandler.getColumnsByColumnClass(
      dspropsUpdated.fields,
      dspropsUpdated.props
    )
    columnClassSeq.foreach(a => {
      println(s"${a._1} -> ${ArrayUtils.toString(a._2)}")
    })
    val restrictedColumns = ColumnClassHandler.getRestrictedColumns(props)
    println(restrictedColumns)
    assert(restrictedColumns("4:ORION").equals("AMOUNT,STATUS"))
    assert(restrictedColumns("5:TEST").equals("CCTRANS_ID"))
    assert(restrictedColumns("3:TEST").equals("ADDRESS_ID"))
    assert(restrictedColumns("3:SPDR").equals("PARENT_ID,SHARED_ID"))
    assert(restrictedColumns("3:ORION").equals("ACH_ID"))
  }
}
