/*
 * Copyright 2019 PayPal Inc.
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

package com.paypal.udc.service;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import com.paypal.udc.entity.zone.Zone;
import com.paypal.udc.exception.ValidationError;


public interface IZoneService {

    List<Zone> getAllZones();

    Zone getZoneById(long zoneId) throws ValidationError;

    Zone addZone(Zone zone) throws ValidationError, IOException, InterruptedException, ExecutionException;

    Zone updateZone(Zone zone) throws ValidationError, IOException, InterruptedException, ExecutionException;

    Zone deActivateZone(long zoneId) throws ValidationError, IOException, InterruptedException, ExecutionException;

    Zone getZoneByName(String zoneName);

    Zone reActivateZone(long zoneId) throws ValidationError, IOException, InterruptedException, ExecutionException;
}
