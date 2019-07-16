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
import com.paypal.udc.entity.entity.Entity;
import com.paypal.udc.exception.ValidationError;


public interface IEntityService {

    Entity getEntityById(long tenantId) throws ValidationError;

    Entity addEntity(Entity entity) throws ValidationError, IOException, InterruptedException, ExecutionException;

    Entity getEntityByName(String tenantName);

    List<Entity> getAllEntities();

    Entity updateEntity(Entity entity) throws ValidationError, IOException, InterruptedException, ExecutionException;

    Entity deActivateEntity(long entityId)
            throws ValidationError, IOException, InterruptedException, ExecutionException;

    Entity reActivateEntity(long entityId)
            throws ValidationError, IOException, InterruptedException, ExecutionException;

}
