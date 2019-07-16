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

package com.paypal.udc.entity.integration.schema;

import java.util.List;


public class SchemaDatasetMapInput {

    private String db_server;
    private String model_name;
    private String object_type;
    private String name;
    private String comment;
    private String schema_name;
    private List<SchemaDatasetColumnMapInput> columns;
    private String createdUser;
    private String providerName;

    public SchemaDatasetMapInput() {

    }

    public SchemaDatasetMapInput(final String db_server, final String model_name, final String object_type,
            final String name, final String comment, final String schema_name, final String createdUser,
            final List<SchemaDatasetColumnMapInput> columns, final String providerName) {
        this.db_server = db_server;
        this.model_name = model_name;
        this.object_type = object_type;
        this.name = name;
        this.comment = comment;
        this.schema_name = schema_name;
        this.columns = columns;
        this.createdUser = createdUser;
        this.providerName = providerName;
    }

    public String getProviderName() {
        return this.providerName;
    }

    public void setProviderName(final String providerName) {
        this.providerName = providerName;
    }

    public String getCreatedUser() {
        return this.createdUser;
    }

    public void setCreatedUser(final String createdUser) {
        this.createdUser = createdUser;
    }

    public List<SchemaDatasetColumnMapInput> getColumns() {
        return this.columns;
    }

    public void setColumns(final List<SchemaDatasetColumnMapInput> columns) {
        this.columns = columns;
    }

    public String getDb_server() {
        return this.db_server;
    }

    public void setDb_server(final String db_server) {
        this.db_server = db_server;
    }

    public String getModel_name() {
        return this.model_name;
    }

    public void setModel_name(final String model_name) {
        this.model_name = model_name;
    }

    public String getObject_type() {
        return this.object_type;
    }

    public void setObject_type(final String object_type) {
        this.object_type = object_type;
    }

    public String getName() {
        return this.name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getComment() {
        return this.comment;
    }

    public void setComment(final String comment) {
        this.comment = comment;
    }

    public String getSchema_name() {
        return this.schema_name;
    }

    public void setSchema_name(final String schema_name) {
        this.schema_name = schema_name;
    }

}
