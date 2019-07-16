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

package com.paypal.udc.util.enumeration;

//To identify changes on the Timeline, show different timeline dimensions with colors mentioned in Enum for the specific dimension

public enum TimelineEnumeration {

    DATASET("DATASET", "Dataset Name", "#ec734ded"),
    TAG("TAG", "Tags", "#e0a333"),
    SCHEMA("SCHEMA", "Schema", "#6576d0"),
    OBJECT_DESC("OBJECT_DESC", "Dataset Description", "#9a4197"),
    COLUMN_DESC("COLUMN_DESC", "Dataset Column Description", "#629e36"),
    OWNERSHIP("OWNERSHIP", "Ownership", "#8f9e1bd4"),
    CLASSIFICATION("CLASSIFICATION", "Classification", "#d88b17"),
    OBJECT_ATTRIBUTES("OBJECT_ATTRIBUTES", "Object Attributes", "#0391a2");

    private String flag;
    private final String description;
    private String color;

    TimelineEnumeration(final String flag, final String description, final String color) {

        this.flag = flag;
        this.description = description;
        this.color = color;
    }

    public String getFlag() {
        return this.flag;
    }

    public void setFlag(final String flag) {
        this.flag = flag;
    }

    public String getColor() {
        return this.color;
    }

    public void setColor(final String color) {
        this.color = color;
    }

    public String getDescription() {
        return this.description;
    }

}
