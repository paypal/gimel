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

package com.paypal.udc.convertor;

import java.io.IOException;
import java.util.List;
import javax.persistence.AttributeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class ListStringJsonConverter implements AttributeConverter<List<String>, String> {

    final static Logger logger = LoggerFactory.getLogger(ListStringJsonConverter.class);
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String convertToDatabaseColumn(final List<String> hostProperties) {
        try {
            return this.objectMapper.writeValueAsString(hostProperties);
        }
        catch (final JsonProcessingException e) {
            logger.error("coudln't convert HostProperties Dto to json, wont persist");
            return null;
        }
    }

    @Override
    public List<String> convertToEntityAttribute(final String data) {

        try {
            if (data != null) {
                return this.objectMapper.readValue(data,
                        this.objectMapper.getTypeFactory().constructCollectionType(List.class, String.class));
            }
        }
        catch (final IOException e) {
            logger.error("couldn't convert json of properties to Dto", e);
        }
        return null;
    }
}
