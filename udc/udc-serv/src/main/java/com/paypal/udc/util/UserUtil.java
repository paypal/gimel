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

package com.paypal.udc.util;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.google.gson.Gson;
import com.paypal.udc.dao.user.ElasticSearchUserRepository;
import com.paypal.udc.dao.user.UserRepository;
import com.paypal.udc.entity.user.ElasticSearchUser;
import com.paypal.udc.entity.user.User;
import com.paypal.udc.exception.ValidationError;


@Component
public class UserUtil {
    @Autowired
    private UserRepository userRepository;
    @Autowired
    private ElasticSearchUserRepository esUserRepository;
    @Autowired
    private TransportClient client;

    public User validateUser(final long userId) throws ValidationError {

        final User user = this.userRepository.findById(userId).orElse(null);
        if (user == null) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Approver ID is not valid");
            throw v;
        }
        return user;
    }
    //
    // public User validateUser(final String userName) throws ValidationError {
    //
    // if (userName == null) {
    // final ValidationError v = new ValidationError();
    // v.setErrorCode(HttpStatus.BAD_REQUEST);
    // v.setErrorDescription("Username is not supplied");
    // throw v;
    // }
    // final User user = this.userRepository.findByUserName(userName);
    // if (user == null) {
    // final ValidationError v = new ValidationError();
    // v.setErrorCode(HttpStatus.BAD_REQUEST);
    // v.setErrorDescription("Username is not valid");
    // throw v;
    // }
    // return user;
    // }

    public void upsertUsers(final String esUserIndex, final String esType, final User user,
            final ElasticsearchTemplate esTemplate) throws IOException, InterruptedException, ExecutionException {

        final List<ElasticSearchUser> esUsers = this.esUserRepository.findByUserId(user.getUserId());

        if (esUsers == null || esUsers.isEmpty()) {
            this.save(esUserIndex, esType, user);
            esTemplate.refresh(esUserIndex);
        }
        else {
            this.update(esUserIndex, esType, esUsers.get(0), user);
            esTemplate.refresh(esUserIndex);
        }
    }

    public void save(final String indexName, final String typeName, final User user) {

        final Gson gson = new Gson();

        final ElasticSearchUser esUser = new ElasticSearchUser(user.getUserId(), user.getUserName(),
                user.getUserFullName(), user.getIsActiveYN(), user.getCreatedUser(),
                user.getCreatedTimestamp(), user.getUpdatedUser(), user.getUpdatedTimestamp(), user.getRoles(),
                user.getManagerName(), user.getOrganization(), user.getQid());

        final IndexResponse response = this.client.prepareIndex(indexName, typeName, esUser.get_id())
                .setSource(gson.toJson(esUser), XContentType.JSON)
                .get();
    }

    public void update(final String indexName, final String typeName,
            final ElasticSearchUser esUser,
            final User user) throws IOException, InterruptedException, ExecutionException {

        final Gson gson = new Gson();

        final UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexName).type(typeName)
                .id(esUser.get_id())
                .doc(gson.toJson(user), XContentType.JSON);

        final UpdateResponse updateResponse = this.client.update(updateRequest).get();

    }
}
