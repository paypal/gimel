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

package com.paypal.udc.service.impl;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.paypal.udc.dao.user.UserRepository;
import com.paypal.udc.entity.user.User;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IUserService;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class UserService implements IUserService {

    @Autowired
    private UserRepository userRepository;
    @Autowired
    private UserUtil userUtil;

    @Value("${elasticsearch.user.name}")
    private String esUserIndex;

    @Value("${elasticsearch.type.name}")
    private String esType;

    @Value("${udc.es.write.enabled}")
    private String isEsWriteEnabled;

    @Autowired
    private ElasticsearchTemplate esTemplate;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
    final static Logger logger = LoggerFactory.getLogger(UserService.class);

    @Override
    public User getUserById(final long userId) throws ValidationError {
        final User user = this.userUtil.validateUser(userId);
        return user;
    }

    @Override
    public List<User> getAllUsers() {
        final List<User> users = new ArrayList<User>();
        this.userRepository.findAll().forEach(user -> {
            users.add(user);
        });
        return users;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public User deleteUser(final long userId)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        User user = this.userUtil.validateUser(userId);
        user.setUpdatedTimestamp(sdf.format(timestamp));
        user.setIsActiveYN(ActiveEnumeration.NO.getFlag());
        user = this.userRepository.save(user);
        if (this.isEsWriteEnabled.equals("true")) {
            this.userUtil.upsertUsers(this.esUserIndex, this.esType, user, this.esTemplate);
        }
        return user;

    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public User addUser(final User user) throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        final User insertedUser;
        try {
            user.setCreatedUser(user.getUserName());
            user.setUpdatedUser(user.getUserName());
            user.setCreatedTimestamp(sdf.format(timestamp));
            user.setUpdatedTimestamp(sdf.format(timestamp));
            user.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            insertedUser = this.userRepository.save(user);

        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("User name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("User name is duplicated");
            throw v;
        }
        if (this.isEsWriteEnabled.equals("true")) {
            this.userUtil.upsertUsers(this.esUserIndex, this.esType, user, this.esTemplate);
        }
        return insertedUser;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public User updateUser(final User retrievedUser, final User singleSignOnUser)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        try {
            retrievedUser.setUserFullName(singleSignOnUser.getUserFullName());
            retrievedUser.setManagerName(singleSignOnUser.getManagerName());
            retrievedUser.setQid(singleSignOnUser.getQid());
            retrievedUser.setRoles(singleSignOnUser.getRoles());
            retrievedUser.setOrganization(singleSignOnUser.getOrganization());
            retrievedUser.setLocation(singleSignOnUser.getLocation());
            retrievedUser.setUpdatedTimestamp(sdf.format(timestamp));
            retrievedUser.setCreatedUser(singleSignOnUser.getUserName());
            retrievedUser.setUpdatedUser(singleSignOnUser.getUserName());
            this.userRepository.save(retrievedUser);
        }
        catch (final TransactionSystemException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("User name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("User name is duplicated");
            throw v;
        }
        if (this.isEsWriteEnabled.equals("true")) {
            this.userUtil.upsertUsers(this.esUserIndex, this.esType, retrievedUser, this.esTemplate);
        }
        return retrievedUser;
    }

    @Override
    public User getUserByName(final String userName) {
        return this.userRepository.findByUserName(userName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public User enableUser(final long userId)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        User user = this.userUtil.validateUser(userId);
        user.setUpdatedTimestamp(sdf.format(timestamp));
        user.setIsActiveYN(ActiveEnumeration.YES.getFlag());
        user = this.userRepository.save(user);
        if (this.isEsWriteEnabled.equals("true")) {
            this.userUtil.upsertUsers(this.esUserIndex, this.esType, user, this.esTemplate);
        }
        return user;

    }
}
