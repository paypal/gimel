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

package com.paypal.udc.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;


public class ValidationError extends Exception {

    private static final long serialVersionUID = -5166164364710079372L;
    final static Logger logger = LoggerFactory.getLogger(ValidationError.class);
    HttpStatus errorCode;
    String errorDescription;

    public ValidationError() {
        this.errorCode = HttpStatus.OK;
        this.errorDescription = "";
    }

    public HttpStatus getErrorCode() {
        return this.errorCode;
    }

    public void setErrorCode(final HttpStatus errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorDescription() {
        return this.errorDescription;
    }

    public void setErrorDescription(final String errorDescription) {
        this.errorDescription = errorDescription;
    }

    public static Error getError(final String error_log) {

        logger.error(error_log);
        final Error error = new Error(error_log);
        return error;
    }

}
