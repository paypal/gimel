/*
 * Copyright 2018 PayPal Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.paypal.gimel.logging.message;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.paypal.gimel.logging.impl.JSONSystemLogger;


/**
 */
public class SystemLoggerMocked extends JSONSystemLogger {

    public SystemLoggerMocked(final String className) {
        super(className);
    }

    public String toJson(final Object... args) {
        final ObjectNode js = this.encodeArgs(args);
        return js.toString();
    }

}
