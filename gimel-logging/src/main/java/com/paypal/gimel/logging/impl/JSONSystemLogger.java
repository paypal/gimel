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

package com.paypal.gimel.logging.impl;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.paypal.gimel.logging.Constants;
import com.paypal.gimel.logging.SystemLogger;
import com.paypal.gimel.logging.utils.Context;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Logs messages in JSON format and sends relevant messages to Kafka.
 *
 */
public class JSONSystemLogger extends BaseLogger implements SystemLogger {

    private static JSONSystemLogger instance;

    private final Logger logger = LogManager.getLogger(this.getClass().toString());

    protected JSONSystemLogger(String className) {
        super(className);
    }

    /**
     * Gives instance of {@code Context}
     * <p>
     * Note: {@code ipAddress} and {@code hostname} will be initialized with the current system details. Those values
     * </p>
     *
     * @return {@code Context} instance.
     */
    public static JSONSystemLogger getInstance(Class<?> clazz) {
        if (instance == null) {
            synchronized (Context.class) {
                if (instance == null) {
                    instance = new JSONSystemLogger(clazz.getName());
                }
            }
        }
        return instance;
    }


    /**
     * Converts the arguments to JSON format and logs to log file and sends to kafka.
     *
     * @param args
     */
    @Override
    public void debug(final Object... args) {
        if (args == null) {
            return;
        }
        if (args.length == 1 && args[0] instanceof String) {
            logger.debug(args[0]);
            return;
        }

        final ObjectNode js = this.encodeArgs(args);
        this.publishToKafka(Constants.TopicType.SYSTEM, js.toString());
        logger.debug(js.toString());
    }

    /**
     * Converts the arguments to JSON format and logs to log file and sends to kafka.
     *
     * @param args
     */
    @Override
    public void info(final Object... args) {
        if (args == null) {
            return;
        }
        if (args.length == 1 && args[0] instanceof String) {
            logger.info(args[0]);
            return;
        }
        final ObjectNode js = this.encodeArgs(args);
        this.publishToKafka(Constants.TopicType.SYSTEM, js.toString());
        logger.info(js.toString());
    }

    /**
     * Converts the arguments to JSON format and logs to log file and sends to kafka.
     *
     * @param args
     */
    @Override
    public void warn(final Object... args) {
        if (args == null) {
            return;
        }
        if (args.length == 1 && args[0] instanceof String) {
            logger.warn(args[0]);
            return;
        }
        final ObjectNode js = this.encodeArgs(args);
        this.publishToKafka(Constants.TopicType.SYSTEM, js.toString());
        this.logger.warn(js.toString());
    }

    @Override
    public void error(final String message) {
        this.publishToKafka(Constants.TopicType.SYSTEM, message);
    }

}
