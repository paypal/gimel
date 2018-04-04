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

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.googlecode.protobuf.format.JsonFormat;
import com.paypal.gimel.logging.LogProvider;


/**
 */
public class TestHelper {

    private static JsonFormat jsonFormat;

    static {
        jsonFormat = new JsonFormat();
    }

    /**
     * Currently mocking for a private member of a class is not available. This method will use reflections to set the
     * mocked member (Technically hack it).
     *
     * @param logger
     * @param logProvider
     */
    public static void setLogProvider(final SystemLoggerMocked logger, final LogProvider logProvider) {
        if (logger == null) {
            return;
        }
        try {
            final Field field = logger.getClass().getDeclaredField("logProvider");
            field.setAccessible(true);
            field.set(logger, logProvider);
        }
        catch (final Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Converts given message to Proto Buf message.
     *
     * @param message
     * @return protoBufMessage if given message is parsable; null otherwise
     */
    public static Message getProtobufMessage(final String message, final Class<? extends Message> protoMsg) {

        try {
            if (protoMsg == null) {
                return null;
            }
            final Method method = protoMsg.getMethod("newBuilder", null);
            final Object builder = method.invoke(null);
            TestHelper.jsonFormat.merge(new ByteArrayInputStream(message.getBytes()), (Builder) builder);
            System.out.println(builder.getClass());
            final Method buildMethod = builder.getClass().getMethod("build", null);
            final Object obj = buildMethod.invoke(builder, null);
            return (Message) obj;
        }
        catch (final Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Properties getLog4jConfig() {
        final Properties props = new Properties();
        props.put("log4j.rootLogger", "INFO");
        props.put("log4j.logger.testKafka", "DEBUG, KAFKA");
        props.put("log4j.appender.KAFKA.layout", "org.apache.log4j.PatternLayout");
        props.put("log4j.appender.KAFKA.layout.ConversionPattern", "%-5p: %c - %m%n");
        props.put("log4j.appender.KAFKA.BrokerList", "localhost:9093");
        props.put("log4j.appender.KAFKA.Topic", "test-topic");
        props.put("log4j.appender.KAFKA.RequiredNumAcks", "1");
        props.put("log4j.appender.KAFKA.SyncSend", "false");
        return props;
    }

}
