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

package com.paypal.gimel.logging;

public class Constants {

  // Topics in Kafka
  public final static String GIMEL_LOGGER_PROPERTY_PREFIX = "gimel.logger.kafka";
  public final static String GIMEL_LOGGER_SYSTEM_TOPIC_KEY = "gimel.logger.system.topic";
  public final static String GIMEL_LOGGER_APPMETRICS_TOPIC_KEY = "gimel.logger.appMetrics.topic";
  public final static String GIMEL_LOGGER_PROPERTIES_FILEPATH_KEY = "gimel.logger.properties.filepath";
  // The messages types which shall be passed into google proto as the object type
  // So deserializer can know the type of message
  public enum MessageType {
    SYSTEM(6);

    int messageId;

    MessageType(final int messageName) {
      this.messageId = messageName;
    }

    public int getValue() {
      return this.messageId;
    }
  }
  public enum TopicType {
    SYSTEM("gimel.logger.system.topic"),
    APPLICATION("gimel.logger.appMetrics.topic");

    String messageName;

    TopicType(final String messageName) {
      this.messageName = messageName;
    }

    @Override
    public String toString() {
      return this.messageName;
    }
  }
}
