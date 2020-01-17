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

import java.util.Enumeration;
import java.util.Properties;

import com.paypal.gimel.logging.utils.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * Provides logger for different types of messages.
 *
 */
public class LogProvider {

  private KafkaProducer<byte[], byte[]> kafkaProducer;
  private KafkaProducer<byte[], byte[]> customKafkaProducer;
  private Configuration config = Configuration.getInstance();
  private final Properties kafkaProps = config.getKafkaProperties();
  private final Properties topics = config.getKafkaTopics();


  public String getTopicName(Constants.TopicType topicType) {
    return topics.get(topicType.toString()).toString();
  }


  public LogProvider(final String className) {
  }

  /**
   * Returns a logger for the given type of {@linkplain Constants.TopicType}.
   *
   * @param topicType
   * @return logger instance
   */
  public KafkaProducer getKafkaProducer(final Constants.TopicType topicType) {
    return this.getKafkaProducer(topicType, this.kafkaProps);
  }

  public KafkaProducer getKafkaProducer(final Constants.TopicType topicType, final Properties kafkaProps) {
    switch (topicType) {
      case APPLICATION:
        if (this.customKafkaProducer == null) {
          this.customKafkaProducer = new KafkaProducer<byte[], byte[]>(kafkaProps);
        }
        return this.customKafkaProducer;
      default: {
        if (kafkaProducer == null) {
          kafkaProducer = new KafkaProducer<byte[], byte[]>(config.getKafkaProperties());
        }
        return this.kafkaProducer;
      }

    }
  }

  public KafkaProducer getDefaultLogger() {
    return this.kafkaProducer;
  }

  public KafkaProducer getSystemLogger() {
    return this.getKafkaProducer(Constants.TopicType.SYSTEM);
  }


  public KafkaProducer getApplicationLogger() {
    return this.getKafkaProducer(Constants.TopicType.APPLICATION);
  }

  public void initCustomMetrics(Properties customProps) {

    for (Enumeration propertyNames = customProps.propertyNames();
         propertyNames.hasMoreElements(); ) {
      Object key = propertyNames.nextElement();
      if (key.toString().indexOf(Constants.GIMEL_LOGGER_PROPERTY_PREFIX) != -1)
        kafkaProps.put(key, customProps.get(key));

    }
  }

  public void initCustomMetrics(Properties customProps, String topic) {
    initCustomMetrics(customProps);
    topics.put(Constants.GIMEL_LOGGER_APPMETRICS_TOPIC_KEY, topic);
  }
}
