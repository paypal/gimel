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

import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.paypal.gimel.logging.Constants;
import com.paypal.gimel.logging.LogProvider;
import com.paypal.gimel.logging.GimelLogger;
import com.paypal.gimel.logging.utils.Context;

/**
 * Contains the basic logger functionalities define in {@link GimelLogger}
 *
 */
public class BaseLogger implements GimelLogger {

  protected LogProvider logProvider;
  private JsonFormat protoBufFormat;
  private final Context context = new Context();
  protected final ObjectMapper jsonizer = new ObjectMapper();

  private final Logger logger = LogManager.getLogger(this.getClass().toString());

  protected BaseLogger(String className) {
    this.init(className);
    this.jsonizer.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  @Override
  public void logCustomMetrics(Object... args) {
    if (args == null) {
      return;
    }
    if (args.length == 1 && args[0] instanceof String) {
      logger.info(args[0]);
      return;
    }
    final ObjectNode js = this.encodeArgs(args);
    this.publishToKafka(Constants.TopicType.APPLICATION, js.toString());
  }

  @Override
  public void initCustomMetricsProperties(Properties props) {
    this.logProvider.initCustomMetrics(props);
  }

  @Override
  public void initCustomMetricsProperties(Properties props, String topic) {
    this.logProvider.initCustomMetrics(props, topic);
  }


  @SuppressWarnings("deprecation")
  protected ObjectNode encodeArgs(final Object[] args) {
    final ObjectNode result = this.jsonizer.getNodeFactory().objectNode();
    if (args.length == 1 && args[0] instanceof Map) {
      return this.jsonizer.valueToTree(args[0]);
    }
    // arguments are alternating name and value; if there's an
    // odd number of arguments, it's a value for field "data"
    for (int i = 0; i < args.length; i += 2) {
      try {
        if (i + 1 < args.length) {
          result.put(args[i].toString(), this.jsonizer.valueToTree(args[i + 1]));
        } else {
          result.put("data", this.jsonizer.valueToTree(args[i]));
        }
      } catch (final Exception jse) {
        jse.printStackTrace();
        // ignore any exceptions and keep marching
      }
    }
    return result;
  }

  private void init(String classname) {

    this.logProvider = new LogProvider(classname);
    this.protoBufFormat = new JsonFormat();
  }


  protected void publishToKafka(Constants.TopicType topicType, String value) {
    String currentTopic = this.logProvider.getTopicName(topicType);
    KafkaProducer<byte[], byte[]> kafka = this.logProvider.getKafkaProducer(topicType);

    // if kafka producer is not created, ignore the error and march
    // TODO: Needs to be handled better way. But for now we decided to ignore and not fail
    //  applications.
    if (kafka == null) {
      logger.error("Unable to send metrics to kafka. Printing the metrics to console.");
      System.out.println(value);
      return;
    }
    kafka.send(new ProducerRecord(currentTopic, value.getBytes()), new Callback() {
      @Override
      public void onCompletion(final RecordMetadata metadata, final Exception e) {
        if (e != null) {
          logger.error("Unable to write to Kafka in appender [ " + metadata + "]", e);
        }
      }
    });

  }

  /**
   * Converts {@link Message} to json string
   *
   * @param message
   * @return json string of the given message
   */
  protected String messageToString(final Message message) {
    String json = null;
    json = this.protoBufFormat.printToString(message);
    return json;
  }

}