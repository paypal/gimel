
## LoggingUtils

It gives you a log4j wrapper to log messages into the log file in JSON format and also sends relevant messages to Kafka, which eventually can be routed to Elastic Search or other sinks via a simple Gimel Consumer.

There are three major types of log API:
   
   1. String log API such as logger.info(String msg), the API is for debug purpose.

   2. Key Value pair log API such as logger.info("key1", valueObj1, "key2", valueObj2...)

   
All types of logs come to local log. 


## Examples for SQL system logging. 
> Note: Following APIs are for SQL internal use only.

### 1. Java Example:
```
    import com.paypal.sql.logging.impl.JSONSystemLogger;

    // Get JSONSystemLogger. JSONSystemLogger is for Gimel system use.
    final JSONSystemLogger systemLogger = JSONSystemLogger.getInstance(AppStatusTest.class);

    // Simple log file logging.
    systemLogger.info("This is an example code");

    // Key-value pair logging, which will be sent to Kafka and log file in json format.
    systemLogger.info("jobExecutionId", 1, "flowExecutionId", 1, "jobId", 4, "flowId", 1, "ruleId", 5,
            "ruleLogLocation", "/logs", "ruleExecutionEndTimeMS", System.currentTimeMillis(), "ruleAction",
            "rule action",
            "ruleStats", "{\"stats\",\"No stats yet\"}", "ruleExecutionStatus", "SUCCESS", "metaUpdate", true);

```

### 2. Scala Example:

```

import scala.math.random
import org.apache.spark._

import com.paypal.sql.logging.impl.JSONSystemLogger;

object SparkWordCount {
  def main(args: Array[String]) {
    
    //Define Logger
    lazy val logger = JSONSystemLogger.getLogger(getClass())
    
    // Simple log file logging.
    logger.info("In main, about to start!!!");

    // Key-value pair logging, which will be sent to Kafka and log file in json format.
    logger.info("jobExecutionId", 1, "flowExecutionId", 1, "jobId", 4, "flowId", 1, "ruleId", 5,
            "ruleLogLocation", "/logs", "ruleExecutionEndTimeMS", System.currentTimeMillis(), "ruleAction", "rule action", "ruleStats", "{\"stats\",\"No stats yet\"}", "ruleExecutionStatus", "SUCCESS", "metaUpdate", true);

  }
}
```

## Enable log auditing in Gimel

- Sending logs related to Data API access and spark listener metrics to Kafka.
- In order to configure log auditing, following steps are required:
    - Enable flag gimel.logging.audit.enabled or spark.gimel.logging.audit.enabled to true/false.
    - Configure the property file by following one of the following steps:
        1. System parameter -> gimel.logger.properties.filepath
            - Add it to spark distributed cache through "spark.files" conf or "--files" option in spark-submit or spark-shell.
            - Mention the file name through system parameter -> gimel.logger.properties.filepath
        2. Resource file
            - Add the file in resources folder in gimel-logging module and rebuild the jar. 
    
Example:

```shell script
spark-shell --jars gimel-tools-2.0.0-SNAPSHOT-uber.jar \
--conf spark.driver.extraJavaOptions="-Dgimel.logger.properties.filepath=gimelLoggerConfig.properties" \
--conf spark.executor.extraJavaOptions="-Dgimel.logger.properties.filepath=gimelLoggerConfig.properties" \
--conf spark.files=/path/to/gimelLoggerConfig.properties \
--conf spark.gimel.logging.audit.enabled=true
```

Here, "/path/to/gimelLoggerConfig.properties" is the path to the property file which can be local or in hdfs.
Example: 
Local -> /home/gimeluser/gimelLoggerConfig.properties
HDFS -> hdfs://namenode:8082/user/gimeluser/gimelLoggerConfig.properties

**Sample gimelLoggerConfig.properties file:**

```text
# kafka
gimel.logger.system.topic=gimel_logging_kafka_topic
gimel.logger.appMetrics.topic=gimel_app_metrics_kafka_topic

# Kafka connection properties.
gimel.logger.kafka.bootstrap.servers = kafka_broker_1:9092,kafka_broker_2:9092
gimel.logger.kafka.key.serializer=com.paypal.shaded.org.apache.kafka.common.serialization.ByteArraySerializer
gimel.logger.kafka.value.serializer=com.paypal.shaded.org.apache.kafka.common.serialization.ByteArraySerializer
gimel.logger.kafka.acks=0
gimel.logger.kafka.retries=3
```

Data API access logs and spark listener metrics will be pushed to the kafka cluster specified in this file.