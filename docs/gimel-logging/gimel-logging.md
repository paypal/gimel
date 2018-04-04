
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

