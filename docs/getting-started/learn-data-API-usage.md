
# Data API Usage

Quick Starter for DataSet and DataStream APIs.
Please refer individual storage system documentation for details.

```scala

import com.paypal.gimel._
import org.apache.spark.sql._
import scala.collection.immutable.Map

// Initiate DataSet
val sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
val dataSet = DataSet(sparkSession)

// Read Data
val readOptions = Map[String,Any]()
val data1 : DataFrame = dataSet.read("pcatalog.table1",readOptions)
val data2 : DataFrame = dataSet.read("pcatalog.table2")

// Write Data
val writeOptions = Map[String,Any]()
dataSet.write("pcatalog.table3",data1,writeOptions)
dataSet.write("pcatalog.table4",data2)

// Initiate DataStream
val dataStream = DataStream(sparkSession)

// Get Reference to Stream
  val streamingResult: StreamingResult = dataStream.read(datasetName)
  // Clear CheckPoint if necessary
  streamingResult.clearCheckPoint("some message")
  // Helper for Clients
  streamingResult.dStream.foreachRDD { rdd =>
    val count = rdd.count()
    if (count > 0) {
      /**
        * Mandatory | Get Offset for Current Window, so we can checkpoint at the end of this window's operation
        */
      streamingResult.getCurrentCheckPoint(rdd)
      /**
        * Begin | User's Usecases
        */
      // dataSet.write("pcatalog.targetDataSet",derivedDF)
      streamingResult.saveCurrentCheckPoint()
    }
  }
  // Start the Context
  dataStream.streamingContext.start()
  dataStream.streamingContext.awaitTermination()
```