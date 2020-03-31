

## PySpark / Python Support

- Gimel Data API is fully compatible with pyspark, although the library itself is built in Scala.
- Pyspark provides an extremely powerful feature to tap into the JVM, and thus get a reference to all Java/Scala classes/objects in the JVM.
- This mean, by adding the JAR (Gimel Library) at runtime - you may leverage all the features of Gimel.
- Please see below illustration of how you may develop on PySpark, but still leverage Gimel Data API or Gimel SQL in their entirety.

## Launch PySpark

While you launch pyspark - add the Gimel Jar.

```bash
export SPARK_MAJOR_VERSION=2
pyspark --jars hdfs://gimel_jar
# or
pyspark --jars file://gimel_jar
```

## Using Gimel Data API in PySpark

```python
# import DataFrame and SparkSession
from pyspark.sql import DataFrame, SparkSession

# fetch reference to the class in JVM
ScalaDataSet = sc._jvm.com.paypal.gimel.DataSet

# fetch reference to java SparkSession
jspark = spark._jsparkSession

# initiate dataset
dataset = ScalaDataSet.apply(jspark)

# invoking the read API gives reference to the scala DataFrame result-set
scala_df = dataset.read("pcatalog.your_dataset","")

# passing options - an example
scala_df_with_options = dataset.read("pcatalog.your_dataset","gimel.kafka.throttle.batch.fetchRowsOnFirstRun=1")

# convert to pyspark dataframe
python_df = DataFrame(scala_df,jspark)

# from now - you may use regular pyspark lingua to play with the data
python_df.show(10)
```

## Using Gimel SQL in PySpark

```python
# fetch reference to GimelQueryProcessor Class in JVM
gsql = sc._jvm.com.paypal.gimel.sql.GimelQueryProcessor

# fetch reference to java SparkSession
jspark = spark._jsparkSession

# your SQL
sql = "select * from pcatalog.your_dataset limit 5"

# execute GSQL, this can be any sql of type "insert into ... select .. join ... where .."
gsql.executeBatch(sql, jspark)

# execute GSQL, and get reference to resulting dataset of the SQL
scala_df = gsql.executeBatch(sql, jspark)

# convert to pyspark dataframe
python_df = DataFrame(scala_df, jspark)

# from now - you may use regular pyspark lingua to play with the data
python_df.show(10)
```

## Sample Read and Write illustration

```python
# DataSet
dataset = ScalaDataSet.apply(jspark)

# Read from HDFS
hdfs_data = DataFrame(dataset.read("pcatalog.flights_hdfs",""),jspark)

# Illustration Count
hdfs_data.count()
# 1398164

# Read from Kafka
kafka_data = DataFrame(dataset.read("pcatalog.flights_kafka",""),jspark)

# Illustration Count
kafka_data.count()
# 0

# Write to Kafka
dataset.write("pcatalog.flights_kafka",hdfs_data._jdf,"")

# Read from Kafka post-write
kafka_data = DataFrame(dataset.read("pcatalog.flights_kafka",""),jspark)

# Illustration Count
kafka_data.count()
# 1398164

# Sample Data
kafka_data.show(3)

#+-------------------+----------+--------+-----------------+---------+-------+-------------+----------------+----+--------------+--------+--------------+--------+-------+--------------------+------+-------------------+---------+------+----------------+--------------+--------+--------------+-------------+-----+----+
#|ACTUAL_ELAPSED_TIME|AIRLINE_ID|AIR_TIME|CANCELLATION_CODE|CANCELLED|CARRIER|CARRIER_DELAY|CRS_ELAPSED_TIME|DEST|DEST_CITY_NAME|DISTANCE|DISTANCE_GROUP|DIVERTED|FLIGHTS|             FL_DATE|FL_NUM|LATE_AIRCRAFT_DELAY|NAS_DELAY|ORIGIN|ORIGIN_CITY_NAME|SECURITY_DELAY|TAIL_NUM|UNIQUE_CARRIER|WEATHER_DELAY|month|year|
#+-------------------+----------+--------+-----------------+---------+-------+-------------+----------------+----+--------------+--------+--------------+--------+-------+--------------------+------+-------------------+---------+------+----------------+--------------+--------+--------------+-------------+-----+----+
#|               68.0|     20304|    39.0|             null|      0.0|     OO|         null|            62.0| ORD|   Chicago, IL|   157.0|             1|     0.0|    1.0|2017-10-01T00:00:...|  2936|               null|     null|   FWA|  Fort Wayne, IN|          null|  N464SW|            OO|         null|   10|2017|
#|               63.0|     20304|    35.0|             null|      0.0|     OO|         null|            60.0| ORD|   Chicago, IL|   137.0|             1|     0.0|    1.0|2017-10-01T00:00:...|  2940|               null|     null|   GRR|Grand Rapids, MI|          null|  N727SK|            OO|         null|   10|2017|
#|               65.0|     20304|    42.0|             null|      0.0|     OO|         null|            72.0| ALO|  Waterloo, IA|   234.0|             1|     0.0|    1.0|2017-10-01T00:00:...|  2942|               null|     null|   ORD|     Chicago, IL|          null|  N423SW|            OO|         null|   10|2017|
#+-------------------+----------+--------+-----------------+---------+-------+-------------+----------------+----+--------------+--------+--------------+--------+-------+--------------------+------+-------------------+---------+------+----------------+--------------+--------+--------------+-------------+-----+----+
```
