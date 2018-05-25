

## Python Support

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
gsql = sc._jvm.com.paypal.gimel.scaas.GimelQueryProcessor

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
