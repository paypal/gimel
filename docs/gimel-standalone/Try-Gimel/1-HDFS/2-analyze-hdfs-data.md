
* [G-SQL](#g--sql)
    * [Read Data from HDFS](#read-data-from-hdfs)
    * [Count Records from HDFS](#count-records-from-hdfs)
* [Scala API](#scala-api)
    * [Read Data from HDFS](#read-data-from-hdfs)
    * [Count Records from HDFS](#count-records-from-hdfs)
   
# G-SQL

## Read Data from HDFS
```
gsql("select * from pcatalog.flights_hdfs").show()
```

## Count Records From HDFS
```
gsql("select * from pcatalog.flights_hdfs").count()
```
______________________________________________________

# Scala API

## Read Data from HDFS
```
import com.paypal.gimel._
val dataSet = DataSet(spark)
val df = dataSet.read("pcatalog.flights_hdfs")
df.count
```

## Count Records From HDFS
```
import com.paypal.gimel._
val dataSet = DataSet(spark)
val df = dataSet.read("pcatalog.flights_hdfs")
df.count
```
_________________________________________________



