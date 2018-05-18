
* [Rest API](#rest-api)
  * [Note](#note)
  * [Create rest Table](#create-rest-table)
  * [Common rest Hive Table](#create-hive-table-pointing-to-rest-table)
  * [Common imports](#common-imports-in-all-rest-api-usages)
  * [Sample API usage](#rest-api-usage)
  * [Sample API usage GSQL](#rest-api-usage-gsql)


--------------------------------------------------------------------------------------------------------------------



# Rest API

## Note

* Experimental API, meaning there is no production use-case on this yet.

--------------------------------------------------------------------------------------------------------------------

## Create Hive Table pointing to Rest table

The following hive table points to a Rest API

```sql
create external table pcatalog.youtube
(payload string )
LOCATION '/tmp/youtube'
TBLPROPERTIES
(
  'gimel.restapi.baseURL' = 'https://www.googleapis.com/youtube'
  ,'gimel.restapi.apiVersion' = 'v3'
  ,'gimel.restapi.accessKey' = 'YOURKEY'
  ,'gimel.restapi.url.pattern' = 'gimel.restapi.pattern.subscriptions'
  ,'gimel.restapi.videoId' = 'F7C0xojv2fE'
  ,'gimel.restapi.channelId' = 'UCXe1qKfGweMKTnmRrMw9yOg'
  ,'gimel.restapi.pattern.subscriptions' = '{gimel.restapi.baseURL}/{gimel.restapi.apiVersion}/subscriptions/?channelId={gimel.restapi.channelId}&part=snippet%2CcontentDetails&key={gimel.restapi.accessKey}'
  ,'gimel.restapi.pattern.channelsById' = '{gimel.restapi.baseURL}/{gimel.restapi.apiVersion}/channels/?id={gimel.restapi.channelId}&part=snippet%2CcontentDetails%2Cstatistics&key={gimel.restapi.accessKey}'
  ,'gimel.restapi.pattern.channelsByUserName' = '{gimel.restapi.baseURL}/{gimel.restapi.apiVersion}/channels?key={gimel.restapi.accessKey}&forUsername={gimel.restapi.userName}&part=id'
  ,'gimel.restapi.pattern.commentsByChannelId' = '{gimel.restapi.baseURL}/{gimel.restapi.apiVersion}/comments/?parentId={gimel.restapi.channelId}&part=snippet&key={gimel.restapi.accessKey}'
  ,'gimel.restapi.pattern.commentThreads' = '{gimel.restapi.baseURL}/{gimel.restapi.apiVersion}/commentThreads/?videoId={gimel.restapi.videoId}&part=snippet%2Creplies&key={gimel.restapi.accessKey}'
)
```

--------------------------------------------------------------------------------------------------------------------

## Catalog Properties

| Property | Mandatory? | Description | Example | Default |
|----------|------------|-------------|------------|-------------------|
| gimel.restapi.parse.payload | N | if set to true, the resulting dataframe will show the json payload parsed into fields | true/false | false |
| gimel.restapi.use.payload | N | if set to true, only payload column from dataframe will be use to write via Post/Put | true/false | false |
| gimel.restapi.url | N | URL will be used to directly read or write without any consideration given to other properties (except above) | complete URL | Empty |

--------------------------------------------------------------------------------------------------------------------



## Common Imports in all Rest API Usages

```scala
import com.paypal.gimel._
import com.paypal.gimel.common.catalog.{DataSetProperties,Field}
val dataset = DataSet(spark);

```


## Rest API Usage

```scala
// Setting catalog provider as user

spark.conf.set("gimel.catalog.provider" , "USER");
spark.conf.set("gimel.logging.level" , "CONSOLE");

// Properties, that can go into either Hive TBLPROPERTIES or as a Map programmatically

val baseDetailsYoutube = Map(
  "gimel.restapi.baseURL" -> "https://www.googleapis.com/youtube"
  , "gimel.restapi.apiVersion" -> "v3"
  , "gimel.restapi.accessKey" -> "YOURKEY"
  , "gimel.restapi.url.pattern" -> "gimel.restapi.pattern.subscriptions"
  , "gimel.restapi.videoId" -> "F7C0xojv2fE"
  , "gimel.restapi.channelId" -> "UCXe1qKfGweMKTnmRrMw9yOg"
  , "gimel.restapi.pattern.subscriptions" -> "{gimel.restapi.baseURL}/{gimel.restapi.apiVersion}/subscriptions/?channelId={gimel.restapi.channelId}&part=snippet%2CcontentDetails&key={gimel.restapi.accessKey}"
  , "gimel.restapi.pattern.channelsById" -> "{gimel.restapi.baseURL}/{gimel.restapi.apiVersion}/channels/?id={gimel.restapi.channelId}&part=snippet%2CcontentDetails%2Cstatistics&key={gimel.restapi.accessKey}"
  , "gimel.restapi.pattern.channelsByUserName" -> "{gimel.restapi.baseURL}/{gimel.restapi.apiVersion}/channels?key={gimel.restapi.accessKey}&forUsername={gimel.restapi.userName}&part=id"
  , "gimel.restapi.pattern.commentsByChannelId" -> "{gimel.restapi.baseURL}/{gimel.restapi.apiVersion}/comments/?parentId={gimel.restapi.channelId}&part=snippet&key={gimel.restapi.accessKey}"
  , "gimel.restapi.pattern.commentThreads" -> "{gimel.restapi.baseURL}/{gimel.restapi.apiVersion}/commentThreads/?videoId={gimel.restapi.videoId}&part=snippet%2Creplies&key={gimel.restapi.accessKey}"
)

// Constructing DataSetProperties object programmatically

val dataSetProperties = DataSetProperties("RESTAPI",Array(),Array(),baseDetailsYoutube)

// Setting dataSetProperties

val props = Map("youtube.dataSetProperties" ->dataSetProperties )

// Data API - Read

val urlData = dataset.read("youtube",  props)

// Without Parsing response PayLoad, a resulting DataFrame with just one column - "payload"

spark.conf.set("gimel.restapi.parse.payload" , "true");
val urlData = dataset.read("youtube",  props)
urlData.printSchema
root
 |-- etag: string (nullable = true)
 |-- items: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- contentDetails: struct (nullable = true)
 |    |    |    |-- activityType: string (nullable = true)
 |    |    |    |-- newItemCount: long (nullable = true)
 |    |    |    |-- totalItemCount: long (nullable = true)
 |    |    |-- etag: string (nullable = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- kind: string (nullable = true)
 |    |    |-- snippet: struct (nullable = true)
 |    |    |    |-- channelId: string (nullable = true)
 |    |    |    |-- description: string (nullable = true)
 |    |    |    |-- publishedAt: string (nullable = true)
 |    |    |    |-- resourceId: struct (nullable = true)
 |    |    |    |    |-- channelId: string (nullable = true)
 |    |    |    |    |-- kind: string (nullable = true)
 |    |    |    |-- thumbnails: struct (nullable = true)
 |    |    |    |    |-- default: struct (nullable = true)
 |    |    |    |    |    |-- url: string (nullable = true)
 |    |    |    |    |-- high: struct (nullable = true)
 |    |    |    |    |    |-- url: string (nullable = true)
 |    |    |    |    |-- medium: struct (nullable = true)
 |    |    |    |    |    |-- url: string (nullable = true)
 |    |    |    |-- title: string (nullable = true)
 |-- kind: string (nullable = true)
 |-- nextPageToken: string (nullable = true)
 |-- pageInfo: struct (nullable = true)
 |    |-- resultsPerPage: long (nullable = true)
 |    |-- totalResults: long (nullable = true)


// With Parsing Payload into DataFrame with fields.

spark.conf.set("gimel.restapi.parse.payload" , "false");
val urlData = dataset.read("youtube",  props)
urlData.printSchema
root
 |-- payload: string (nullable = true)



// Adding additional runtime props as example to showcase overriding options

spark.conf.set("gimel.restapi.url.pattern","gimel.restapi.pattern.channelsById")
spark.conf.set("gimel.restapi.channelId", "UCXe1qKfGweMKTnmRrMw9yOg")
spark.conf.set("gimel.restapi.parse.payload" , "false");
val urlData = dataset.read("youtube",  props)
urlData.collect.foreach(println)

// Override all properties and just set the complete-URL directly

spark.conf.set("gimel.restapi.url","https://www.googleapis.com/youtube/v3/activities/?maxResults=10&channelId=UC_x5XG1OV2P6uZZ5FSM9Ttw&part=snippet%2CcontentDetails&key=AIzaSyBeYqw8TdtDjwnoXQBfxyokhUmyyxGExY0")
val urlData = dataset.read("youtube",  props)
urlData.collect.foreach(println)
```

--------------------------------------------------------------------------------------------------------------------

## Rest API Usage GSQL

```

* GSQL

```scala
val ddl = """
|create external table pcatalog.youtube
|(payload string )
|LOCATION '/tmp/youtube'
|TBLPROPERTIES
|(
|  'gimel.restapi.baseURL' = 'https://www.googleapis.com/youtube'
|  ,'gimel.restapi.apiVersion' = 'v3'
|  ,'gimel.restapi.accessKey' = 'YOURKEY'
|  ,'gimel.restapi.url.pattern' = 'gimel.restapi.pattern.subscriptions'
|  ,'gimel.restapi.videoId' = 'F7C0xojv2fE'
|  ,'gimel.restapi.channelId' = 'UCXe1qKfGweMKTnmRrMw9yOg'
|  ,'gimel.restapi.pattern.subscriptions' = '{gimel.restapi.baseURL}/{gimel.restapi.apiVersion}/subscriptions/?channelId={gimel.restapi.channelId}&part=snippet%2CcontentDetails&key={gimel.restapi.accessKey}'
|  ,'gimel.restapi.pattern.channelsById' = '{gimel.restapi.baseURL}/{gimel.restapi.apiVersion}/channels/?id={gimel.restapi.channelId}&part=snippet%2CcontentDetails%2Cstatistics&key={gimel.restapi.accessKey}'
|  ,'gimel.restapi.pattern.channelsByUserName' = '{gimel.restapi.baseURL}/{gimel.restapi.apiVersion}/channels?key={gimel.restapi.accessKey}&forUsername={gimel.restapi.userName}&part=id'
|  ,'gimel.restapi.pattern.commentsByChannelId' = '{gimel.restapi.baseURL}/{gimel.restapi.apiVersion}/comments/?parentId={gimel.restapi.channelId}&part=snippet&key={gimel.restapi.accessKey}'
|  ,'gimel.restapi.pattern.commentThreads' = '{gimel.restapi.baseURL}/{gimel.restapi.apiVersion}/commentThreads/?videoId={gimel.restapi.videoId}&part=snippet%2Creplies&key={gimel.restapi.accessKey}'
|)
|"""

val gsql = com.paypal.gimel.sql.GimelQueryProcessor.executeBatch(_:String,spark)
gsql: String => org.apache.spark.sql.DataFrame = <function1>

// Create DDL
gsql(ddl)

// Set Catalog Provider Hive
gsql("set gimel.catalog.provider=HIVE")
gsql("select * from pcatalog.youtube")
```

--------------------------------------------------------------------------------------------------------------------

