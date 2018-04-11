
* [Bootstrap Data](#bootstrap-data)
   * [Create Elastic Dataset](#create-elastic-dataset)

# Bootstrap Data

### Create Elastic Dataset
<table>
  <tbody>
    <tr>
      <th align="center">Catalog Provider</th>
      <th align="center">Command</th>
    </tr>
    <tr>
      <td align="center">USER</td>
      <td align="left">
      
      gsql("""set pcatalog.gimel_flights_elastic.dataSetProperties=
      {
          "datasetType": "ELASTIC_SEARCH",
          "fields": [],
          "partitionFields": [],
          "props": {
                  "es.mapping.date.rich":"true",
                  "es.nodes":"http://elasticsearch",
                  "es.port":"9200",
                  "es.resource":"flights/data",
                  "es.index.auto.create":"true",
                  "gimel.es.index.partition.delimiter":"-",
                  "gimel.es.index.partition.isEnabled":"true",
                  "gimel.es.index.read.all.partitions.isEnabled":"true",
                  "gimel.es.index.partition.suffix":"20180205",
                  "gimel.storage.type":"ELASTIC_SEARCH",
                  "datasetName":"pcatalog.gimel_flights_elastic"
          }
      }
      """)
     
   </td>
   </tr>
   <tr>
         <td align="center">HIVE</td>
         <td align="left">
      
      drop table if exists pcatalog.gimel_flights_elastic;   
      CREATE EXTERNAL TABLE `pcatalog.gimel_flights_elastic`(
      `payload` string
      )
      ROW FORMAT SERDE
      'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
      STORED AS INPUTFORMAT
      'org.apache.hadoop.mapred.TextInputFormat'
      OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
      TBLPROPERTIES(
      "es.mapping.date.rich"="true",
        "es.nodes"="http://elasticsearch",
        "es.port"="9200",
        "es.resource"="flights/data",
        "es.index.auto.create"="true",
        "gimel.es.index.partition.delimiter"="-",
        "gimel.es.index.partition.isEnabled"="true",
        "gimel.es.index.read.all.partitions.isEnabled"="true",
        "gimel.es.index.partition.suffix"="20180205",
        "gimel.storage.type"="ELASTIC_SEARCH");
        
   </td>
   </tr>
  </tbody>
</table>
_____________________________________________________