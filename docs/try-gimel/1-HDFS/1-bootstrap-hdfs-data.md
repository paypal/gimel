* [Note](#note)
* [Setting Catalog Provider](#setting-catalog-provider)
* [Bootstrap Data](#bootstrap-data)
   * [Bootstrap Flights Data](#bootstrap-flights-data) 
      * [Create HDFS Datasets for loading Flights Data](#create-hdfs-dataset-for-loading-flights-data)
   * [Bootstrap Flights Lookup Data](#bootstrap-flights-lookup-data)
      * [Create HDFS Datasets for loading Flights Lookup Data](#create-hdfs-datasets-for-loading-flights-lookup-data)

# Note 

*The ```quickstart/start-gimel``` by default creates all the hive DDLs which you are seeing below. 
If you want to build a custom dataset hive table, these can be used as a reference.*

*Also if you want to see the tables which got created. Please execute the following command in a new terminal.*
```
docker exec -it hive-server bash -c 'hive -e "show tables in  pcatalog"'
```


# Setting Catalog Provider
In this step, developer/user can choose a catalog provider. The default option is HIVE.



| Catalog Provider | Command | Notes |
| ---------------- | -------- | -------- |
| USER | ```gsql("set gimel.catalog.provider=USER")``` | To override the default option we need to use the option|
| HIVE | ```gsql("set gimel.catalog.provider=HIVE")``` | This is the default option provided to the users |

# Bootstrap Data

## Bootstrap Flights Data

### Create HDFS Dataset for loading Flights Data
<table>
  <tbody>
    <tr>
      <th align="center">Catalog Provider</th>
      <th align="center">Command</th>
    </tr>
    <tr>
      <td align="center">USER</td>
      <td align="left">
      
      gsql("""set pcatalog.flights_hdfs.dataSetProperties={ 
          "datasetType": "HDFS",
          "fields": [],
          "partitionFields": [],
          "props": {
               "gimel.hdfs.data.format":"csv",
               "location":"hdfs://namenode:8020/flights/data",
               "datasetName":"pcatalog.flights_hdfs"
          }
      }""")
     
   </td>
   </tr>
   <tr>
         <td align="center">HIVE</td>
         <td align="left">
         
    drop table if exists pcatalog.flights_hdfs;
    CREATE external TABLE if not exists pcatalog.flights_hdfs(
        payload string)
    PARTITIONED BY (year string, month string)
    LOCATION 'hdfs://namenode:8020/flights/data'
    TBLPROPERTIES (
        'gimel.storage.type'='HDFS',
        'gimel.hdfs.data.format'='csv'
    );
        
   </td>
   </tr>
  </tbody>
</table>

___________________________________________________________________________________________________________________

## Bootstrap Flights Lookup Data

### Create HDFS Datasets for loading Flights Lookup Data
<table>
  <tbody>
    <tr>
      <th align="center">Catalog Provider</th>
      <th align="center">Command</th>
    </tr>
    <tr>
      <td align="center">USER</td>
      <td align="left">
      
      gsql("""set pcatalog.flights_lookup_carrier_code_hdfs.dataSetProperties={
          "datasetType": "HDFS",
          "fields": [],
          "partitionFields": [],
          "props": {
               "gimel.hdfs.data.format":"csv",
               "location":"hdfs://namenode:8020/flights/lkp/carrier_code",
               "datasetName":"pcatalog.flights_lookup_carrier_code_hdfs"
          }
      }""")
     
   </td>
   </tr>
   <tr>
         <td align="center">HIVE</td>
         <td align="left">
         
      drop table if exists pcatalog.flights_lookup_carrier_code_hdfs;
      CREATE external TABLE if not exists pcatalog.flights_lookup_carrier_code_hdfs(
        payload string)
      LOCATION 'hdfs://namenode:8020/flights/lkp/carrier_code'
      TBLPROPERTIES (
        'gimel.storage.type'='HDFS',
        'gimel.hdfs.data.format'='csv'
      );
        
   </td>
   </tr>
  </tbody>
</table>

___________________________________________________________________________________________________________________
<table>
  <tbody>
    <tr>
      <th align="center">Catalog Provider</th>
      <th align="center">Command</th>
    </tr>
    <tr>
      <td align="center">USER</td>
      <td align="left">
     
      gsql("""set pcatalog.flights_lookup_airline_id_hdfs.dataSetProperties={
          "datasetType": "HDFS",
          "fields": [],
          "partitionFields": [],
          "props": {
               "gimel.hdfs.data.format":"csv",
               "location":"hdfs://namenode:8020/flights/lkp/airline_id",
               "datasetName":"pcatalog.flights_lookup_airline_id_hdfs"
          }
      }""")
     
   </td>
   </tr>
   <tr>
         <td align="center">HIVE</td>
         <td align="left">
      
      drop table if exists pcatalog.flights_lookup_airline_id_hdfs;   
      CREATE external TABLE if not exists pcatalog.flights_lookup_airline_id_hdfs(
        payload string)
      LOCATION 'hdfs://namenode:8020/flights/lkp/airline_id'
      TBLPROPERTIES (
        'gimel.storage.type'='HDFS',
        'gimel.hdfs.data.format'='csv'
      );
        
   </td>
   </tr>
  </tbody>
</table>

___________________________________________________________________________________________________________________
<table>
  <tbody>
    <tr>
      <th align="center">Catalog Provider</th>
      <th align="center">Command</th>
    </tr>
    <tr>
      <td align="center">USER</td>
      <td align="left">
      
      gsql("""set pcatalog.flights_lookup_cancellation_code_hdfs.dataSetProperties={
          "datasetType": "HDFS",
          "fields": [],
          "partitionFields": [],
          "props": {
               "gimel.hdfs.data.format":"csv",
               "location":"hdfs://namenode:8020/flights/lkp/cancellation_code",
               "datasetName":"pcatalog.flights_lookup_cancellation_code_hdfs"
          }
      }""")
     
   </td>
   </tr>
   <tr>
         <td align="center">HIVE</td>
         <td align="left">
      
      drop table if exists pcatalog.flights_lookup_cancellation_code_hdfs;   
      CREATE external TABLE if not exists pcatalog.flights_lookup_cancellation_code_hdfs(
        payload string)
      LOCATION 'hdfs://namenode:8020/flights/lkp/cancellation_code'
      TBLPROPERTIES (
        'gimel.storage.type'='HDFS',
        'gimel.hdfs.data.format'='csv'
      );
        
   </td>
   </tr>
  </tbody>
</table>

___________________________________________________________________________________________________________________
<table>
  <tbody>
    <tr>
      <th align="center">Catalog Provider</th>
      <th align="center">Command</th>
    </tr>
    <tr>
      <td align="center">USER</td>
      <td align="left">
      
      gsql("""set pcatalog.flights_lookup_airports_hdfs.dataSetProperties={
          "datasetType": "HDFS",
          "fields": [],
          "partitionFields": [],
          "props": {
               "gimel.hdfs.data.format":"csv",
               "location":"hdfs://namenode:8020/flights/lkp/airports",
               "datasetName":"pcatalog.flights_lookup_airports_hdfs"
          }
      }""")
     
   </td>
   </tr>
   <tr>
         <td align="center">HIVE</td>
         <td align="left">
      
      drop table if exists pcatalog.flights_lookup_airports_hdfs;  
      CREATE external TABLE if not exists pcatalog.flights_lookup_airports_hdfs(
        payload string)
      LOCATION 'hdfs://namenode:8020/flights/lkp/airports'
      TBLPROPERTIES (
        'gimel.storage.type'='HDFS',
        'gimel.hdfs.data.format'='csv'
      );
        
   </td>
   </tr>
  </tbody>
</table>

___________________________________________________________________________________________________________________