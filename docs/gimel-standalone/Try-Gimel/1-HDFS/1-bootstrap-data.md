
* [Catalog Provider as USER](#catalog-provider-as-user)
* [Bootstrap Data](#bootstrap-data)
   * [Bootstrap Flights Data](#bootstrap-flights-data)
      * [Create HDFS Datasets for loading Flights Data](#create-hdfs-datasets-for-loading-flights-data)
   * [Bootstrap Flights Lookup Data](#bootstrap-flights-lookup-data)
      * [Create HDFS Datasets for loading Flights Lookup Data](#create-hdfs-datasets-for-loading-flights-lookup-data)
   * [Cache Airports Data from HDFS](#cache-airports-data-from-hdfs)

# Catalog Provider as USER

```
gsql("set gimel.catalog.provider=USER");
```


# Bootstrap Data

## Bootstrap Flights Data

### Create HDFS Dataset for loading Flights Data

```
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
```

___________________________________________________________________________________________________________________

## Bootstrap Flights Lookup Data

### Create HDFS Datasets for loading Flights Lookup Data

```
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
```

```
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
```

```
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
```

```
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
```

___________________________________________________________________________________________________________________

## Cache Airports Data from HDFS

```
val sql="""cache table lkp_airport
select 
struct(lat,lon) as location
,concat(lat,",",lon) as location1
, * 
from 
(
select iata, lat, lon, country, city, name
, row_number() over (partition by iata order by 1 desc ) as rnk
from pcatalog.flights_lookup_airports_hdfs
) tbl
where rnk  = 1
"""
```
```
gsql(sql)
```

___________________________________________________________________________________________________________________