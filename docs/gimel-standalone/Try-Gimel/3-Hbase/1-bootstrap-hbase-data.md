
* [Bootstrap Data](#bootstrap-data)
      * [Create HBase Datasets](#create-hbase-datasets)

# Bootstrap Data

### Create HBase Datasets

```
gsql("""set pcatalog.flights_lookup_cancellation_code_hbase.dataSetProperties=
{
    "datasetType": "HBASE",
    "fields": [
        {
            "fieldName": "Code",
            "fieldType": "string",
            "isFieldNullable": false
        },
        {
            "fieldName": "Description",
            "fieldType": "string",
            "isFieldNullable": false
        }
    ],
    "partitionFields": [],
    "props": {
        "gimel.hbase.rowkey":"Code",
        "gimel.hbase.table.name":"flights:flights_lookup_cancellation_code",
        "gimel.hbase.namespace.name":"flights",
        "gimel.hbase.columns.mapping":":key,flights:Description",
         "datasetName":"pcatalog.flights_lookup_cancellation_code_hbase"
    }
}
""")
```

```
gsql("""set pcatalog.flights_lookup_carrier_code_hbase.dataSetProperties=
{
    "datasetType": "HBASE",
    "fields": [
        {
            "fieldName": "Code",
            "fieldType": "string",
            "isFieldNullable": false
        },
        {
            "fieldName": "Description",
            "fieldType": "string",
            "isFieldNullable": false
        }
    ],
    "partitionFields": [],
    "props": {
        "gimel.hbase.rowkey":"Code",
        "gimel.hbase.table.name":"flights:flights_lookup_carrier_code",
        "gimel.hbase.namespace.name":"flights",
        "gimel.hbase.columns.mapping":":key,flights:Description",
         "datasetName":"pcatalog.flights_lookup_carrier_code_hbase"
    }
}
""")
```

```
gsql("""set pcatalog.flights_lookup_airline_id_hbase.dataSetProperties=
{
    "datasetType": "HBASE",
    "fields": [
        {
            "fieldName": "Code",
            "fieldType": "string",
            "isFieldNullable": false
        },
        {
            "fieldName": "Description",
            "fieldType": "string",
            "isFieldNullable": false
        }
    ],
    "partitionFields": [],
    "props": {
        "gimel.hbase.rowkey":"Code",
        "gimel.hbase.table.name":"flights:flights_lookup_airline_id",
        "gimel.hbase.namespace.name":"flights",
        "gimel.hbase.columns.mapping":":key,flights:Description",
         "datasetName":"pcatalog.flights_lookup_airline_id_hbase"
    }
}
""")

```

___________________________________________________________________________________________________________________