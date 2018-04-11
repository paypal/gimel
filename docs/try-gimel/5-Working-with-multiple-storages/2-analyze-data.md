
* [Analyze Data](#analyze-data)
   * [Enrich Flights Data, Denormalize, Add Geo Coordinate](#enrich-flights-data-denormalize-add-geo-coordinate)
   * [Bootstrap Enriched Data in Elastic Search](#bootstrap-enriched-data-in-elastic-search)
      * [Create Elastic Search Dataset](#create-elastic-search-dataset)
      * [Load Enriched Data into Elastic Search](#load-enriched-data-into-elastic-search)
   * [Explore, Visualize and Discover Data on Kibana](#explore-visualize-and-discover-data-on-kibana)
   
# Analyze Data

## Enrich Flights Data, Denormalize, Add Geo Coordinate

```
val sql = """cache table flights_log_enriched

SELECT 
 to_date(substr(fl_date,1,10)) as flight_date
,flights_kafka.*
,lkp_airport_origin.location as origin_location
,lkp_airport_origin.name as origin_airport_name
,lkp_airport_origin.city as origin_airport_city
,lkp_airport_origin.country as origin_airport_country
,lkp_airport_dest.location as dest_location
,lkp_airport_dest.name as dest_airport_name
,lkp_airport_dest.city as dest_airport_city
,lkp_airport_dest.country as dest_airport_country
,lkp_carrier.description as carrier_desc
,lkp_airline.description as airline_desc
,lkp_cancellation.description as cancellation_reason

from flights flights_kafka                                  

left join lkp_carrier lkp_carrier                          
on flights_kafka.unique_carrier = lkp_carrier.code 

left join lkp_airline lkp_airline                          
on flights_kafka.airline_id = lkp_airline.code

left join lkp_cancellation lkp_cancellation                 
on flights_kafka.CANCELLATION_CODE = lkp_cancellation.code

left join lkp_airport lkp_airport_origin                   
on flights_kafka.origin = lkp_airport_origin.iata

left join lkp_airport lkp_airport_dest                    
on flights_kafka.dest = lkp_airport_dest.iata
"""

gsql(sql)
```

___________________________________________________________________________________________________________________

## Bootstrap Enriched Data in Elastic Search

### Create Elastic Search Dataset

```
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
  		"gimel.es.schema.mapping":"{\"location\": { \"type\": \"geo_point\" } }",
		  "gimel.es.index.partition.delimiter":"-",
		  "gimel.es.index.partition.isEnabled":"true",
		  "gimel.es.index.read.all.partitions.isEnabled":"true",
		  "gimel.es.index.partition.suffix":"20180205",
		  "gimel.es.schema.mapping":"{\"executionStartTime\": {\"format\": \"strict_date_optional_time||epoch_millis\", \"type\": \"date\" }, \"createdTime\": {\"format\": \"strict_date_optional_time||epoch_millis\", \"type\": \"date\"},\"endTime\": {\"format\": \"strict_date_optional_time||epoch_millis\", \"type\": \"date\"}}",
		  "gimel.storage.type":"ELASTIC_SEARCH",
		  "datasetName":"pcatalog.gimel_flights_elastic"
    }
}
""")
```

### Load Enriched Data into Elastic Search

```
val sql = """insert into pcatalog.gimel_flights_elastic
select * from flights_log_enriched
where cancelled = 1"""

gsql(sql)
```

___________________________________________________________________________________________________________________

## Explore, Visualize and Discover Data on Kibana

* Go to Kibana at http://localhost:5601
* Create the index pattern for flights index
* Explore and Visualize your data on Kibana Dashboard

___________________________________________________________________________________________________________________