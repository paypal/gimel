# Teradata-Gimel-API-Benchmarking

## Small Dataset- Yelp dataset

Gimel Teradata Write API is tested for [Yelp Tip Dataset](https://www.yelp.com/dataset).
The API is tested with varying number of records written into Teradata from 10000 to 5M. The average performnace is measured by taking average of 3 iterations.(Itr=Iteration)

Following parameters are varied while benchmarking:
* BATCHSIZE (kept constant for this case, 10000)
* Number of executors
* Number of executor cores
* Write type(BATCH/FASTLOAD)

#### Batchsize=10000 | Executor Cores=1 | Executors=5 
![10000-1-5](https://github.com/laxpatil/Teradata-Gimel-API-Benchmarking/blob/master/plots/10000-1-5.png "Batchsize=10000 | Executor Cores=1 | Executors=5")

#### Batchsize=10000 | Executor Cores=1 | Executors=10
![10000-1-5](https://github.com/laxpatil/Teradata-Gimel-API-Benchmarking/blob/master/plots/10000-1-10.png "Batchsize=10000 | Executor Cores=1 | Executors=5")

#### Batchsize=10000 | Executor Cores=1 | Executors=15
![10000-1-5](https://github.com/laxpatil/Teradata-Gimel-API-Benchmarking/blob/master/plots/10000-1-15.png "Batchsize=10000 | Executor Cores=1 | Executors=5")

#### Batchsize=10000 | Executor Cores=4 | Executors=10
![10000-1-5](https://github.com/laxpatil/Teradata-Gimel-API-Benchmarking/blob/master/plots/10000-4-10.png "Batchsize=10000 | Executor Cores=1 | Executors=5")

#### FASTLOAD | Executor Cores=1 | Executors=1
![10000-1-5](https://github.com/laxpatil/Teradata-Gimel-API-Benchmarking/blob/master/plots/FASTLOAD-1.png "Batchsize=10000 | Executor Cores=1 | Executors=5")


### Average case performance for all of the above settings (Records vs Time-in-seconds)
(NOTE: `BATCHSIZE | NUM_CORES | EXECUTORS`)

|Records|10000-1-5|10000-1-10|10000-1-15|10000-4-10|FASTLOAD-4-10|
|----|-----|-----|-----|----|----|
|10000|4.22|6.37|4.10|5.27|17.39|
|20000|2.96|3.19|2.99|3.05|14.64|
|30000|3.16|3.38|4.14|2.92|15.12|
|40000|3.92|4.47|4.00|5.18|14.80|
|50000|4.58|4.39|4.62|4.50|14.79|
|60000|5.29|4.88|5.28|4.73|14.99|
|70000|6.28|6.80|6.10|5.32|14.91|
|80000|6.55|7.16|7.30|7.19|15.50|
|90000|7.38|7.76|7.45|7.42|15.13|
|100000|9.23|7.93|8.80|7.03|15.40|
|200000|15.57|13.88|15.84|14.00|16.58|
|300000|23.43|21.85|23.25|17.70|18.23|
|400000|30.25|28.20|28.83|17.92|18.97|
|500000|22.73|36.20|35.87|18.59|19.95|
|600000|24.45|41.97|41.78|18.92|21.10|
|700000|25.05|50.67|44.62|19.69|22.09|
|800000|24.67|54.64|41.59|25.33|23.08|
|900000|30.15|54.75|38.25|27.42|24.09|
|1000000|29.94|29.45|41.73|29.67|25.71|
|2000000|43.18|27.39|29.54|58.39|35.48|
|3000000|67.65|35.90|29.92|83.46|43.73|
|4000000|85.72|46.26|37.14|114.20|57.68|
|5000000|102.13|52.30|43.45|131.12|63.70|


![10000-1-5](https://github.com/laxpatil/Teradata-Gimel-API-Benchmarking/blob/master/plots/Average.png "Average")

### Inference
From the plots, there are two sharp points for Batch writes to Teradata i.e. Records size=10000 & Records size=1M.
After 1M, as the increasing number of executors makes write into Teradata faster.

Write into Teradata with FASTLOAD has sharp point at 1M, till then time required for write remains almost steady.

It is clearly observed that as we increase the parallelism i.e. increase in the number of executors to write into Teradata in batch mode, performace gets better for more records. 

---------------------------------------------------------------------------------------------------------------------

## Big Dataset
We tested Gimel Teradata Read/Write API on big dataset/table. Below are some details about data and benchmarking.

#### About table
This table stores payment metrics and attributes. It has one record per payment transaction. 
It has 212 columns with various data types.
The table has primary key with bigint type.  

#### Size of data

|Records in Millions|Size of Data in Gb|
|----|----|
|1M	|0.987g|
|5M|	4.6g|
|10M|	9.2g|
|25M|	21.5g|
|45M|	44.4g|
|75M|	74.56g|


### Teradata READ API Benchmarking - Batch vs FASTEXPORT
##### Executor Cores=1 | Executors=16 | FetchSize = 25000 | FASTEXPORT SESSIONS=16
![Batch vs FASTEXPORT](https://github.com/laxpatil/Teradata-Gimel-API-Benchmarking/blob/master/plots/Teradata%20READ%20Comparison_Big_table.png "Executor Cores=1 | Executors=16 | FetchSize = 25000 | FASTEXPORT SESSIONS=16")


### Teradata WRITE API Benchmarking - Batch vs FASTLOAD
##### Executor Cores=1 | Executors=12 | BatchSize = 25000 | FASTLOAD SESSIONS=12
![Batch vs FASTLOAD](https://github.com/laxpatil/Teradata-Gimel-API-Benchmarking/blob/master/plots/Teradata%20WRITE%20Comparison_Big_table.png "Executor Cores=1 | Executors=12 | FetchSize = 25000 | FASTLOAD SESSIONS=12")




