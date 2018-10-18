# Teradata Write API

Here is some detailed explanation on how Teradata Write API works:

## Flow Diagram for Teradata Write API
<img src="teradata-flow-diagrams/Teradata Write-Edits.jpg" alt="Teradata Write Flow Diagram"/>

### How it works:

1) Submit the dataset you want to write to and the input dataframe to Gimel API. You can use Teradata API as using Dataset.write() or ExecuteBatch SQL.

2) Repartition given dataframe into specified number of partitions.

3) Create connection to Teradata via each executor independently and insert partittioned data into temp partition table through each executor in parallel.

4) Union all temp partittion tables in Teradata and insert into target table.

5) Drop all the temp partitioned tables from spark driver.
