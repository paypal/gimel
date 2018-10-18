# Gimel Teradata Read API


Here is some detailed explanation on how Teradata Read API works:

## Flow Diagram for Teradata Read API

<img src="teradata-flow-diagrams/Teradata Read-Edits.jpg" alt="Teradat Read"/>

### How it works:
1) Submit the dataset to Gimel API you want to read. You can use Teradata API as using Dataset.read() or ExecuteBatch SQL

2) If partitionColumn is not specified, get the primary of the teradata table from dbc.indicess to be used as a partitionColumn.

3) Split the query and submit the query to the executors, in order to paralllize the read operation.

4) Create a connection to Teradata from each exceutor and read the data in parallel, get data from table in datdataFrame.
