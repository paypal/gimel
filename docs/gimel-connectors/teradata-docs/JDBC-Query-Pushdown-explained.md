# JDBC Query PushDown

Here is some detailed explanation on how JDBC Query PushDown API works.


## Flow Diagram for Teradata Write API
<img src="teradata-flow-diagrams/Teradata Query Pushdown.jpg" alt="JDBC Query PushDown Diagram"/>

### How it works:

1) Submit query you want to execute using Gimel datasets including JDBC dataset. The query you want to pushdown to JDBC datasource (e.g. Teradata) **must** include all JDBC datasets. You must use **ExecuteBatch SQL**.

2) Spark driver will submit the query direcly to JDBC datasource ex. Teradata.

3) Excute the complex query on datasource (ex. Teradata) and get the result in the form of spark dataframe.
