
# Modules

| Modules | Purpose |
| -------- | -------- |
| gimel-tools | Tools (core + sql + runnables such as copyDataSet, etc) |
| gimel-sql | SQL Support (core + Gimel-SQL Functionality) |
| gimel-core | Core (Contains All Connectors, the Unified Data API) |

# Refer as Maven Dependency

### Tools
```xml
    <dependency>
      <groupId>com.paypal.gimel</groupId>
      <artifactId>gimel-tools</artifactId> <!--Refer one of the below listed 3 versions, depending on the required spark version -->
      <version>2.0.0-SNAPSHOT</version> <!--provides spark 2.2.0 compiled code-->
      <scope>provided</scope> <!--Ensure scope is provided as the gimel libraries can be added at runtime-->
    </dependency>
```
### SQL
```xml
    <dependency>
      <groupId>com.paypal.gimel</groupId>
      <artifactId>gimel-sql</artifactId> <!--Refer one of the below listed 3 versions, depending on the required spark version -->
      <version>2.0.0-SNAPSHOT</version> <!--provides spark 2.2.0 compiled code-->
      <scope>provided</scope> <!--Ensure scope is provided as the gimel libraries can be added at runtime-->
    </dependency>
```
### Core
```xml
    <dependency>
      <groupId>com.paypal.gimel</groupId>
      <artifactId>gimel-core</artifactId> <!--Refer one of the below listed 3 versions, depending on the required spark version -->
      <version>2.0.0-SNAPSHOT</version> <!--provides spark 2.2.0 compiled code-->
      <scope>provided</scope> <!--Ensure scope is provided as the gimel libraries can be added at runtime-->
    </dependency>
```


--------------------------------------------------------------------------------------------------------------------

