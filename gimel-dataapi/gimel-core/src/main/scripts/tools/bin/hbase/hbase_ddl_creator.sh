#!/bin/bash
# Usage hbase_ddl_creator.sh all <hiveDatabaseName> <hbase table file location>; To run this script for all the tables in hbase_tables.txt
# Usage hbase_ddl_creator.sh <hbaseTableName> <hiveDatabaseName>; To run this script for <hbaseTableName>.
input=$3
table=$1
hiveDatabase=$2
if [ $table == "all" ]; then
    while IFS= read -r var
    do
        echo "$var"
        export HBASE_TABLE=$var
        export HIVE_DB=$hiveDatabase
        echo "scan '$var', LIMIT => 100" | hbase shell | awk -F'=' '{print $2}' | python hbase_ddl_tool.py >> ddls.sql
    done < "$input"
#if only one table needs to be scanned
else
    echo "$var"
    export HBASE_TABLE=$table
    export HIVE_DB=$hiveDatabase
    echo "scan '$table', LIMIT => 100" | hbase shell | awk -F'=' '{print $2}' | python hbase_ddl_tool.py >> ddls.sql
fi

