#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
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

