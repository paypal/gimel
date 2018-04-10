#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

this_script=`basename $0`
this_dir=`dirname $0`
full_file="${this_dir}/${this_script}"

source ${GIMEL_HOME}/build/gimel_functions

write_log "Started Script --> ${full_file}"

export BOOTSTRAP_SQL_DIR=$GIMEL_HOME/gimel-dataapi/gimel-standalone/sql

bootstrap_storage()
{
storage=$1
write_log "-----------------------------------------------------------------"
write_log " Executing $BOOTSTRAP_SQL_DIR/bootstrap_${storage}.sql ..."
write_log "-----------------------------------------------------------------"
write_log "copying $BOOTSTRAP_SQL_DIR/bootstrap_${storage}.sql to HiveServer Docker Container..."
run_cmd "docker cp $BOOTSTRAP_SQL_DIR/bootstrap_${storage}.sql hive-server:/root"
export to_run="hive -f /root/bootstrap_${storage}.sql"
run_cmd "docker exec -it hive-server ${to_run}"
}

bootstrap_storage kafka
bootstrap_storage hbase
bootstrap_storage hive
bootstrap_storage elasticsearch

write_log "Completed Script --> ${full_file}"
