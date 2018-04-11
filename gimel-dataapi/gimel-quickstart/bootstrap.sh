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

source $GIMEL_HOME/quickstart/set-env
source $GIMEL_HOME/build/gimel_functions

export this_file=${0}
export this_script=$(basename $this_file)

write_log "------------------------------------------------------------------------------"
write_log "                          Starting Docker Containers ...."
write_log "------------------------------------------------------------------------------"

cd ${standalone_dir}/docker
export storages_list=$1
if [ "$storages_list" == "all" ] || [ -z "$storages_list" ]; then
  run_cmd "docker-compose up -d"
else
  export storages=$(printf '%s\n' "$storages_list" | tr ',' ' ')
  run_cmd "docker-compose up -d $storages namenode datanode spark-master spark-worker-1"
fi

write_log "------------------------------------------------------------------------------"
write_log "                         Staring Hive Metastore ...."
write_log "------------------------------------------------------------------------------"

#this sleep is required to allow time for DBs to start though the container is started.
sleep 5s
run_cmd "docker-compose up -d hive-metastore"

#check_error $? 999 "Bootstrap Dockers - failure"

sleep 5s

write_log "------------------------------------------------------------------------------"
write_log "			    unzipping flights data ..."
write_log "------------------------------------------------------------------------------"

cd $GIMEL_HOME/gimel-dataapi/gimel-quickstart/
if [ ! -d "flights" ]; then
  run_cmd "unzip flights.zip"
else 
  write_log "Looks like Flights Data already exists.. Skipping Unzip !"
fi

write_log "------------------------------------------------------------------------------"
write_log "               Attempting to Turn Off Safe Mode on Name Node.."
write_log "------------------------------------------------------------------------------"

write_log "Attempting to Turn Off Safe Mode on Name Node..."
run_cmd "docker exec -it namenode hadoop dfsadmin -safemode leave"

write_log "------------------------------------------------------------------------------"
write_log "                      Copy the flights data to Docker images ..."
write_log "------------------------------------------------------------------------------"

run_cmd "docker cp flights namenode:/root"
run_cmd "docker exec -it namenode hadoop fs -rm -r -f /flights"
run_cmd "docker exec -it namenode hadoop fs -put /root/flights /"

write_log "------------------------------------------------------------------------------"
write_log "                   Bootstraping the Physical Storage - HBASE ...."
write_log "------------------------------------------------------------------------------"

run_cmd "sh ${standalone_dir}/bin/bootstrap_hbase.sh" ignore_errors

write_log "------------------------------------------------------------------------------"
write_log "                   Bootstraping the Physical Storage - KAFKA ...."
write_log "------------------------------------------------------------------------------"

run_cmd "sh ${standalone_dir}/bin/bootstrap_kafka.sh" ignore_errors

sleep 5s

write_log "------------------------------------------------------------------------------"
write_log "                   Bootstraping the Physical Storage - HIVE ...."
write_log "------------------------------------------------------------------------------"

run_cmd "sh ${standalone_dir}/bin/bootstrap_hive.sh" ignore_errors

write_log "------------------------------------------------------------------------------"
write_log "                     ALL STORAGE CONTAINERS - LAUNCHED"
write_log "------------------------------------------------------------------------------"
write_log ""
write_log ""
write_log "------------------------------------------------------------------------------"
write_log "                       Setting up spark container..."
write_log "------------------------------------------------------------------------------"

run_cmd "docker cp $final_jar spark-master:/root/"
run_cmd "docker cp hive-server:/opt/hive/conf/hive-site.xml $GIMEL_HOME/tmp/hive-site.xml"
run_cmd "docker cp $GIMEL_HOME/tmp/hive-site.xml spark-master:/spark/conf/"
run_cmd "docker cp hbase-master:/opt/hbase-1.2.6/conf/hbase-site.xml $GIMEL_HOME/tmp/hbase-site.xml"
run_cmd "docker cp $GIMEL_HOME/tmp/hbase-site.xml spark-master:/spark/conf/"

write_log ""
write_log ""
write_log "----------------------------------------------------------------------------------------------------------------------"
write_log "---------- Bootstrap Complete | Ready to use Gimel | Use below command to start spark --------------------------------"
write_log "----------------------------------------------------------------------------------------------------------------------"
write_log ""
write_log ""
export tmp_var="export USER=$USER; export SPARK_HOME=/spark/; /spark/bin/spark-shell --jars /root/$gimel_jar_name"
export var="docker exec -it spark-master bash -c ${tmp_var}"
write_log "----------------------------------------------------------------------------------------------------------------------"
write_log "$var"
write_log "----------------------------------------------------------------------------------------------------------------------"

exit 0
