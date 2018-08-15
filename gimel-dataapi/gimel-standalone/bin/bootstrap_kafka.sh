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

write_log "-----------------------------------------------------------------"
write_log "Deleting topic if exists..."
write_log "-----------------------------------------------------------------"
run_cmd "docker exec -it kafka kafka-topics --delete --zookeeper zookeeper:2181 --topic gimel.demo.flights"
run_cmd "docker exec -it kafka kafka-topics --delete --zookeeper zookeeper:2181 --topic gimel.demo.flights.json"
run_cmd "docker exec -it kafka kafka-topics --delete --zookeeper zookeeper:2181 --topic gimel.demo.flights1.json"

write_log "-----------------------------------------------------------------"
write_log "Creating Kafka topic"
write_log "-----------------------------------------------------------------"
run_cmd "docker exec -it kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic gimel.demo.flights"
run_cmd "docker exec -it kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic gimel.demo.flights.json"
run_cmd "docker exec -it kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic gimel.demo.flights1.json"
run_cmd "docker exec -it kafka kafka-topics --zookeeper zookeeper:2181 --alter --topic gimel.demo.flights.json --config retention.ms=604800000"
run_cmd "docker exec -it kafka kafka-topics --zookeeper zookeeper:2181 --alter --topic gimel.demo.flights1.json --config retention.ms=604800000"

write_log "Completed Script --> ${full_file}"
