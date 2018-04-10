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
write_log "If exists, - Disable HBase tables"
write_log "-----------------------------------------------------------------"

command_for_docker="echo \"
disable 'flights:flights_lookup_carrier_code'
disable 'flights:flights_lookup_cancellation_code'
disable 'flights:flights_lookup_airline_id'\" | hbase shell"

docker exec -it hbase-master bash -c "${command_for_docker}"

write_log "-----------------------------------------------------------------"
write_log "If exists, - Drop HBase tables"
write_log "-----------------------------------------------------------------"

command_for_docker="echo \"
drop 'flights:flights_lookup_carrier_code'
drop 'flights:flights_lookup_cancellation_code'
drop 'flights:flights_lookup_airline_id'
drop_namespace 'flights'\" | hbase shell"

docker exec -it hbase-master bash -c "${command_for_docker}"

write_log "-----------------------------------------------------------------"
write_log "Create HBase tables"
write_log "-----------------------------------------------------------------"

command_for_docker="echo \"
create_namespace 'flights'
create 'flights:flights_lookup_carrier_code','flights'
create 'flights:flights_lookup_cancellation_code','flights'
create 'flights:flights_lookup_airline_id','flights'\" | hbase shell"

docker exec -it hbase-master bash -c "${command_for_docker}"

write_log "Completed Script --> ${full_file}"
