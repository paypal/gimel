#!/usr/bin/env bash

#----------------------------function will write the message to Log File----------------------------#

#Usage : write_log < Whatever message you need to log >

write_log()
{

        msg=$1
        echo "$(date '+%Y%m%d %H:%M:%S') : $msg"
        #echo $1
        #echo "" >> ${log_file}
        #echo "$(date '+%Y%m%d %H:%M:%S') : $msg" >> ${log_file}
}

#----------------------------function will check for error code & exit if failure, else proceed further----------------------------#

#usage : Run immediately after executing any shell command - with 3 arguments.
#Example: check_error < pass $? from the shell command > < Error Code that you want the code to fail with > < Custom Message for errorcode -gt 0 >

check_error()
{

        cmd_error_code=$1
        pgm_exit_code=$2
        pgm_exit_msg=$3
        if  [ ${cmd_error_code} -gt 0 ]; then
                write_log "Error ! Exiting Program ... ${cmd_error_code} ${pgm_exit_code} ${pgm_exit_msg}"
#                cd ${standalone_dir}/docker
#                docker-compose down
                exit ${pgm_exit_code}
        else
                echo ""
        fi
}

write_log "Creating HBase tables"

docker exec -it hbase-master bash -c "echo \"
disable 'flights:flights_lookup_carrier_code'
disable 'flights:flights_lookup_cancellation_code'
disable 'flights:flights_lookup_airline_id'
drop 'flights:flights_lookup_carrier_code'
drop 'flights:flights_lookup_cancellation_code'
drop 'flights:flights_lookup_airline_id'
drop_namespace 'flights'
create_namespace 'flights'
create 'flights:flights_lookup_carrier_code','flights'
create 'flights:flights_lookup_cancellation_code','flights'
create 'flights:flights_lookup_airline_id','flights'\" | hbase shell"

check_error $? 999 "Creating HBase tables - failed"