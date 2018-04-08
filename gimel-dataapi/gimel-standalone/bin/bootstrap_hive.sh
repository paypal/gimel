#!/usr/bin/env bash

export FLIGHTS_LOAD_DATA_SCRIPT=$GIMEL_HOME/gimel-dataapi/gimel-standalone/sql/flights_load_data.sql

#----------------------------function will write the message to Log File----------------------------#

#Usage : write_log < Whatever message you need to log >

write_log()
{

        msg=$1
        echo "$(date '+%Y%m%d %H:%M:%S') : $msg"
#        echo $1
#        echo "" >> ${log_file}
#        echo "$(date '+%Y%m%d %H:%M:%S') : $msg" >> ${log_file}
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

write_log "Creating Hive external tables for CSV data"

docker cp $FLIGHTS_LOAD_DATA_SCRIPT hive-server:/root
docker exec -it hive-server bash -c "hive -f /root/flights_load_data.sql"

check_error $? 999 "Creating Hive external tables - failed"