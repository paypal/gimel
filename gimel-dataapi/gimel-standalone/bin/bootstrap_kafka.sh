#!/usr/bin/env bash


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

write_log "Creating Kafka topic"
docker exec -it kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 3 --topic gimel.demo.flights
docker exec -it kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 3 --topic gimel.demo.flights.json
docker exec -it kafka kafka-topics --zookeeper zookeeper:2181 --alter --topic gimel.demo.flights.json --config retention.ms=604800000
check_error $? 999 "Creating Hive external tables - failed"