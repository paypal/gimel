#!/usr/bin/env bash

export gimel_repo_home=${GIMEL_HOME}
export this_file=${0}
export this_script=$(basename $this_file)
#export this_time="$(date '+%Y%m%d%H%M%S')"
#export log_dir=${gimel_repo_home}/.gimel_logs
export standalone_dir=${gimel_repo_home}/gimel-dataapi/gimel-standalone
#export log_file=${log_dir}/${this_script}.${this_time}
export gimel_jar_name=gimel-sql-1.2.0-SNAPSHOT-uber.jar
export final_jar=${standalone_dir}/lib/$gimel_jar_name
export FLIGHTS_DATA_PATH=$GIMEL_HOME/gimel-dataapi/gimel-quickstart/flights

#mkdir -p ${log_dir}

#----------------------------function will write the message to Log File----------------------------#

#Usage : write_log < Whatever message you need to log >

write_log()
{

        msg=$1
        echo "$(date '+%Y%m%d %H:%M:%S') : $msg"
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
                #send_email "FAILURE | ${this_script}"
#                cd ${standalone_dir}/docker
#                docker-compose down
                exit ${pgm_exit_code}
        else
                echo ""
        fi
}

check_warning()
{

        cmd_error_code=$1
        pgm_exit_code=$2
        pgm_exit_msg=$3
        if  [ ${cmd_error_code} -gt 0 ]; then
                write_log "WARNING ! ${cmd_error_code} ${pgm_exit_code} ${pgm_exit_msg}"
        else
                echo ""
        fi
}

write_log "Starting Docker Containers ...."
cd ${standalone_dir}/docker
export storages_list=$1
if [ "$storages_list" == "all" ] || [ -z "$storages_list" ]; then
  docker-compose up -d
else
  export storages=$(printf '%s\n' "$storages_list" | tr ',' ' ')
  docker-compose up -d $storages namenode datanode spark-master spark-worker-1
fi

sleep 10s
docker-compose up -d hive-metastore

check_error $? 999 "Bootstrap Dockers - failure"

sleep 10s

#mkdir -p ${gimel_repo_home}/tmp
#cd ${gimel_repo_home}/tmp
#write_log "Getting the spark binary"
#if [ ! -d "spark-2.2.1" ]; then
#  curl -O http://mirrors.sorengard.com/apache/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz
#  check_error $? 999 "Getting Spark binary - failed"
#  tar -xzf spark-2.2.1-bin-hadoop2.7.tgz
#  mv spark-2.2.1-bin-hadoop2.7 spark-2.2.1
#fi

write_log "Loading Flights data to HDFS"

cd $GIMEL_HOME/gimel-dataapi/gimel-quickstart/
if [ ! -d "flights" ]; then
  unzip flights.zip
fi
docker exec -it namenode hadoop dfsadmin -safemode leave
docker cp flights namenode:/root
docker exec -it namenode hadoop fs -rm -r -f /flights
docker exec -it namenode hadoop fs -put /root/flights /

check_error $? 999 "Loading flights data to HDFS - failed"

write_log "Bootstraping the Physical Storages"

#write_log "Bootstraping hive tables"
#sh ${standalone_dir}/bin/bootstrap_hive.sh
#check_error $? 999 "Bootstraping hive tables - failed"

write_log "Bootstraping HBase tables"
sh ${standalone_dir}/bin/bootstrap_hbase.sh
check_warning $? 999 "Bootstraping hbase tables - failed"

write_log "Bootstraping Kafka topic"
sh ${standalone_dir}/bin/bootstrap_kafka.sh
check_warning $? 999 "Bootstraping kafka topic - failed"

#export SPARK_HOME=${gimel_repo_home}/tmp/spark-2.2.1
#sh $SPARK_HOME/bin/spark-shell --jars ${final_jar}

sleep 10s

write_log "Starting spark shell..."

docker cp $final_jar spark-master:/root/
docker cp hive-server:/opt/hive/conf/hive-site.xml $GIMEL_HOME/tmp
docker cp $GIMEL_HOME/tmp/hive-site.xml spark-master:/spark/conf/
docker cp hbase-master:/opt/hbase-1.2.6/conf/hbase-site.xml $GIMEL_HOME/tmp
docker cp $GIMEL_HOME/tmp/hbase-site.xml spark-master:/spark/conf/
docker exec -it spark-master bash -c "export USER=$USER; export SPARK_HOME=/spark/; /spark/bin/spark-shell --jars /root/$gimel_jar_name"

check_error $? 999 "Starting spark shell - failed"

# send_email "${this_script} : Gimel Docker Up - Success"

write_log "SUCCESS !!!"