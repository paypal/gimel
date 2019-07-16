#!/bin/bash

if [ -z $UDC_HOME ]; then
        export UDC_HOME=${PWD}
fi

# ENVIRONMENT VARIABLES FOR UDC_SERV
export udc_serv_exposed_port=8080
export udc_repo_home=${UDC_HOME}
export udc_jar_name=udc-services-0.0.1-SNAPSHOT.jar
export udc_serv_container_name=udc-services
export udc_serv_image_name=udc-services

# ENVIRONMENT VARIABLES FOR UDC_META_STORE
export mysql_exposed_port=3309
export mysql_root_password=password
export mysql_database=pcatalog
export mysql_user=root
export mysql_password=password
export mysql_image_name=mysql
export mysql_container_name=mysql-standalone
export mysql_image_tag_name=latest
