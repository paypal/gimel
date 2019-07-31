
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/95130dc3cc0a4be1852ca5d4363d3214)](https://www.codacy.com/app/Dee-Pac/gimel?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=paypal/gimel&amp;utm_campaign=Badge_Grade)
[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)


# <img src="docs/images/udc-short-logo.png" width="60" height="80" /> Unified Data Catalog

Unified Data Catalog (UDC) is a metadata catalog which provides a list of all datasets in an Enterprise.

  * [Overview](docs/overview.md)
  * [UDC Modules](docs/udc-modules.md)
  * [Contributing](docs/CONTRIBUTING.md)
  
--------------------------------------------------------------------------------------------------------------------

# Getting Started

For setting up UDC, you need to build and run UDC Services module and UDC Web which is the UDC UI.
### UDC Services

#### Build

* Fork and clone this repo

* Invoke the following command for fetching the dependencies and building the udc-services jar

  ```shell
  udc-serv/build/udc 
  ```

  You should be able to find ``udc-services-0.0.1-SNAPSHOT.jar`` in udc-serv/target folder.
  
#### Running the Services

You can run the services either by docker or manually
 * [Docker Setup](#docker-setup)
 * [Manual Setup](#manual-setup)

##### Docker Setup

* Invoke the following command for setting up docker containers for following services:
Elastic Search,
Kibana and
Mysql Metastore
  
    ```shell
    udc-serv/quickstart/start-udc-serv-prereq
    ```
  
  * Verify that Kibana is running at ``http://localhost:5601``
  * Try out Elastic Search API 
  
    ``curl http://localhost:9200/_cat/indices?v``
  
* Add User pcatalog in mysql and grant all privilages.

  ```
  docker exec -it mysql bash mysql -uroot -ppassword
  
  mysql> CREATE USER 'pcatalog'@'%' IDENTIFIED BY 'password';
  Query OK, 0 rows affected (0.00 sec)
  
  mysql> GRANT ALL PRIVILEGES ON * . * TO 'pcatalog'@'%';
  Query OK, 0 rows affected (0.00 sec)
  
  mysql> quit
  ```
* Invoke the following command for bootstrapping udc meta data in the Mysql docker container.

  ```shell
  udc-serv/quickstart/bootstrap-mysql
  ```

  Verify that udc metadata is created in mysql docker.
  
  ```
  docker exec -it mysql bash mysql -uroot -ppassword
  
  bash-4.2# mysql -uroot -ppassword
  
  mysql> show tables in pcatalog;
  +----------------------------------------------+
  | Tables_in_pcatalog                           |
  +----------------------------------------------+
  | pc_api_usage_metrics                         |
  | pc_attribute_discussion_init                 |
  | pc_attribute_discussion_responses            |
  | pc_classifications                           |
  | pc_classifications_attributes                |
  | pc_classifications_map                       |
  | pc_dataset_column_description_map            |
  | pc_dataset_description_map                   |
  | pc_dataset_ownership_map                     |
  | pc_discussion_to_attribute_map               |
  | pc_entities                                  |
  | pc_notifications                             |
  | pc_object_schema_map                         |
  | pc_policy_discovery_metric                   |
  | pc_ranger_policy                             |
  | pc_ranger_policy_user_group                  |
  | pc_schema_dataset_column_map                 |
  | pc_schema_dataset_map                        |
  | pc_source_provider                           |
  | pc_storage                                   |
  | pc_storage_cluster                           |
  | pc_storage_cluster_attribute_key             |
  | pc_storage_cluster_attribute_value           |
  | pc_storage_dataset                           |
  | pc_storage_dataset_change_log                |
  | pc_storage_dataset_system                    |
  | pc_storage_dataset_tag_map                   |
  | pc_storage_object_attribute_custom_key_value |
  | pc_storage_object_attribute_value            |
  | pc_storage_system                            |
  | pc_storage_system_attribute_value            |
  | pc_storage_system_container                  |
  | pc_storage_system_discovery                  |
  | pc_storage_type                              |
  | pc_storage_type_attribute_key                |
  | pc_tags                                      |
  | pc_teradata_policy                           |
  | pc_user_dataset_top_picks                    |
  | pc_user_storage_dataset_map                  |
  | pc_users                                     |
  | pc_zones                                     |
  +----------------------------------------------+
  41 rows in set (0.02 sec)
  
  mysql>
  ```
  
* Invoke the following command for starting udc-services.

  ```shell
  udc-serv/quickstart/start-udc-serv
  ```

* Navigate to ``localhost:8080/swagger-ui.html``. The app will let you see all the available webservices

##### Manual Setup

* Follow the instructions for setting up Elastic Search, Kibana and mysql in their documentation.

* Bootstrap UDC Metastore in Mysql

  ```
  mysql -uroot -ppassword < udc-meta/udc_ddl.sql
  ```

* Invoke the following command to start the udc-services

  ```
  java -jar -Dspring.profiles.active=dev udc-serv/target/udc-services-0.0.1-SNAPSHOT.jar
  ```

* Navigate to ``localhost:8080/swagger-ui.html``. The app will let you see all the available webservices.

***Note: Checkout configurations related to udc services like mysql metastore jdbc url in udc-serv/src/main/resources/application.properties.***

### UDC UI

You can setup UDC UI by using docker or manual setup

* [Docker](#docker)
* [Manual](#manual)

#### Docker

* Invoke the following command for setting up docker container for udc-ui.
  
  ```
  udc-web/quickstart/start_udc_ui
  ```

  This would build the docker image consisting of node and angular and builds the udc ui inside the container using npm.
  
* Navigate to ``http://localhost:80``. It will navigate you to the UDC home page where you can search for datasets.

#### Manual
  
  * If you do not have brew installed. Please follow the steps below
    
    ```
    xcode-select --install
    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    ```
    
* Invoke the following command for installing node and angular on your machine

  ```shell
  brew install node@8
  brew link node@8
  node --version
  ```

* Build the UI code

  ```
  cd udc-web
  sudo npm install -g @angular/cli@7.1.3
  npm install
  ```

* Run the server

    ```
    ng serve
    ```
  
  Navigate to ``http://localhost:4200``. It will navigate you to the UDC home page where you can search for datasets.

***Note: Checkout configurations related to udc ui like udc rest api url and port in udc-web/src/environments/environment.ts.***

--------------------------------------------------------------------------------------------------------------------

# Questions

  * [User Forum](https://groups.google.com/d/forum/udc-user)
  * [Developer Forum](https://groups.google.com/d/forum/udc-dev)
