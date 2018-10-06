### <a name="run-app"></a> How to Run the App in docker?
* Fork and clone this repo

* Invoke the following command for fetching the dependencies and building the jar

  ```shell
  build/udc 
  ```

* Invoke the following command for setting up docker containers for udc-services and Mysql metastore

  ```shell
  quickstart/setup-udc-containers
  ```

* Invoke the following command for bootstrapping Mysql data in the Mysql docker container

  ```shell
  quickstart/bootstrap-mysql
  ```

* Invoke the following command for starting udc-services

  ```shell
  quickstart/bootstrap-udc-serv
  ```

* Navigate to ``localhost:8080/swagger-ui.html``. The app will let you see all the available webservices


### <a name="run-app"></a> How to Run the App on your local machine?

* Invoke the following command for fetching the dependencies and building the jar

  ```shell
  build/udc
  ```

* Invoke the following commands to setup Mysql in local

  ```shell
  docker pull mysql
  docker rm mysql-standalone
  docker run --name mysql-standalone -p 3306:3306 -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=udc -e MYSQL_USER=udcadmin -e MYSQL_PASSWORD=Udc@123 -d mysql:latest
  docker exec -i mysql-standalone mysql -uudcadmin -pUdc@123 udc < build/sql/udc_ddl.sql
  ```

* Invoke the following command to start the udc-services

  ```shell
  java -jar -Dspring.profiles.active=dev target/udc-services-0.0.1-SNAPSHOT.jar
  ```

* Navigate to ``localhost:8080/swagger-ui.html``. The app will let you see all the available webservices

### <a name="run-app"></a> How to do debug incase of any issues?
* Enter into docker mysql container

  ```shell
  docker exec -it ${mysql_container_name} mysql -u${mysql_user} -p${mysql_password}
  ```
* Look into udc-serv logs in the Docker container

  ```shell
  docker logs udc-services
  ```
