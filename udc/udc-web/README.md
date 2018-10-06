### <a name="run-app"></a> How to Run the App in docker?
* Fork and clone this repo and enter into the directory

* Install node, npm and nvm 
  As a pre-requisite you will have to install nvm, node and npm on your local dev  machine

  ```shell
  brew install node
  brew install npm
  brew install nvm
  nvm install v8
  ```

* Invoke the following command to download all the dependencies for the application

  ```shell
  build/udc_ui
  ```

* Invoke the following command for setting up docker container for udc-ui

  ```shell
  quickstart/start_udc_ui
  ```

* While the application is running in the foreground, navigate to ``localhost:8081/dist``. It will navigate you to the UDC home page where you can search for datasets. 


### <a name="run-app"></a> How to Run the App on your local machine?

* Invoke the following command for fetching the dependencies

  ```shell
  build/udci_ui
  ```

* Invoke the following commands to setup Mysql in local

  ```shell
  ng serve  
  ```

* While the application is running in the background, Navigate to ``localhost:4200``. It will navigate you to the UDC home page where you can search for datasets
