# get open jdk
FROM openjdk:8

# expose the port 
EXPOSE 8080

# add jar to docker container
ADD target/udc-services-0.0.1-SNAPSHOT.jar udc-services-0.0.1-SNAPSHOT.jar

# Run application jar
ENTRYPOINT [ "java","-jar","-Dspring.profiles.active=dev", "udc-services-0.0.1-SNAPSHOT.jar" ]
