# Ubuntu Linux as the base image
FROM ubuntu:latest

# Install the packages
RUN apt-get update && \
    apt-get -y install openjdk-8-jdk

# Open the default port
EXPOSE 80

# Change the work directory
WORKDIR /app

# Add our .jar file
ADD vertx/target/vertx-1.0-SNAPSHOT-fat.jar /app/vertx.jar

# Start nginx server
CMD ["java", "-jar", "vertx.jar"]

