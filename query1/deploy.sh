#!/usr/bin/env bash

sudo apt-get -y update
sudo apt-get -y install default-jdk

sudo update-java-alternatives -l
export JAVA_HOME='/usr/lib/jvm/java-1.11.0-openjdk-amd64'

curl http://mirror.metrocast.net/apache/tomcat/tomcat-8/v8.5.34/bin/apache-tomcat-8.5.34.tar.gz > apace-tomcat.tar.gz

tar xf apace-tomcat.tar.gz
sudo mv -f apache-tomcat-8.5.34 /usr/local

# change port to 80
sed -i -- 's/8080/80/g' /usr/local/apache-tomcat-8.5.34/conf/server.xml

# deploy war
sudo rm -r /usr/local/apache-tomcat-8.5.34/webapps/ROOT
sudo cp ~/ROOT.war /usr/local/apache-tomcat-8.5.34/webapps/ROOT.war

# start tomcat
sudo /usr/local/apache-tomcat-8.5.34/bin/startup.sh