#!/usr/bin/env bash

wget http://mirrors.koehn.com/apache/hbase/stable/hbase-1.4.8-bin.tar.gz
tar xzvf hbase-1.4.8-bin.tar.gz

sudo apt update
sudo apt install -y openjdk-8-jdk

echo "JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64" >> ~/.profile
source ~/.profile

echo "HBASE_HOME=/home/ubuntu/hbase-1.4.8" >> ~/.profile
source ~/.profile

echo "PATH=$PATH:$JAVA_HOME/bin:$HBASE_HOME/bin" >> ~/.profile
source ~/.profile


