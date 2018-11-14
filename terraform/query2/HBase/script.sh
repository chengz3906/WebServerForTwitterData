#!/usr/bin/env bash

wget http://mirrors.koehn.com/apache/hbase/stable/hbase-1.4.8-bin.tar.gz
tar xzvf hbase-1.4.8-bin.tar.gz

sudo apt update
sudo apt install -y openjdk-8-jdk

mv ~/conf/.profile ~/.profile
source ~/.profile

mv ~/conf/hbase-env.sh $HBASE_HOME/conf/hbase-env.sh
mv ~/conf/hbase-site.xml $HBASE_HOME/conf/hbase-site.xml

./start-hbase.sh

# load data
# java -cp hbase_etl.jar cmu.cc.team.spongebob.etl.hbase.LoadHBase

# run web-tier
#sudo java -cp vertx.jar cmu.cc.team.spongebob.vertx.MySQLVerticle




