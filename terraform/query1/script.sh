#!/usr/bin/env bash
# update apt-get
sudo apt update
# install java
sudo apt install -y openjdk-8-jdk
# run web-tier
sudo java -jar vertx.jar


