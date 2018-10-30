#!/usr/bin/env bash
sudo apt update
sleep 30
sudo apt install -y mysql-server
sleep 30
sudo mysql < config_mysql.sql
echo "
# romote access
[mysqld]
bind-address = 0.0.0.0

# 💩 𝌆
# UTF-8 should be used instead of Latin1. Obviously.
# NOTE "utf8" in MySQL is NOT full UTF-8: http://mathiasbynens.be/notes/mysql-utf8mb4

[client]
default-character-set = utf8mb4

[mysqld]
character-set-client-handshake = FALSE
character-set-server = utf8mb4
collation-server = utf8mb4_bin

[mysql]
default-character-set = utf8mb4
" | sudo tee --append /etc/mysql/mysql.conf.d/mysqld.cnf
sudo service mysql restart

# install tomcat 8
sudo apt-get install -y tomcat8
sleep 30

# move our .war
sudo chown tomcat8:tomcat8 q1.war
sudo chown tomcat8:tomcat8 q2.war
sudo mv q1.war /var/lib/tomcat8/webapps/
sudo mv q2.war /var/lib/tomcat8/webapps/

# set environment variables
sudo mkdir /var/lib/tomcat8/bin
echo "
export MYSQL_DB_NAME=twitter
export MYSQL_DNS=localhost
export MYSQL_DNS=localhost
export MYSQL_USER=spongebob
export MYSQL_PWD=15619
" | sudo tee --append /var/lib/tomcat8/bin/setenv.sh

# DANGEROUS
echo "
export TEAMID=<TEAMID>
<<<<<<< HEAD
export TEAM_AWS_ACCOUNT_ID=<TEAMID>
" | sudo tee --append /var/lib/tomcat8/bin/setenv.sh
sleep 30
=======
export TEAM_AWS_ACCOUNT_ID=<AWS_ACCOUNT_ID>
" | sudo tee --append /var/lib/tomcat8/bin/setenv.sh
>>>>>>> 44269ea4ebf1f1dc4a82985e7981740cf6f8d276
sudo service tomcat8 restart

# create new databases
sudo mysql < create_twitter_database.sql
