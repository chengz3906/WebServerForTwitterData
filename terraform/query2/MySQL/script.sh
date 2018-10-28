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
export MYSQL_DB_NAME=twitter
export MYSQL_DNS=localhost
export MYSQL_USER=spongebob
export MYSQL_PWD=15619