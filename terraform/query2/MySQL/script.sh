#!/usr/bin/env bash
sudo apt update
sudo apt install -y mysql-server
sudo mysql < config_mysql.sql
#sudo sed -i -- 's/bind-address/#bind-address/' /etc/mysql/mysql.conf.d/mysqld.cnf
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
character-set-server = utf8mb4
collation-server = utf8mb4_unicode_ci

[mysql]
default-character-set = utf8mb4
" | sudo tee --append /etc/mysql/mysql.conf.d/mysqld.cnf
sudo service mysql restart