#!/usr/bin/env bash
# Installing Percona XtraBackup
wget https://repo.percona.com/apt/percona-release_0.1-6.$(lsb_release -sc)_all.deb
sudo dpkg -i percona-release_0.1-6.$(lsb_release -sc)_all.deb
sudo apt update
sudo apt install -y percona-xtrabackup-24 # 2.4 with mysql 5.7
sudo apt install -y mysql-server
sudo apt install -y openjdk-8-jdk
echo "
# romote access
[mysqld]
bind-address = 0.0.0.0
character-set-client-handshake = FALSE
character-set-server = utf8mb4
collation-server = utf8mb4_bin
innodb_buffer_pool_size = 4G
max_connections=500

[client]
default-character-set = utf8mb4

[mysql]
default-character-set = utf8mb4
" | sudo tee --append /etc/mysql/mysql.conf.d/mysqld.cnf


# recover backup
wget https://s3.amazonaws.com/cmucc-team-phase2/full_backup_innobackupex_mysql/mysql_backup.tar.gz
tar -xvf mysql_backup.tar.gz # about 5 min
rm mysql_backup.tar.gz
sudo service mysql stop
sudo rm -r /var/lib/mysql
sudo innobackupex --copy-back ./2018-11-07_02-37-03/ # about 4 min
sudo chown -R mysql:mysql /var/lib/mysql
sudo service mysql start
rm -r ./2018-11-07_02-37-03/

# Full backup
# sudo innobackupex --user=DBUSER --password=DBUSERPASS /path/to/BACKUP-DIR/
# Full restore
# innobackupex --copy-back /path/to/BACKUP-DIR

## install tomcat 8
#sudo apt-get install -y tomcat8
#sleep 30
#
## move our .war
#sudo chown tomcat8:tomcat8 q1.war
#sudo chown tomcat8:tomcat8 q2.war
#sudo mv q1.war /var/lib/tomcat8/webapps/
#sudo mv q2.war /var/lib/tomcat8/webapps/
#
## set environment variables
#sudo mkdir /var/lib/tomcat8/bin
#echo "
#export MYSQL_DB_NAME=twitter
#export MYSQL_DNS=localhost
#export MYSQL_DNS=localhost
#export MYSQL_USER=spongebob
#export MYSQL_PWD=15619
#" | sudo tee --append /var/lib/tomcat8/bin/setenv.sh
#
## DANGEROUS
#echo "
#export TEAMID=<TEAM_ID>
#export TEAM_AWS_ACCOUNT_ID=<AWS_ACCOUNT_ID>
#" | sudo tee --append /var/lib/tomcat8/bin/setenv.sh
#sudo service tomcat8 restart


