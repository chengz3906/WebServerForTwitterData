# update apt-get
sudo apt-get update

# install tomcat 8
sudo apt-get install -y tomcat8

# install java ?

# start tomcat
# sudo service tomcat8 start

# stop tomcat
# sudo service tomcat8 stop

sudo chown tomcat8:tomcat8 q1.war
sudo mv q1.war /var/lib/tomcat8/webapps/

