# update apt-get
sudo apt-get update

# avoid some package unfound bugs
sleep 30

# install tomcat 8
sudo apt-get install -y tomcat8

# install servlet
sudo chown tomcat8:tomcat8 q1.war
sudo mv q1.war /var/lib/tomcat8/webapps/

