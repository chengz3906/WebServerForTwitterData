#!/bin/bash 
sudo apt-get update -y
sudo apt-get install python3 -y
sudo apt-get install python3-pip -y
sudo pip3 install flask
python3 qrcode.py &
