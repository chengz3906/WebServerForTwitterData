use mysql;
create user 'spongebob'@'%' identified by '15619';
grant all privileges on *.* to 'spongebob'@'%' identified by '15619' with grant option;
flush privileges;