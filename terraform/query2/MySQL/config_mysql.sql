use mysql;
create user '<SQL_USER>'@'%' identified by '<SQL_PASSWORD>';
grant all privileges on *.* to '<SQL_USER>'@'%' identified by '<SQL_PASSWORD>' with grant option;
flush privileges;