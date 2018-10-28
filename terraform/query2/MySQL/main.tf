provider "aws" {
  region = "us-east-1"
}

resource "aws_instance" "mysql_server" {
//  count = "${var.count}"
  ami = "${var.ami}" # variable
  instance_type = "${var.instance_type}" # variable
  key_name = "${var.key_name}" # variable

  connection {
    type = "ssh"
    user = "ubuntu"
    private_key = "${file("team-project.pem")}"
  }

  provisioner "file" {
    source = "config_mysql.sql"
    destination = "config_mysql.sql"
  }

  provisioner "remote-exec" {
//    script = "script.sh"
    inline = [
      "sudo apt update",
      "sudo apt install -y mysql-server",
      "sudo mysql < config_mysql.sql",
      "echo '[mysqld]' | sudo tee --append /etc/mysql/mysql.conf.d/mysqld.cnf",
      "echo 'bind-address = 0.0.0.0' | sudo tee --append /etc/mysql/mysql.conf.d/mysqld.cnf",
      "echo 'character-set-server = utf8mb4' | sudo tee --append /etc/mysql/mysql.conf.d/mysqld.cnf",
      "echo 'collation-server = utf8mb4_unicode_ci' | sudo tee --append /etc/mysql/mysql.conf.d/mysqld.cnf",
      "echo '[client]' | sudo tee --append /etc/mysql/mysql.conf.d/mysqld.cnf",
      "echo 'default-character-set = utf8mb4' | sudo tee --append /etc/mysql/mysql.conf.d/mysqld.cnf",
      "echo '[mysql]' | sudo tee --append /etc/mysql/mysql.conf.d/mysqld.cnf",
      "echo 'default-character-set = utf8mb4' | sudo tee --append /etc/mysql/mysql.conf.d/mysqld.cnf",
      "sudo service mysql restart"
    ]
  }

  tags {
    Name = "MySQL test"
    Project = "Phase1"
  }
}

//resource "aws_default_vpc" "default_vpc" {}
//
//data "aws_subnet_ids" "default_subnet_ids" {
//  vpc_id = "${aws_default_vpc.default_vpc.id}"
//}
//
//resource "aws_lb_target_group" "lb_target_group" {
//  name = "tf-lb-tg"
//  port = 3306
//  protocol = "TCP"
//  vpc_id = "${aws_default_vpc.default_vpc.id}"
//}
//
//resource "aws_lb" "lb" {
//  name = "tf-lb"
//  internal = false
//  load_balancer_type = "network"
////  security_groups = [""] // default
//  subnets = ["${data.aws_subnet_ids.default_subnet_ids.ids}"]
//
//  tags = {
//    Project = "Phase1"
//  }
//}
//
//resource "aws_lb_listener" "lb_listener" {
//  "default_action" {
//    type = "forward"
//    target_group_arn = "${aws_lb_target_group.lb_target_group.arn}"
//  }
//  load_balancer_arn = "${aws_lb.lb.arn}"
//  port = 3306
//  protocol = "TCP"
//}
//
//resource "aws_lb_target_group_attachment" "lb_tga" {
//  count = "${var.count}"
//  target_group_arn = "${aws_lb_target_group.lb_target_group.arn}"
//  target_id = "${aws_instance.mysql_server.*.id[count.index]}"
//}