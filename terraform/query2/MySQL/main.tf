provider "aws" {
  region = "us-east-1"
}

resource "aws_instance" "mysql_server" {
  count = "${var.count}"
  ami = "${var.ami}" # variable
  instance_type = "${var.instance_type}" # variable
  key_name = "${var.key_name}" # variable

  connection {
    type = "ssh"
    user = "ubuntu"
    private_key = "${file("../../../../team-project.pem")}"
  }

  provisioner "remote-exec" {
    script = "script.sh"
  }

  root_block_device {
    volume_type = "gp2"
    volume_size = "100"
    delete_on_termination = "true"
  }

  volume_tags {
    Name = "HBase ${count.index}"
    Project = "Phase2"
    teambackend = "mysql"
  }

  tags {
    Name = "HBase ${count.index}"
    Project = "Phase2"
    teambackend = "mysql"
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
//  port = 8080
//  protocol = "TCP"
//  vpc_id = "${aws_default_vpc.default_vpc.id}"
//}
//
//resource "aws_lb" "lb" {
//  name = "tf-lb"
//  internal = false
//  load_balancer_type = "network"
//  subnets = ["${data.aws_subnet_ids.default_subnet_ids.ids}"]
//
//  tags = {
//    Project = "Phase2"
//    teambackend = "mysql"
//  }
//}
//
//resource "aws_lb_listener" "lb_listener" {
//  "default_action" {
//    type = "forward"
//    target_group_arn = "${aws_lb_target_group.lb_target_group.arn}"
//  }
//  load_balancer_arn = "${aws_lb.lb.arn}"
//  port = 80
//  protocol = "TCP"
//}
//
//resource "aws_lb_target_group_attachment" "lb_tga" {
//  count = "${var.count}"
//  target_group_arn = "${aws_lb_target_group.lb_target_group.arn}"
//  target_id = "${aws_instance.mysql_server.*.id[count.index]}"
//}