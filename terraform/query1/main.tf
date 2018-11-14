provider "aws" {
  region = "us-east-1"
}

resource "aws_instance" "backend_server" {
  count = "${var.instance_num}"
  ami = "${var.ami}" # variable
  instance_type = "${var.instance_type}" # variable
  key_name = "${var.key_name}" # variable

  # Any connection information provided in a resource will apply to all the provisioners
  connection {
    type = "ssh"
    user = "ubuntu"
    private_key = "${file("../../../team-project.pem")}" # variable
  }

//  # upload web-tier
//  provisioner "file" {
//    source = "../../vertx/target/*-fat.jar" # variable
//    destination = "vertx.jar" # variable
//  }

  provisioner "remote-exec" {
    script = "script.sh"
  }

  tags {
    Name = "QRCode ${count.index}"
    Project = "Phase2"
  }
}

resource "aws_default_vpc" "default_vpc" {}

data "aws_subnet_ids" "default_subnet_ids" {
  vpc_id = "${aws_default_vpc.default_vpc.id}"
}

resource "aws_lb_target_group" "lb_target_group" {
  name = "tf-lb-tg"
  port = 80
  protocol = "TCP"
  vpc_id = "${aws_default_vpc.default_vpc.id}"
}

resource "aws_lb" "lb" {
  name = "tf-lb"
  internal = false
  load_balancer_type = "network"
  subnets = ["${data.aws_subnet_ids.default_subnet_ids.ids}"]

  tags = {
    Project = "Phase2"
  }
}

resource "aws_lb_listener" "lb_listener" {
  "default_action" {
    type = "forward"
    target_group_arn = "${aws_lb_target_group.lb_target_group.arn}"
  }
  load_balancer_arn = "${aws_lb.lb.arn}"
  port = 80
  protocol = "TCP"
}

resource "aws_lb_target_group_attachment" "lb_tga" {
  target_group_arn = "${aws_lb_target_group.lb_target_group.arn}"
  target_id = "${aws_instance.backend_server.*.id[count.index]}"
}