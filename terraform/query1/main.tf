provider "aws" {
  region = "us-east-1"
}

resource "aws_default_vpc" "default_vpc" {}

data "aws_subnet_ids" "default_subnet_ids" {
  vpc_id = "${aws_default_vpc.default_vpc.id}"
}

resource "aws_instance" "backend_server" {
  count = 2
  ami = "ami-0ac019f4fcb7cb7e6" # variable
  instance_type = "t2.micro" # variable
  key_name = "phase1-qrcode" # variable

  # security_groups = [] # default is default

  # Any connection information provided in a resource will apply to all the provisioners
  connection {
    type = "ssh"
    user = "ubuntu"
    private_key = "${file("phase1-qrcode.pem")}" # variable
  }

  provisioner "file" {
    source = "q1.war" # variable
    destination = "q1.war" # variable
  }

  provisioner "remote-exec" {
    script = "script.sh"
  }

  tags {
    Name = "CC Team Terraform Primer"
    Project = "Phase1"
  }
}


resource "aws_lb_target_group" "lb_target_group" {
  name = "tf-lb-tg"
  port = 8080
  protocol = "HTTP"
  vpc_id = "${aws_default_vpc.default_vpc.id}"
  health_check {
    port = "8080"
    protocol = "HTTP"
    path = "/q1/heartbeat"
  }
}

resource "aws_lb" "lb" {
  name = "tf-lb"
  internal = false
  load_balancer_type = "application"
//  security_groups = [""] // default
  subnets = ["${data.aws_subnet_ids.default_subnet_ids.ids}"]

  tags = {
    Project = "Phase1"
  }
}

resource "aws_lb_listener" "lb_listener" {
  "default_action" {
    type = "forward"
    target_group_arn = "${aws_lb_target_group.lb_target_group.arn}"
  }
  load_balancer_arn = "${aws_lb.lb.arn}"
  port = 80
  protocol = "HTTP"
}

resource "aws_lb_target_group_attachment" "lb_tga" {
  target_group_arn = "${aws_lb_target_group.lb_target_group.arn}"
  target_id = "${aws_instance.backend_server.*.id}"
}