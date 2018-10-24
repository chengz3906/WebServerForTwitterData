# Provides an On-Demand EC2 instance resource with the student image,
# along with the Security Group
#
# Usage:
# Configure the credentials first with `aws configure`
# Create a file named `terraform.tfvars` and set the values of the variables defined at `variables.tf`
#
# terraform init      Initialize a Terraform working directory
# terraform validate  Validates the Terraform files
# terraform fmt       Rewrites config files to a canonical format
# terraform plan      Generate and show an execution plan
# terraform apply     Builds or changes infrastructure
# terraform destroy   Destroy Terraform-managed infrastructure

provider "aws" {
  region = "us-east-1"
}


resource "aws_instance" "student_on_demand_instance" {
  ami = "${var.ami}"
  instance_type = "${var.instance_type}"
  key_name = "${var.key_name}"
  count = "3"

  security_groups = [
    # note that name is used, other than id
    "default"
  ]
  provisioner "file" {
    source = "../impl_python/"
    destination = "~/"
    
  connection {
      type     = "ssh"
      user     = "ubuntu"
      private_key = "${file("../../15619demo.pem.txt")}" 
    }
  }
  provisioner "remote-exec" {
    script = "../impl_python/flask_setup.sh"
    
    connection {
      type     = "ssh"
      user     = "ubuntu"
      private_key = "${file("../../15619demo.pem.txt")}" 
      script_path = "~/flask_setup.sh"
    }
  } 
  tags {
    Project = "Phase1"
  }
}
resource "aws_elb" "elb" {
  name               = "query1-elb"
  availability_zones = ["us-east-1a", "us-east-1b", "us-east-1c"]

  listener {
    instance_port      = 5000
    instance_protocol  = "http"
    lb_port            = 80
    lb_protocol        = "http"
  }

  health_check {
    healthy_threshold   = 2
    unhealthy_threshold = 2
    timeout             = 3
    target              = "HTTP:5000/healthcheck"
    interval            = 30
  }
  instances = ["${aws_instance.student_on_demand_instance.*.id}"]
}

