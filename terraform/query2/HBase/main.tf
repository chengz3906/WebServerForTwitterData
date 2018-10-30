# HBase EMR Cluster Template
#
# In this module, you need to provision an EMR Cluster for HBase
# with the following requirements.
#   applications            -- Hadoop, HBase
#   emr version             -- emr-5.13.0
#   number of master nodes  -- 1
#   number of core nodes    -- 1
#   node instance tag       -- "Key:Project, Value:3.1"
#   node instance type      -- m4.large (spot)
#
# Usage:
# Configure the credentials first with `aws configure` with root credentials
# DO NOT use credentials of an IAM user unless you grant it with full admin permissions
#
# Create a file named `terraform.tfvars` and set the values of
# the variables defined at `variables.tf`
#
# Windows doesn't show file extensions by default.
# On Windows, make sure the file extension of the terraform.tfvars is correct,
# e.g. not "terraform.tfvars.txt".
#
# terraform init      Initialize a Terraform working directory
# terraform validate  Validates the Terraform files
# terraform fmt       Rewrites config files to canonical format
# terraform plan      Generate and show an execution plan
# terraform apply     Builds or changes infrastructure
# terraform destroy   Destroy Terraform-managed infrastructure

provider "aws" {
  region = "us-east-1"
}

resource "aws_security_group" "hbase_additional_sg" {
  # inbound internet access for ssh
  ingress {
    from_port = 22
    to_port   = 22
    protocol  = "tcp"

    cidr_blocks = [
      "0.0.0.0/0",
    ]
  }
//
//  # inbound internet access for hbase master and data nodes
//  ingress {
//    from_port = 16000
//    to_port   = 16030
//    protocol  = "tcp"
//
//    cidr_blocks = [
//      "0.0.0.0/0",
//    ]
//  }
//
//  # inbound internet access for hbase zookeeper
//  ingress {
//    from_port = 2181
//    to_port   = 2181
//    protocol  = "tcp"
//
//    cidr_blocks = [
//      "0.0.0.0/0",
//    ]
//  }

  tags {
    Name = "EMR additional security group"
  }
}

resource "aws_emr_cluster" "database_hbase" {
  name          = "project-database-hbase"
  release_label = "${var.cluster_release_label}"

  applications = ["Hadoop", "HBase"]

  bootstrap_action {
    path = "s3://elasticmapreduce/bootstrap-actions/run-if"
    name = "runif"
    args = ["instance.isMaster=true", "echo running on master node"]
  }

  ec2_attributes {
    # m4.large can only be used in a VPC and a Subnet is required
    subnet_id = "${aws_default_subnet.us-east-1-default-subnet.id}"

    # the key pair name
    key_name         = "${var.key_name}"
    instance_profile = "EMR_EC2_DefaultRole"

    # Additional security group for master
    additional_master_security_groups = "${aws_security_group.hbase_additional_sg.id}"
    additional_slave_security_groups  = "${aws_security_group.hbase_additional_sg.id}"
  }

  # so that you can add steps to run the MapReduce streaming jobs later
  keep_job_flow_alive_when_no_steps = true
  termination_protection            = false

  instance_group {
    instance_role  = "MASTER"
    instance_type  = "${var.master_node_instance_type}"
    instance_count = "${var.master_node_instance_count}"

    ebs_config {
      size                 = "32"
      type                 = "gp2"
      volumes_per_instance = 1
    }

    bid_price = "${var.master_node_bid_price}"
  }

  instance_group {
    instance_role  = "CORE"
    instance_type  = "${var.core_node_instance_type}"
    instance_count = "${var.core_node_instance_count}"

    ebs_config {
      size                 = "32"
      type                 = "gp2"
      volumes_per_instance = 1
    }

    bid_price = "${var.core_node_bid_price}"
  }

  tags {
    Name    = "HBase Cluster"
    Project = "Phase1"
  }

  service_role = "EMR_DefaultRole"
}

# aws_default_subnet provides a resource to manage a default AWS VPC subnet in
# the current region.
# The aws_default_subnet behaves differently from normal resources, in that
# Terraform does not create this resource, but instead "adopts" it into management.
# Terraform will not destroy the subnet during `terraform destory`.
resource "aws_default_subnet" "us-east-1-default-subnet" {
  availability_zone = "us-east-1a"
}
