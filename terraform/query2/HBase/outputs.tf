# (reference)
# output variables is a way to organize data to be easily queried and
# shown back to the Terraform user.
#
# As a user of Terraform, you may be interested in values of importance,
# e.g. a load balancer IP, VPN address, etc.
#
# Outputs are a way to tell Terraform what data is important.
# This data is outputted when "terraform apply" is called,
# and can be queried using the "terraform output" command.

output master_public_dns {
  description = "The public DNS name of the master EC2 instance"
  value       = "${aws_emr_cluster.database_hbase.master_public_dns}"
}

output "sg_info" {
  description = "The security group ID"
  value = "${aws_security_group.hbase_additional_sg.id}"
}
