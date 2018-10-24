# output variables is a way to organize data to be easily queried and
# shown back to the Terraform user.
#
# As a user of Terraform, you may be interested in values of importance,
# e.g. a load balancer IP, VPN address, etc.
#
# Outputs are a way to tell Terraform what data is important.
# This data is outputted when "terraform apply" is called,
# and can be queried using the "terraform output" command.

output instance_id {
  description = "The instance ID"
  value = "${aws_instance.student_on_demand_instance.*.id}"
}

output instance_address {
  description = "The public DNS address of the instance"
  value = "${aws_instance.student_on_demand_instance.*.public_dns}"
}
