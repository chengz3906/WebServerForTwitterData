# ami id
variable "ami" {default = "ami-0795e993eb27953cf"}

# instance type
variable "instance_type" {}

# Update "key_name" with the key pair name for SSH connection
# Note: it is NOT the path of the pem file
# you can find it in https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#KeyPairs:sort=keyName
variable "key_name" {}
