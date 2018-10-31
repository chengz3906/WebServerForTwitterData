# ami id
variable "ami" {
  default = "ami-0ac019f4fcb7cb7e6"
}

# instance type
variable "instance_type" {
  default = "m5.large"
}

# ssh key
variable "key_name" {
  default = "team-project"
}

# instance number
variable "count" {
  default = 5
}