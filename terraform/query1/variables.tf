# ami id
variable "ami" {
  default = "ami-0ac019f4fcb7cb7e6"
}

# instance type
variable "instance_type" {
  default = "m5.large"
}

variable "instance_num" {
  default = 6
}

variable "key_name" {
  default = "team-project"
}
