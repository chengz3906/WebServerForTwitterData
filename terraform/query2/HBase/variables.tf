# instance type

variable "cluster_release_label" {
  default = "emr-5.13.0"
}

variable "master_node_instance_type" {
  default = "m4.large"
}

variable "master_node_instance_count" {
  default = "1"
}

variable "master_node_bid_price" {
  default = "0.1"
}

variable "core_node_instance_type" {
  default = "m4.large"
}

variable "core_node_instance_count" {
  default = "1"
}

variable "core_node_bid_price" {
  default = "0.1"
}

# Update "key_name" with the key pair name
variable "key_name" {
  default = "team-project"
}
