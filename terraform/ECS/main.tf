### ECS

resource "aws_ecs_cluster" "ecs_cluster" {
  name = "vertx"
}

resource "aws_ecs_task_definition" "task_def" {
  family                   = "vertx"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "1024"
  memory                   = "2048"

  container_definitions = <<DEFINITION
[
  {
    "cpu": 1024,
    "image": "859423033203.dkr.ecr.us-east-1.amazonaws.com/vertx:list_final_2.0",
    "memory": 2048,
    "name": "vertx",
    "networkMode": "awsvpc",
    "portMappings": [
      {
        "containerPort": 80,
        "hostPort": 80
      }
    ]
  }
]
DEFINITION
}

resource "aws_ecs_service" "main" {
  name            = "tf-ecs-service"
  cluster         = "${aws_ecs_cluster.ecs_cluster.id}"
  task_definition = "${aws_ecs_task_definition.task_def.arn}"
  desired_count   = "6"
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = ["subnet-8aa7ffc0"]
  }
}

resource "aws_db_instance" "db1" {
  allocated_storage    = 750
  storage_type         = "gp2"
  engine               = "mysql"
  engine_version       = "5.7"
  instance_class       = "db.m5.large"
  name                 = "q2-pkey-q3-pkey"
  username             = "${mysql_username}"
  password             = "${mysql_password}"
  parameter_group_name = "default.mysql5.7"
}

resource "aws_db_instance" "db2" {
  allocated_storage    = 750
  storage_type         = "gp2"
  engine               = "mysql"
  engine_version       = "5.7"
  instance_class       = "db.m5.large"
  name                 = "q2-pkey-q3-pkey"
  username             = "${mysql_username}"
  password             = "${mysql_password}"
  parameter_group_name = "default.mysql5.7"
}