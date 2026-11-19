terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project = "qBachTorRick"
    }
  }
}

variable "aws_region" {
  description = "AWS region in which to create the resources."
  type        = string
  default     = "us-west-1"
}

variable "instance_type" {
  description = "EC2 instance type compatible with the amd64 Ubuntu image."
  type        = string
  default     = "t3.micro"
}

variable "ssh_key_name" {
  description = "Name for the EC2 SSH key pair created from your public key."
  type        = string
  default     = "qbachtorrick"
}

variable "ssh_public_key_path" {
  description = "Path to an existing SSH public key on the machine running OpenTofu."
  type        = string
}

variable "ssh_cidr" {
  description = "IPv4 CIDR allowed to connect over SSH, such as your public IP followed by /32."
  type        = string

  validation {
    condition     = can(cidrnetmask(var.ssh_cidr))
    error_message = "ssh_cidr must be a valid IPv4 CIDR."
  }
}

variable "dht_port" {
  description = "Public UDP DHT port; use the same port in each node's config.json."
  type        = number
  default     = 6881

  validation {
    condition     = var.dht_port >= 1 && var.dht_port <= 65535 && floor(var.dht_port) == var.dht_port
    error_message = "dht_port must be in [1, 65535]."
  }
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_key_pair" "dht" {
  key_name   = var.ssh_key_name
  public_key = file(pathexpand(var.ssh_public_key_path))
}

resource "aws_vpc" "dht" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
}

resource "aws_subnet" "public" {
  vpc_id     = aws_vpc.dht.id
  cidr_block = "10.0.1.0/24"
}

resource "aws_internet_gateway" "dht" {
  vpc_id = aws_vpc.dht.id
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.dht.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.dht.id
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "dht" {
  name_prefix = "qbachtorrick-"
  description = "Public DHT traffic and restricted SSH access"
  vpc_id      = aws_vpc.dht.id
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  security_group_id = aws_security_group.dht.id
  description       = "SSH from the configured administrator network"
  cidr_ipv4         = var.ssh_cidr
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
}

resource "aws_vpc_security_group_ingress_rule" "dht" {
  security_group_id = aws_security_group.dht.id
  description       = "Public DHT UDP traffic"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "udp"
  from_port         = var.dht_port
  to_port           = var.dht_port
}

resource "aws_vpc_security_group_egress_rule" "outbound" {
  security_group_id = aws_security_group.dht.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_instance" "dht" {
  count = 2

  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public.id
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.dht.id]
  key_name                    = aws_key_pair.dht.key_name
  user_data                   = file("${path.module}/bootstrap.sh")

  tags = {
    Name = "qbachtorrick-${count.index + 1}"
  }

  depends_on = [aws_route_table_association.public]
}

output "public_ips" {
  description = "Public IPv4 addresses of the two DHT instances, in node order."
  value       = aws_instance.dht[*].public_ip
}
