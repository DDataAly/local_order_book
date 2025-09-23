provider "aws" {
  region = "eu-west-2"
}

# Need to configure AWS credentials as environmental variables
# Not sure if data block is needed
data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  owners = ["099720109477"] # Canonical
}

# This block specifies what we create in AWS - need to change to create an S3 bucket not EC2
resource "aws_instance" "app_server" {
  ami           = data.aws_ami.ubuntu.id
  instance_type = "t2.micro"

  tags = {
    Name = "learn-terraform"
  }
}