## AWS account level config: region
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-west-2"
}

## Key to allow connection to our EC2 instance
variable "key_name" {
  description = "EC2 key name"
  type        = string
  default     = "sde-key"
}

## EC2 instance type
variable "instance_type" {
  description = "Instance type for EMR and EC2"
  type        = string
  default     = "m4.xlarge"
}

## Alert email receiver
variable "alert_email_id" {
  description = "Email id to send alerts to "
  type        = string
  default     = "abahonuh@gmail.com"
}

## Your repository url
variable "repo_url" {
  description = "Repository url to clone into production machine"
  type        = string
  default     = "https://github.com/josephmachado/Stock-Data-News-Sentiment-Pipeline.git"
}

variable "aws_s3_bucket" {
  description = "AWS S3 bucket name"
  type        = string
  default     = "stock-data-etl-kjsj353kl3j4"
}

variable "aws_access_key" {}
variable "aws_secret_key" {}
variable "name" { default = "dynamic-aws-creds-vault-admin" }
