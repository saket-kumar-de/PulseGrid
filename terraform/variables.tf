# Short name used as a prefix for every resource this project creates
variable "project_name" {
  description = "Project name, used as a resource name prefix"
  type        = string
  default     = "pulsegrid"
}

variable "environment" {
  description = "Deployment environment (dev, prod, etc.)"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "ap-south-1"
}
