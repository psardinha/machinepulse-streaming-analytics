
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "MachinePulse"
      Environment = "dev"
      ManagedBy   = "Terraform"
    }
  }
}
