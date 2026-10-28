# Uncomment and fill in following instructions to have Terraform state stored in S3

#terraform {
#  backend "s3" {
#    bucket       = "<S3-BUCKET-TO-DEFINE>"
#    key          = "machinepulse/dev/terraform.tfstate"
#    region       = "eu-west-1"
#    encrypt      = true
#    use_lockfile = true
#  }
#}
