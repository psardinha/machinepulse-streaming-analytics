# Generate a suffix to help ensure the S3 bucket name is unique.
# resource "random_id" "bucket_suffix" {
#   byte_length = 4
# }

# Shared application-data bucket.
# Terraform state remains in your separate remote state bucket.
resource "aws_s3_bucket" "machinepulse_data" {
  #bucket = "${var.data_bucket_name}-${random_id.bucket_suffix.hex}"
  bucket = var.data_bucket_name

  lifecycle {
    prevent_destroy = true
  }
}

# Encrypt objects at rest using S3-managed encryption.
resource "aws_s3_bucket_server_side_encryption_configuration" "machinepulse_data" {
  bucket = aws_s3_bucket.machinepulse_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Prevent public access to the bucket and its objects.
resource "aws_s3_bucket_public_access_block" "machinepulse_data" {
  bucket = aws_s3_bucket.machinepulse_data.id

  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

# Upload the packaged Apache Flink application.
resource "aws_s3_object" "flink_package" {
  bucket = aws_s3_bucket.machinepulse_data.id
  key    = "artifacts/${var.flink_package_filename}"
  source = var.flink_package_path

  # Detect local package changes and trigger an upload.
  etag = filemd5(var.flink_package_path)
}
