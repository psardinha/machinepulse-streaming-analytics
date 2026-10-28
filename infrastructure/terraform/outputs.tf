output "kinesis_stream_name" {
  description = "Telemetry input stream"
  value       = aws_kinesis_stream.telemetry.name
}

output "flink_application_name" {
  description = "Managed Flink application"
  value       = aws_kinesisanalyticsv2_application.machinepulse.name
}

output "flink_application_arn" {
  description = "Managed Flink application ARN"
  value       = aws_kinesisanalyticsv2_application.machinepulse.arn
}

output "flink_code_bucket" {
  description = "S3 bucket containing the Flink application package"
  value       = aws_s3_bucket.machinepulse_data.bucket
}

output "processed_data_bucket" {
  description = "S3 bucket containing processed data"
  value       = aws_s3_bucket.machinepulse_data.bucket
}
