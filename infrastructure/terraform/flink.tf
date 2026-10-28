resource "aws_cloudwatch_log_group" "flink" {
  name              = "/aws/kinesis-analytics/${var.flink_application_name}"
  retention_in_days = 1
}

resource "aws_cloudwatch_log_stream" "flink" {
  name           = "application"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

resource "aws_kinesisanalyticsv2_application" "machinepulse" {
  name                   = var.flink_application_name
  runtime_environment    = var.flink_runtime
  service_execution_role = aws_iam_role.flink.arn

  description = "MachinePulse stateful telemetry processing"

  application_configuration {
    application_code_configuration {
      code_content {
        s3_content_location {
          bucket_arn = aws_s3_bucket.machinepulse_data.arn
          file_key   = aws_s3_object.flink_package.key
        }
      }

      code_content_type = "ZIPFILE"
    }

    environment_properties {
      property_group {
        property_group_id = "kinesis.analytics.flink.run.options"

        property_map = {
          python  = "main.py"
          pyFiles = "flink_processor.py,processing.py,kinesis_connector.py"
          jarfile = "lib/pyflink-dependencies.jar"
        }
      }

      property_group {
        property_group_id = "MachinePulseConfig"

        property_map = {
          INPUT_STREAM_ARN      = aws_kinesis_stream.telemetry.arn
          OUTPUT_BUCKET         = aws_s3_bucket.machinepulse_data.bucket
          OUTPUT_BRONZE_PREFIX  = "bronze/"
          OUTPUT_SILVER_PREFIX  = "silver/"
          OUTPUT_GOLD_PREFIX    = "gold/"
          OUTPUT_QUALITY_PREFIX = "quality/"
          AWS_REGION            = var.aws_region
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = 60000
        min_pause_between_checkpoints = 5000
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "APPLICATION"
      }

      parallelism_configuration {
        configuration_type   = "CUSTOM"
        parallelism          = var.flink_parallelism
        parallelism_per_kpu  = 1
        auto_scaling_enabled = false
      }
    }

    application_snapshot_configuration {
      snapshots_enabled = false
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink.arn
  }

  # Provision first; start explicitly after reviewing configuration.
  start_application = false

  depends_on = [
    aws_iam_role_policy.flink_access
  ]
}
