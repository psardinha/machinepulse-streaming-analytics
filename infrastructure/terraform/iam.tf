resource "aws_iam_role" "flink" {
  name = "${var.flink_application_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "kinesisanalytics.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "flink_access" {
  name = "${var.flink_application_name}-access"
  role = aws_iam_role.flink.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Read telemetry from the input Kinesis stream.
      {
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:ListShards",
          "kinesis:SubscribeToShard"
        ]
        Resource = aws_kinesis_stream.telemetry.arn
      },

      # Read the packaged Flink application from S3.
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject"
        ]
        Resource = "${aws_s3_bucket.machinepulse_data.arn}/artifacts/*"
      },

      # List the bucket, restricted to the prefixes Flink needs.
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = aws_s3_bucket.machinepulse_data.arn
        Condition = {
          StringLike = {
            "s3:prefix" = [
              "artifacts",
              "artifacts/*",
              "bronze",
              "bronze/*",
              "silver",
              "silver/*",
              "gold",
              "gold/*",
              "quality",
              "quality/*"
            ]
          }
        }
      },

      # Write processed telemetry to S3.
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:AbortMultipartUpload"
        ]
        Resource = [
          "${aws_s3_bucket.machinepulse_data.arn}/bronze/*",
          "${aws_s3_bucket.machinepulse_data.arn}/silver/*",
          "${aws_s3_bucket.machinepulse_data.arn}/gold/*",
          "${aws_s3_bucket.machinepulse_data.arn}/quality/*"
        ]
      },

      # CloudWatch logging permissions.
      {
        Effect = "Allow"
        Action = [
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}
