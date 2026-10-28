variable "aws_region" {
  description = "AWS region for MachinePulse"
  type        = string
  default     = "eu-west-1"
}

variable "stream_name" {
  description = "Input Kinesis stream name"
  type        = string
  default     = "machinepulse-telemetry-dev"
}

variable "data_bucket_name" {
  description = "Globally unique S3 bucket name for MachinePulse data and artifacts"
  type        = string
  default     = "machinepulse-data-psardinha"
}

variable "flink_application_name" {
  description = "Managed Flink application name"
  type        = string
  default     = "machinepulse-flink-dev"
}

variable "flink_runtime" {
  description = "Managed Apache Flink runtime"
  type        = string
  default     = "FLINK-2_2"
}

variable "flink_package_path" {
  description = "Local path to the packaged Flink application ZIP"
  type        = string
  default     = "../../flink_app/machinepulse-flink.zip"
}

variable "flink_package_filename" {
  description = "S3 object filename for the packaged Flink application"
  type        = string
  default     = "machinepulse-flink.zip"
}

variable "flink_parallelism" {
  description = "Initial Flink parallelism"
  type        = number
  default     = 1

  validation {
    condition     = var.flink_parallelism >= 1
    error_message = "Flink parallelism must be at least 1."
  }
}
