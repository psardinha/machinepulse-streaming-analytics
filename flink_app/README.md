# Building the AWS Flink application package

The Parquet format factory needs Hadoop's `org.apache.hadoop.conf.Configuration`
at runtime. The Maven dependencies in `pom.xml` include `hadoop-common`; the
class must also be present in the dependency JAR inside the ZIP uploaded to AWS.
The build excludes Apache Commons CLI so Flink uses its own copy; bundling a
second copy can cause a class-loader `LinkageError`.

Build and overwrite the existing application package from the current Python
sources and Maven dependencies:

```bash
cd flink_app
./package.sh
```

The script checks that the generated dependency JAR contains Hadoop's
`Configuration` class before writing `machinepulse-flink.zip`. Terraform keeps
using its existing package path and S3 object filename. Apply Terraform to
upload the rewritten ZIP, then restart the Flink application so it uses the
updated package.

The Managed Flink `pyFiles` run option must list every Python module imported
by the job (`flink_processor.py`, `processing.py`, and `kinesis_connector.py`).

The job writes each source payload unchanged as a string to the Bronze Parquet
sink, alongside its UTC processing/ingestion timestamp. The Silver sink
continues to contain only validated, deduplicated telemetry. Gold contains
detected anomalies under `gold/anomalies/` and per-device five-minute
processing-time KPIs under `gold/device_kpis_5m/` (reading count, average and
peak temperature, and average and peak power). KPI rows are emitted when a
five-minute window closes. Quality contains invalid and duplicate records.

The validator and keyed processor tag their output records as JSON. The
pipeline branches these tagged streams with filters, rather than relying on
Python process-function side-output APIs that are unavailable in the deployed
PyFlink runtime.
