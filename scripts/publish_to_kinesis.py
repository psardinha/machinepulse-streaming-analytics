
import argparse
import json
import sys
import time

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from machinepulse.simulator import generate_events


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish simulated MachinePulse telemetry to Kinesis.")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--device-count", type=int, default=10)
    parser.add_argument("--stream-name", default="machinepulse-telemetry-dev")
    parser.add_argument("--region", default="eu-west-1")
    args = parser.parse_args()

    if args.count < 1 or args.device_count < 1:
        parser.error("--count and --device-count must be positive")

    events = generate_events(count=args.count, device_count=args.device_count)
    client = boto3.client("kinesis", region_name=args.region)

    sent = 0
    failed = []

    try:
        # Kinesis PutRecords accepts at most 500 records per request.
        for start in range(0, len(events), 500):
            batch = events[start : start + 500]
            records = [{"Data": event.model_dump_json().encode("utf-8"),
                        "PartitionKey": event.device_id} for event in batch]

            response = client.put_records(StreamName=args.stream_name, Records=records)

            sent += len(batch) - response["FailedRecordCount"]

            for event, result in zip(batch, response["Records"]):
                if "ErrorCode" in result:
                    failed.append(event.event_id, result["ErrorCode"], result.get("ErrorMessage", ""))

        print(f"Accepted by Kinesis: {sent}/{len(events)}")

        if failed:
            for event_id, code, message in failed:
                print(f"FAILED {event_id}: {code}: {message}", file=sys.stderr)
            return 1

        print(f"Stream: {args.stream_name} ({args.region})")
        print(f"Devices: {args.device_count}")
        print("Kinesis accepted all submitted records.")
        return 0

    except (BotoCoreError, ClientError) as exc:
        print(f"AWS request failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
