"""
Unified CLI for the MayaCardio Bedside Data Pipeline.
Implements the exact required commands from Section 4 of the assessment:
1. pipeline status
2. pipeline export --session ID --from device|cloud
3. pipeline results --session ID
4. pipeline daemon / run
"""

import argparse
import json
import os
import sys
import requests

from mayacardio.storage import StorageEngine
from mayacardio.sync import CloudSynchronizer
from mayacardio.ingestion import IngestionEngine


def get_default_storage() -> StorageEngine:
    db_path = os.getenv("DEVICE_DB_PATH", "mayacardio.db")
    return StorageEngine(db_path)


def cmd_status(args):
    """
    pipeline status:
    Prints exactly one JSON object on one line containing:
    mode, samples_saved, samples_in_cloud, backlog.
    """
    storage = get_default_storage()
    cloud_url = os.getenv("CLOUD_URL", "http://127.0.0.1:8000")
    synchronizer = CloudSynchronizer(storage, cloud_url=cloud_url)

    # Determine mode via probe
    current_mode = synchronizer.update_connectivity()

    samples_saved = storage.get_total_saved_samples()
    backlog = storage.get_backlog_count()

    samples_in_cloud = 0
    if current_mode == "online":
        try:
            res = requests.get(f"{cloud_url}/api/status", timeout=1.5)
            if res.status_code == 200:
                cloud_data = res.json()
                samples_in_cloud = cloud_data.get("total_samples", max(0, samples_saved - backlog))
            else:
                samples_in_cloud = max(0, samples_saved - backlog)
        except Exception:
            samples_in_cloud = max(0, samples_saved - backlog)
    else:
        samples_in_cloud = max(0, samples_saved - backlog)

    output = {
        "mode": current_mode,
        "samples_saved": samples_saved,
        "samples_in_cloud": samples_in_cloud,
        "backlog": backlog,
    }
    # Print exactly one JSON line
    sys.stdout.write(json.dumps(output) + "\n")
    sys.stdout.flush()


def cmd_export(args):
    """
    pipeline export --session ID --from device|cloud
    Outputs JSON lines: samples in index order, then loss records.
    """
    session_id = args.session
    source = args.source.lower()

    if source == "device":
        storage = get_default_storage()
        for item in storage.export_samples_and_losses(session_id):
            sys.stdout.write(json.dumps(item) + "\n")
        sys.stdout.flush()

    elif source == "cloud":
        cloud_url = os.getenv("CLOUD_URL", "http://127.0.0.1:8000")
        try:
            res = requests.get(
                f"{cloud_url}/api/export",
                params={"session_id": session_id},
                stream=True,
                timeout=10.0,
            )
            if res.status_code != 200:
                sys.stderr.write(f"Error fetching cloud export: HTTP {res.status_code}\n")
                sys.exit(1)
            for line in res.iter_lines():
                if line:
                    sys.stdout.write(line.decode("utf-8") + "\n")
            sys.stdout.flush()
        except Exception as e:
            sys.stderr.write(f"Failed to connect to cloud service: {e}\n")
            sys.exit(1)
    else:
        sys.stderr.write(f"Invalid --from target: {source}. Must be 'device' or 'cloud'.\n")
        sys.exit(1)


def cmd_results(args):
    """
    pipeline results --session ID
    Outputs one JSON line per window result record.
    """
    session_id = args.session
    storage = get_default_storage()

    has_results = False
    for res in storage.get_results(session_id):
        sys.stdout.write(json.dumps(res) + "\n")
        has_results = True

    # If no local results, try querying cloud results
    if not has_results:
        cloud_url = os.getenv("CLOUD_URL", "http://127.0.0.1:8000")
        try:
            res = requests.get(
                f"{cloud_url}/api/results",
                params={"session_id": session_id},
                stream=True,
                timeout=5.0,
            )
            if res.status_code == 200:
                for line in res.iter_lines():
                    if line:
                        sys.stdout.write(line.decode("utf-8") + "\n")
        except Exception:
            pass

    sys.stdout.flush()


def cmd_run(args):
    """Runs the ingestion pipeline and synchronization worker daemon."""
    storage = get_default_storage()
    cloud_url = os.getenv("CLOUD_URL", "http://127.0.0.1:8000")
    mock_host = os.getenv("MOCK_HOST", "127.0.0.1")
    mock_port = int(os.getenv("MOCK_PORT", "7100"))

    sync = CloudSynchronizer(storage, cloud_url=cloud_url)
    engine = IngestionEngine(storage, sync, mock_host=mock_host, mock_port=mock_port)

    import signal

    def sig_handler(signum, frame):
        engine.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, sig_handler)

    engine.run()


def main():
    parser = argparse.ArgumentParser(
        prog="pipeline",
        description="MayaCardio Bedside Data Pipeline CLI",
    )
    subparsers = parser.add_subparsers(dest="command")

    # status
    parser_status = subparsers.add_parser("status", help="Print pipeline status as one JSON line")
    parser_status.set_defaults(func=cmd_status)

    # export
    parser_export = subparsers.add_parser("export", help="Export session data as JSON lines")
    parser_export.add_argument("--session", required=True, help="Session identifier")
    parser_export.add_argument(
        "--from",
        dest="source",
        choices=["device", "cloud"],
        default="device",
        help="Source: 'device' or 'cloud'",
    )
    parser_export.set_defaults(func=cmd_export)

    # results
    parser_results = subparsers.add_parser("results", help="Print window processing results as JSON lines")
    parser_results.add_argument("--session", required=True, help="Session identifier")
    parser_results.set_defaults(func=cmd_results)

    # run / daemon
    parser_run = subparsers.add_parser("run", help="Launch continuous acquisition pipeline")
    parser_run.set_defaults(func=cmd_run)

    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
