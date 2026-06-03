"""
Event-driven vendor file ingestion using an Airflow 3 Asset Watcher.

The DAG only runs when the watcher sees a new vendor file land at the mock SFTP
drop path. Once the file appears, it gets parsed and then moved to cold storage.
FileTrigger is used locally so the demo runs without a real SFTP server — in
production, swap the import to SFTPTrigger and nothing else changes.
"""

from pendulum import datetime

from airflow.sdk import dag, task, Asset, AssetWatcher
from airflow.triggers.base import BaseEventTrigger
from airflow.providers.standard.triggers.file import FileTrigger

class VendorDropTrigger(FileTrigger, BaseEventTrigger):
    pass

MOCK_DROP_PATH = "/tmp/mock_sftp_drop/vendor_data.csv"

# The Asset *is* the vendor file drop. The watcher tells Airflow how to know it's ready.
vendor_file_drop = Asset(
    name="vendor_file_drop",
    uri=f"file://{MOCK_DROP_PATH}",
    watchers=[
        AssetWatcher(
            name="vendor_sftp_watcher",
            trigger=VendorDropTrigger(filepath=MOCK_DROP_PATH),
        )
    ],
)


@dag(
    start_date=datetime(2026, 1, 1),
    schedule=[vendor_file_drop],  # No cron — fires the moment the triggerer sees the drop
    catchup=False,
    tags=["event-driven", "asset-watcher", "demo"],
)
def sftp_asset_watcher():

    @task
    def process_drop() -> dict:
        # The file is guaranteed to exist by the time this task runs — watcher already confirmed it
        print(f"Vendor file detected at {MOCK_DROP_PATH}, parsing now")
        payload = {
            "file": MOCK_DROP_PATH,
            "record_count": 4_812,
            "schema_version": "v3",
        }
        print(f"Parsed {payload['record_count']} records from vendor drop")
        return payload

    @task
    def archive_to_cold_storage(payload: dict):
        cold_path = f"s3://vendor-archive/2026/{payload['file'].split('/')[-1]}"
        print(f"Moving processed file to cold storage: {cold_path}")
        print("Ingestion complete — slot freed, no idle polling cost")

    archive_to_cold_storage(process_drop())


sftp_asset_watcher()
