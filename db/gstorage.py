from __future__ import annotations

from google.cloud import storage
from google.oauth2 import service_account

from cron_job.core.config import AppConfig


def get_storage_client(config: AppConfig) -> storage.Client:
    if config.gcp_service_account_credentials:
        creds = service_account.Credentials.from_service_account_info(
            config.gcp_service_account_credentials,
        )
        return storage.Client(project=config.gcp_project_id, credentials=creds)
    return storage.Client(project=config.gcp_project_id)


def get_bucket(config: AppConfig, client: storage.Client | None = None):
    client = client or get_storage_client(config)
    return client.bucket(config.gcs_bucket)
