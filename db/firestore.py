from __future__ import annotations

from typing import Any

import firebase_admin
from firebase_admin import credentials, firestore

from cron_job.core.config import AppConfig


def initialize_firebase_app(config: AppConfig) -> firebase_admin.App:
    """Initialize Firebase Admin lazily and return the default app."""
    try:
        return firebase_admin.get_app()
    except ValueError:
        pass

    options: dict[str, Any] = {"storageBucket": config.firebase_storage_bucket}
    if config.firebase_credentials:
        cred = credentials.Certificate(config.firebase_credentials)
        return firebase_admin.initialize_app(cred, options)
    return firebase_admin.initialize_app(options=options)


def get_firestore_client(config: AppConfig):
    initialize_firebase_app(config)
    return firestore.client(database_id=config.firestore_database_id)


def get_batch_firestore_module():
    return firestore
