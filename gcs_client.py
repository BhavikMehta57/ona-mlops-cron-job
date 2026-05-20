from __future__ import annotations

from typing import Iterable


def parse_gcs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Not a GCS URI: {uri}")
    bucket_and_path = uri[5:]
    bucket_name, sep, blob_name = bucket_and_path.partition("/")
    if not bucket_name or not sep or not blob_name:
        raise ValueError(f"GCS URI must include bucket and object path: {uri}")
    return bucket_name, blob_name


def make_gcs_uri(bucket_name: str, blob_name: str) -> str:
    return f"gs://{bucket_name}/{blob_name.lstrip('/')}"


def upload_bytes(client, uri: str, data: bytes, *, content_type: str = "application/octet-stream") -> None:
    bucket_name, blob_name = parse_gcs_uri(uri)
    blob = client.bucket(bucket_name).blob(blob_name)
    blob.upload_from_string(data, content_type=content_type)


def upload_text(client, uri: str, text: str, *, content_type: str = "text/plain") -> None:
    upload_bytes(client, uri, text.encode("utf-8"), content_type=content_type)


def download_bytes(client, uri: str) -> bytes:
    bucket_name, blob_name = parse_gcs_uri(uri)
    return client.bucket(bucket_name).blob(blob_name).download_as_bytes()


def download_text(client, uri: str) -> str:
    return download_bytes(client, uri).decode("utf-8")


def delete_blob(client, uri: str) -> None:
    bucket_name, blob_name = parse_gcs_uri(uri)
    client.bucket(bucket_name).blob(blob_name).delete()


def blob_exists(client, uri: str) -> bool:
    bucket_name, blob_name = parse_gcs_uri(uri)
    return bool(client.bucket(bucket_name).blob(blob_name).exists())


def list_blob_uris(client, bucket_name: str, prefix: str) -> list[str]:
    return [
        make_gcs_uri(bucket_name, blob.name)
        for blob in client.bucket(bucket_name).list_blobs(prefix=prefix)
    ]


def safe_delete_many(client, uris: Iterable[str]) -> None:
    for uri in uris:
        try:
            delete_blob(client, uri)
        except Exception:
            pass
