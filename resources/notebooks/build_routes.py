# Databricks notebook source
"""Convert the selection manifest into dynamic inference/extraction routes."""

import json
import os
import posixpath
import uuid
from pathlib import Path


def _widget(name: str, default: str = "") -> str:
    dbutils.widgets.text(name, default)
    return dbutils.widgets.get(name).strip()


def _local(path: str) -> str:
    return path.removeprefix("dbfs:")


def _atomic_json(path: str, payload: dict) -> None:
    target = Path(_local(path))
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, target)


selection_manifest = _widget("selection_manifest")
run_root = _widget("run_root")
fail_on_unsupported = _widget("fail_on_unsupported_traits", "true").lower() in {
    "1", "true", "yes", "y"
}
firestore_upload = _widget("trait_extraction_firestore_upload", "true").lower() in {
    "1", "true", "yes", "y"
}
if not selection_manifest or not run_root:
    raise ValueError("selection_manifest and run_root are required")

with open(_local(selection_manifest), "r", encoding="utf-8") as stream:
    selection = json.load(stream)

unsupported = selection.get("unsupported_traits") or []
if unsupported and fail_on_unsupported:
    raise ValueError(
        "Unsupported or missing trait values were detected; no data was silently skipped: "
        + ", ".join(str(value) for value in unsupported)
    )

routes = []
for group in selection.get("groups", []):
    if int(group.get("copied_count") or 0) <= 0:
        continue
    group_key = group["group_key"]
    inference_output_dir = posixpath.join(run_root.rstrip("/"), "inference", group_key)
    extraction_output_dir = posixpath.join(run_root.rstrip("/"), "traits", group_key)
    run_spec = {
        "trait": group["extraction_trait"],
        "method": "computer_vision",
        "crop": group["crop"],
        "input_dir": inference_output_dir,
        "output_prefix": extraction_output_dir,
        "recursive": True,
        "extra": {
            "protocol": group["protocol"],
            "validate_only": False,
            "firestore_upload": firestore_upload,
        },
    }
    routes.append({
        "group_key": group_key,
        "crop": group["crop"],
        "protocol": group["protocol"],
        "trait": group["trait"],
        "inference_trait_type": group["inference_trait_type"],
        "extraction_trait": group["extraction_trait"],
        "input_prefix": group["selected_images_dir"],
        "selected_count": int(group["selected_count"]),
        "inference_output_dir": inference_output_dir,
        "extraction_output_dir": extraction_output_dir,
        "run_spec_json": json.dumps(run_spec, separators=(",", ":")),
    })

if not routes:
    raise RuntimeError("Selection completed but produced no copied images to send to inference")

route_manifest = posixpath.join(run_root.rstrip("/"), "manifests", "routes.json")
payload = {
    "schema_version": 1,
    "run_id": selection.get("run_id"),
    "selection_manifest": selection_manifest,
    "unsupported_traits": unsupported,
    "route_count": len(routes),
    "routes": routes,
}
_atomic_json(route_manifest, payload)
dbutils.jobs.taskValues.set(key="routes", value=routes)
dbutils.jobs.taskValues.set(key="route_manifest", value=route_manifest)
dbutils.jobs.taskValues.set(key="route_count", value=len(routes))
print(json.dumps({"route_manifest": route_manifest, "routes": routes}, indent=2))
