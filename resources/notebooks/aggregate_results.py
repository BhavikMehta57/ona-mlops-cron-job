# Databricks notebook source
"""Validate outputs and publish the final end-to-end result manifest."""

import json
import os
import posixpath
import uuid
from datetime import datetime, timezone
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


run_root = _widget("run_root")
route_manifest = _widget("route_manifest")
selection_manifest = _widget("selection_manifest")
if not run_root or not route_manifest:
    raise ValueError("run_root and route_manifest are required")

with open(_local(route_manifest), "r", encoding="utf-8") as stream:
    routing = json.load(stream)

results = []
missing = []
for route in routing.get("routes", []):
    extraction_root = Path(_local(route["extraction_output_dir"]))
    trait_files = sorted(
        str(path) for path in extraction_root.rglob("*.csv") if path.is_file()
    ) if extraction_root.exists() else []
    inference_root = Path(_local(route["inference_output_dir"]))
    inference_json_count = sum(
        1 for path in inference_root.rglob("*.json") if path.is_file()
    ) if inference_root.exists() else 0
    if not trait_files:
        missing.append(route["group_key"])
    results.append({
        **route,
        "inference_json_count": inference_json_count,
        "trait_result_files": trait_files,
        "status": "succeeded" if trait_files else "missing_trait_results",
    })

if missing:
    raise RuntimeError("Trait extraction returned no CSV results for groups: " + ", ".join(missing))

final_manifest = posixpath.join(run_root.rstrip("/"), "manifests", "final_results.json")
payload = {
    "schema_version": 1,
    "status": "succeeded",
    "run_id": routing.get("run_id"),
    "completed_at": datetime.now(timezone.utc).isoformat(),
    "selection_manifest": selection_manifest,
    "route_manifest": route_manifest,
    "detected_crops": sorted({route["crop"] for route in results}),
    "detected_traits": sorted({route["extraction_trait"] for route in results}),
    "detected_protocols": sorted({route["protocol"] for route in results}),
    "group_count": len(results),
    "groups": results,
}
_atomic_json(final_manifest, payload)
dbutils.jobs.taskValues.set(key="result_manifest", value=final_manifest)
dbutils.jobs.taskValues.set(key="detected_crops", value=payload["detected_crops"])
dbutils.jobs.taskValues.set(key="detected_traits", value=payload["detected_traits"])
dbutils.jobs.taskValues.set(key="detected_protocols", value=payload["detected_protocols"])
print(json.dumps(payload, indent=2))
