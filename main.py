"""CLI entrypoint for the Databricks end-to-end pipeline orchestrator.

Runs the five existing Databricks jobs in order (Batching -> QScoring ->
Selection -> Batch Inference -> Trait Extraction), polling each to completion.

Usage:
    python -m cron_job.main
    python -m cron_job.main --run-id my-run-001
    python -m cron_job.main --smoke-test

Databricks auth is resolved by the SDK from DATABRICKS_HOST/DATABRICKS_TOKEN,
a CLI profile (DATABRICKS_CONFIG_PROFILE), or the notebook/job context.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timezone

from cron_job.core.config import AppConfig, load_config
from cron_job.middleware.logger import get_logger


def _generate_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"e2e-{stamp}-{uuid.uuid4().hex[:8]}"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ONA Databricks end-to-end pipeline orchestrator")
    parser.add_argument("--run-id", default=None, help="Optional run identifier for logs.")
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="Validate config and Databricks connectivity without triggering jobs.",
    )
    return parser


def _smoke_test(config: AppConfig, logger) -> int:
    from databricks.sdk import WorkspaceClient

    kwargs = {}
    if config.databricks_host:
        kwargs["host"] = config.databricks_host
    if config.databricks_profile:
        kwargs["profile"] = config.databricks_profile
    client = WorkspaceClient(**kwargs)
    me = client.current_user.me()
    logger.log(
        "smoke_test",
        "success",
        databricks_user=getattr(me, "user_name", None),
        stages=[
            {
                "key": spec.key,
                "kind": spec.kind,
                "announce_name": spec.announce_name,
                "job_id": spec.job_id,
            }
            for spec in config.ordered_stages
        ],
    )
    return 0


def run(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    run_id = args.run_id or _generate_run_id()
    logger = get_logger(run_id)

    try:
        config = load_config()
    except Exception as exc:  # noqa: BLE001 - fatal bootstrap error
        logger.log_exception("bootstrap", "fatal", exc)
        return 1

    if args.smoke_test:
        try:
            return _smoke_test(config, logger)
        except Exception as exc:  # noqa: BLE001
            logger.log_exception("smoke_test", "failed", exc)
            return 1

    # Imported lazily so --smoke-test / --help work without the SDK installed.
    from cron_job.orchestrator.pipeline import DatabricksPipeline

    pipeline = DatabricksPipeline(config, logger)
    result = pipeline.run(run_id)
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(run())
