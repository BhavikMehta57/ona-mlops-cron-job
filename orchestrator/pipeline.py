"""Imperative Databricks pipeline runner.

Triggers the five existing Databricks jobs in order using the Databricks SDK and
polls each run to completion. This mirrors the role of the declarative bundle
workflow (resources/end_to_end_pipeline.job.yml) but is handy for dynamic,
per-run triggering from a script or another job (similar to the old
`gcloud run jobs execute` loop).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from cron_job.core.config import (
    AppConfig,
    ShapeComparison,
    StageSpec,
    compare_pipeline_shapes,
    expected_bundle_config,
)
from cron_job.middleware.logger import StructuredLogger


TERMINAL_SUCCESS = {"SUCCESS"}
TERMINAL_FAILED = {"FAILED", "TIMEDOUT", "CANCELED", "CANCELLED", "ERROR"}

# Required Batch_Inference job parameters. If any is missing (key absent or
# value None) the stage must not trigger the inference job (Req 2.2, 2.3).
REQUIRED_INFERENCE_PARAMS = (
    "run_id",
    "input_prefix",
    "input_mode",
    "save_json_folder",
    "trait_type",
    "confidence",
    "limit",
    "num_partitions",
)


@dataclass
class StageResult:
    stage: str
    job_id: str
    run_id: int | None
    success: bool
    state: str
    run_page_url: str | None = None
    error: str | None = None


@dataclass
class PipelineResult:
    run_id: str
    stages: list[StageResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return bool(self.stages) and all(stage.success for stage in self.stages)


class DatabricksPipeline:
    """Runs the end-to-end pipeline by chaining existing Databricks jobs."""

    def __init__(
        self,
        config: AppConfig,
        logger: StructuredLogger,
        client: WorkspaceClient | None = None,
    ) -> None:
        self._config = config
        self._logger = logger
        self._client = client or self._build_client(config)

    @staticmethod
    def _build_client(config: AppConfig) -> WorkspaceClient:
        kwargs: dict[str, Any] = {}
        if config.databricks_host:
            kwargs["host"] = config.databricks_host
        if config.databricks_profile:
            kwargs["profile"] = config.databricks_profile
        return WorkspaceClient(**kwargs)

    def _params_for_stage(self, stage: str) -> dict[str, str] | None:
        if stage == "batch_inference":
            return {k: str(v) for k, v in self._config.inference_params.items()}
        if stage == "trait_extraction":
            return {
                "run_spec_json": json.dumps(
                    self._config.trait_extraction_run_spec,
                    separators=(",", ":"),
                    ensure_ascii=True,
                )
            }
        return None

    def _validate_real_stage_params(self, stage: str) -> str | None:
        """Validate required parameters for a real stage before triggering.

        Returns an error message identifying the missing/empty parameter when
        the stage cannot be triggered, or ``None`` when the parameters are
        valid.

        - Batch_Inference: every parameter in :data:`REQUIRED_INFERENCE_PARAMS`
          must be present (key exists) and non-``None`` (Req 2.2, 2.3).
        - Trait_Extraction: ``run_spec_json`` must be present (the run spec is a
          non-empty mapping) and its ``input_dir`` field must be a non-empty,
          non-whitespace string (Req 3.3, 3.4).
        """
        if stage == "batch_inference":
            params = self._config.inference_params or {}
            for name in REQUIRED_INFERENCE_PARAMS:
                if name not in params or params[name] is None:
                    return f"missing required inference parameter: {name}"
            return None

        if stage == "trait_extraction":
            run_spec = self._config.trait_extraction_run_spec
            if not run_spec:
                return "run_spec_json is absent; input_dir is missing"
            input_dir = run_spec.get("input_dir")
            if not isinstance(input_dir, str) or not input_dir.strip():
                return (
                    "input_dir is missing or empty in run_spec_json: "
                    f"{input_dir!r}"
                )
            return None

        return None

    def _run_stage(self, stage: str, job_id: str, run_id: str) -> StageResult:
        param_error = self._validate_real_stage_params(stage)
        if param_error is not None:
            self._logger.log(
                stage,
                "failed",
                run_id=run_id,
                job_id=job_id,
                state="PARAM_MISSING",
                errors=param_error,
            )
            return StageResult(stage, job_id, None, False, "PARAM_MISSING", error=param_error)

        job_parameters = self._params_for_stage(stage)
        self._logger.log(
            stage,
            "started",
            run_id=run_id,
            job_id=job_id,
            job_parameters=job_parameters,
        )
        try:
            waiter = self._client.jobs.run_now(
                job_id=int(job_id),
                job_parameters=job_parameters,
            )
            triggered_run_id = waiter.run_id
        except Exception as exc:  # noqa: BLE001 - surface any SDK/trigger error
            self._logger.log_exception(stage, "failed", exc, run_id=run_id, job_id=job_id)
            return StageResult(stage, job_id, None, False, "TRIGGER_FAILED", error=str(exc))

        state, result_state, page_url, error = self._poll_run(triggered_run_id)
        success = result_state in TERMINAL_SUCCESS
        self._logger.log(
            stage,
            "success" if success else "failed",
            run_id=run_id,
            job_id=job_id,
            triggered_run_id=triggered_run_id,
            life_cycle_state=state,
            result_state=result_state,
            run_page_url=page_url,
            errors=error,
        )
        return StageResult(
            stage=stage,
            job_id=job_id,
            run_id=triggered_run_id,
            success=success,
            state=result_state or state,
            run_page_url=page_url,
            error=error,
        )

    def _run_placeholder(self, spec: StageSpec, run_id: str) -> StageResult:
        """Run an inert placeholder stage.

        The stage only announces its name and always succeeds without ever
        touching the Databricks SDK (``run_now`` is never called). Announcing is
        wrapped in a broad guard so a logging/announce failure never fails the
        stage (Req 1.5, 1.6, 1.7).
        """
        self._logger.log(spec.key, "started", run_id=run_id)

        # The announce must never fail the stage; swallow any logging error.
        try:
            self._logger.log(
                spec.key,
                "announce",
                run_id=run_id,
                announce_name=spec.announce_name,
            )
        except Exception:  # noqa: BLE001 - announce failure must never fail the stage
            pass

        self._logger.log(spec.key, "success", run_id=run_id, state="PLACEHOLDER_SUCCESS")
        return StageResult(
            stage=spec.key,
            job_id="",
            run_id=None,
            success=True,
            state="PLACEHOLDER_SUCCESS",
        )

    def _poll_run(self, triggered_run_id: int) -> tuple[str, str | None, str | None, str | None]:
        started = time.monotonic()
        while True:
            if time.monotonic() - started > self._config.poll_timeout_s:
                return "TIMEOUT", "TIMEDOUT", None, "poll timeout"

            run = self._client.jobs.get_run(run_id=triggered_run_id)
            state = run.state
            page_url = run.run_page_url
            life_cycle = state.life_cycle_state.value if state and state.life_cycle_state else ""
            result_state = state.result_state.value if state and state.result_state else None
            message = state.state_message if state else None

            if life_cycle in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
                error = None if result_state in TERMINAL_SUCCESS else (message or result_state)
                return life_cycle, result_state, page_url, error

            time.sleep(self._config.poll_interval_s)

    def validate_consistency(self, run_id: str | None = None) -> ShapeComparison:
        """Compare the runner's pipeline shape against the bundle's expected shape.

        Derives the runner shape from ``self._config`` and compares it against
        the canonical Bundle_Job shape (:func:`expected_bundle_config`). On
        divergence, logs an error identifying the divergent path so the caller
        can gate on the returned :class:`ShapeComparison` and abort before any
        stage runs (Req 5.5, 6.7).
        """
        runner_shape = self._config.pipeline_shape
        bundle_shape = expected_bundle_config()
        result = compare_pipeline_shapes(runner_shape, bundle_shape)
        if not result.ok:
            self._logger.log(
                "pipeline",
                "failed",
                run_id=run_id,
                errors=result.error,
            )
        return result

    def run(self, run_id: str) -> PipelineResult:
        """Run all stages sequentially, stopping at the first failure.

        Before any stage runs, the runner's shape is checked against the
        bundle's expected shape; on divergence the run is aborted with no stages
        executed (Req 5.5, 6.7).
        """
        result = PipelineResult(run_id=run_id)
        self._logger.log("pipeline", "started", run_id=run_id)

        consistency = self.validate_consistency(run_id=run_id)
        if not consistency.ok:
            # Shapes diverge: abort before triggering or running any stage.
            return result

        for spec in self._config.ordered_stages:
            if spec.kind == "placeholder":
                stage_result = self._run_placeholder(spec, run_id)
            else:
                stage_result = self._run_stage(spec.key, spec.job_id, run_id)
            result.stages.append(stage_result)
            if not stage_result.success:
                self._logger.log(
                    "pipeline",
                    "failed",
                    run_id=run_id,
                    failed_stage=spec.key,
                    errors=stage_result.error,
                )
                return result

        self._logger.log("pipeline", "succeeded", run_id=run_id)
        return result
