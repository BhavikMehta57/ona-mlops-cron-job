from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Literal

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - python-dotenv is optional
    load_dotenv = None


class ConfigError(Exception):
    """Raised when orchestrator configuration is invalid.

    Signals that ``load_config`` refused to build an ``AppConfig`` because a
    value failed validation (e.g. an empty ``save_json_folder``) or because a
    derived invariant could not be satisfied (e.g. ``input_dir`` not matching
    ``save_json_folder``). When raised, no partially-mutated configuration is
    returned; the caller retains whatever configuration it already held.
    """


def _int(value: str | None, default: int) -> int:
    if value is None or value == "":
        return default
    return int(value)


def _float(value: str | None, default: float) -> float:
    if value is None or value == "":
        return default
    return float(value)


def _bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


# Default job IDs for the existing Databricks jobs that make up the pipeline.
DEFAULT_INFERENCE_JOB_ID = "883613979748353"
DEFAULT_TRAIT_EXTRACTION_JOB_ID = "412831839181191"


# A stage is either an inert name-announcing placeholder or a real Databricks job.
StageKind = Literal["placeholder", "real"]


@dataclass(frozen=True)
class StageSpec:
    """Descriptor for a single pipeline stage in execution order.

    ``placeholder`` stages announce ``announce_name`` and never trigger a job,
    so their ``job_id`` is ``None``. ``real`` stages trigger the Databricks job
    identified by ``job_id`` and carry no ``announce_name``.
    """

    key: str
    kind: StageKind
    announce_name: str | None
    job_id: str | None


@dataclass(frozen=True)
class AppConfig:
    """Configuration for the Databricks end-to-end pipeline orchestrator.

    Authentication for the Databricks SDK is resolved by the SDK itself from the
    standard sources (``DATABRICKS_HOST``/``DATABRICKS_TOKEN`` env vars, a CLI
    profile named by ``DATABRICKS_CONFIG_PROFILE``, or a workspace notebook
    context). No secrets are stored on this object.
    """

    # Databricks connection (optional; SDK falls back to its own resolution).
    databricks_host: str | None = None
    databricks_profile: str | None = None

    # Job IDs for the individual pipeline stages.
    inference_job_id: str = DEFAULT_INFERENCE_JOB_ID
    trait_extraction_job_id: str = DEFAULT_TRAIT_EXTRACTION_JOB_ID

    # Polling controls for triggered job runs.
    poll_interval_s: int = 30
    poll_timeout_s: int = 24 * 60 * 60

    # Batch Inference parameters (passed as named job parameters).
    inference_params: dict[str, Any] = field(default_factory=dict)

    # Trait Extraction run spec (serialised to run_spec_json).
    trait_extraction_run_spec: dict[str, Any] = field(default_factory=dict)

    @property
    def ordered_stages(self) -> list[StageSpec]:
        """The fixed five-stage pipeline in execution order.

        The three preprocessing stages are inert placeholders that only announce
        their names; Batch Inference and Trait Extraction are the real jobs.
        """
        return [
            StageSpec(
                key="preprocessing_batching",
                kind="placeholder",
                announce_name="Preprocessing - Batching",
                job_id=None,
            ),
            StageSpec(
                key="preprocessing_qscoring",
                kind="placeholder",
                announce_name="Preprocessing - QScoring",
                job_id=None,
            ),
            StageSpec(
                key="preprocessing_selection",
                kind="placeholder",
                announce_name="Preprocessing - Selection",
                job_id=None,
            ),
            StageSpec(
                key="batch_inference",
                kind="real",
                announce_name=None,
                job_id=self.inference_job_id,
            ),
            StageSpec(
                key="trait_extraction",
                kind="real",
                announce_name=None,
                job_id=self.trait_extraction_job_id,
            ),
        ]

    @property
    def pipeline_shape(self) -> "PipelineShape":
        """The :class:`PipelineShape` implied by this configuration.

        Convenience accessor delegating to :func:`shape_from_config`, used by the
        cross-path consistency check (Req 5.5, 6.7).
        """
        return shape_from_config(self)


@dataclass(frozen=True)
class PipelineShape:
    """Structural fingerprint of the pipeline used for cross-path consistency.

    A ``PipelineShape`` captures only the parts of the pipeline that must agree
    between the declarative Bundle_Job and the imperative Python_Runner (Req
    5.5, 6.7): the ordered stage keys, which stages are inert placeholders, and
    the real Databricks job IDs for the two stages that trigger jobs. Comparing
    two shapes tells the orchestrator whether either execution path diverges.
    """

    ordered_keys: tuple[str, ...]
    placeholder_keys: frozenset[str]
    real_job_ids: dict[str, str]


@dataclass(frozen=True)
class ShapeComparison:
    """Outcome of comparing two :class:`PipelineShape` values.

    ``ok`` is ``True`` only when the two shapes agree on stage order, placeholder
    set, and the real job IDs for ``batch_inference`` / ``trait_extraction``. On
    disagreement, ``ok`` is ``False`` and ``error`` is a human-readable message
    naming the divergent field and identifying which side/path differs.
    """

    ok: bool
    error: str | None = None


# The two real stages whose job IDs must agree across both execution paths.
_REAL_STAGE_KEYS = ("batch_inference", "trait_extraction")


def compare_pipeline_shapes(runner: PipelineShape, bundle: PipelineShape) -> ShapeComparison:
    """Compare two pipeline shapes and report the first divergence, if any.

    This is a pure function: it inspects ``runner`` (the shape derived from the
    Python_Runner configuration) and ``bundle`` (the shape the Bundle_Job is
    expected/declared to have) and returns a :class:`ShapeComparison`.

    The comparison covers three aspects, in order:

    1. **Stage order** — ``ordered_keys`` must match exactly.
    2. **Placeholder set** — ``placeholder_keys`` must match exactly.
    3. **Real job IDs** — the job IDs for ``batch_inference`` and
       ``trait_extraction`` must match.

    On the first mismatch, returns ``ShapeComparison(ok=False, error=...)`` where
    ``error`` names the divergent path (which field differs and the differing
    runner-vs-bundle values). On full agreement, returns
    ``ShapeComparison(ok=True, error=None)``.
    """
    if runner.ordered_keys != bundle.ordered_keys:
        return ShapeComparison(
            ok=False,
            error=(
                "stage order diverges: runner ordered_keys="
                f"{list(runner.ordered_keys)!r} != bundle ordered_keys="
                f"{list(bundle.ordered_keys)!r}"
            ),
        )

    if runner.placeholder_keys != bundle.placeholder_keys:
        return ShapeComparison(
            ok=False,
            error=(
                "placeholder set diverges: runner placeholder_keys="
                f"{sorted(runner.placeholder_keys)!r} != bundle placeholder_keys="
                f"{sorted(bundle.placeholder_keys)!r}"
            ),
        )

    for stage_key in _REAL_STAGE_KEYS:
        runner_job = runner.real_job_ids.get(stage_key)
        bundle_job = bundle.real_job_ids.get(stage_key)
        if runner_job != bundle_job:
            return ShapeComparison(
                ok=False,
                error=(
                    f"real job ID for {stage_key!r} diverges: runner="
                    f"{runner_job!r} != bundle={bundle_job!r}"
                ),
            )

    return ShapeComparison(ok=True, error=None)


def shape_from_config(config: AppConfig) -> PipelineShape:
    """Derive the :class:`PipelineShape` implied by an :class:`AppConfig`.

    Uses ``config.ordered_stages`` to build the shape: ``ordered_keys`` from the
    stage keys in order, ``placeholder_keys`` from stages whose ``kind`` is
    ``"placeholder"``, and ``real_job_ids`` mapping the two real stages to the
    config's ``inference_job_id`` / ``trait_extraction_job_id``.
    """
    stages = config.ordered_stages
    return PipelineShape(
        ordered_keys=tuple(stage.key for stage in stages),
        placeholder_keys=frozenset(
            stage.key for stage in stages if stage.kind == "placeholder"
        ),
        real_job_ids={
            "batch_inference": config.inference_job_id,
            "trait_extraction": config.trait_extraction_job_id,
        },
    )


def expected_bundle_config() -> PipelineShape:
    """Return the canonical :class:`PipelineShape` the Bundle_Job must have.

    This is the declared/expected shape of the deployed Databricks Workflow, so
    the runner can compare its own config-derived shape against it (Req 5.5,
    6.7). The five stages run in the fixed order below; the three preprocessing
    stages are placeholders, and the two real stages carry the confirmed job
    IDs (Batch Inference ``883613979748353`` and Trait Extraction
    ``412831839181191``).
    """
    return PipelineShape(
        ordered_keys=(
            "preprocessing_batching",
            "preprocessing_qscoring",
            "preprocessing_selection",
            "batch_inference",
            "trait_extraction",
        ),
        placeholder_keys=frozenset(
            {
                "preprocessing_batching",
                "preprocessing_qscoring",
                "preprocessing_selection",
            }
        ),
        real_job_ids={
            "batch_inference": DEFAULT_INFERENCE_JOB_ID,
            "trait_extraction": DEFAULT_TRAIT_EXTRACTION_JOB_ID,
        },
    )


def _default_inference_params() -> dict[str, Any]:
    return {
        "run_id": os.getenv("INFERENCE_RUN_ID", "312203213270542"),
        "input_prefix": os.getenv(
            "INFERENCE_INPUT_PREFIX",
            "/Workspace/Users/t.mungubariki@cgiar.org/ona-infer-main/"
            "beanbush_plantstand_bb_344_2-1/test",
        ),
        "input_mode": os.getenv("INFERENCE_INPUT_MODE", "glob"),
        "save_json_folder": os.getenv(
            "INFERENCE_SAVE_JSON_FOLDER",
            "/Volumes/use1_prod_artemis_catalog_3718194974443840/production/data/"
            "ona-infer-main/_fg05_smoke/images_inference/manual",
        ),
        "trait_type": os.getenv("INFERENCE_TRAIT_TYPE", "plant_stand"),
        "confidence": os.getenv("INFERENCE_CONFIDENCE", "0.5"),
        "limit": os.getenv("INFERENCE_LIMIT", "5"),
        "num_partitions": os.getenv("INFERENCE_NUM_PARTITIONS", "4"),
    }


# The Batch Inference service (ona-infer) and the Trait Extraction workflow use
# different vocabularies for the same trait. Map the inference trait_type to the
# trait-extraction trait so both stages operate on the same underlying trait.
# Trait-extraction values (pods | flowering | plantstand) map to themselves so
# either vocabulary is accepted.
INFERENCE_TO_EXTRACTION_TRAIT = {
    "pod": "pods",
    "flower": "flowering",
    "plant_stand": "plantstand",
    "pods": "pods",
    "flowering": "flowering",
    "plantstand": "plantstand",
}


def _extraction_trait_for(inference_trait_type: str) -> str:
    """Map an inference ``trait_type`` to the trait-extraction ``trait`` value.

    Raises ``ConfigError`` if the trait type cannot be mapped, so a run never
    triggers Trait Extraction with a trait its runner registry does not know.
    """
    key = (inference_trait_type or "").strip()
    try:
        return INFERENCE_TO_EXTRACTION_TRAIT[key]
    except KeyError as exc:
        raise ConfigError(
            f"cannot map inference trait_type {inference_trait_type!r} to a trait-"
            f"extraction trait; expected one of {sorted(INFERENCE_TO_EXTRACTION_TRAIT)}"
        ) from exc


def _default_trait_extraction_run_spec(
    inference_save_json_folder: str, inference_trait_type: str
) -> dict[str, Any]:
    raw = os.getenv("TRAIT_EXTRACTION_RUN_SPEC_JSON")
    if raw:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("TRAIT_EXTRACTION_RUN_SPEC_JSON must be valid JSON") from exc
        if not isinstance(parsed, dict):
            raise ValueError("TRAIT_EXTRACTION_RUN_SPEC_JSON must be a JSON object")
        return parsed

    # Wire trait extraction input to the inference output and match its trait.
    # An explicit TRAIT_EXTRACTION_TRAIT override wins; otherwise the trait is
    # derived from the inference trait_type so both stages run the same trait.
    trait = os.getenv("TRAIT_EXTRACTION_TRAIT") or _extraction_trait_for(
        inference_trait_type
    )
    return {
        "trait": trait,
        "method": os.getenv("TRAIT_EXTRACTION_METHOD", "computer_vision"),
        "input_dir": inference_save_json_folder,
        "output_prefix": inference_save_json_folder.replace(
            "/images_inference/", "/traits/"
        ),
        "extra": {
            "validate_only": _bool(os.getenv("TRAIT_EXTRACTION_VALIDATE_ONLY"), False),
            "firestore_upload": _bool(
                os.getenv("TRAIT_EXTRACTION_FIRESTORE_UPLOAD"), True
            ),
        },
    }


def load_config() -> AppConfig:
    """Load orchestrator configuration from environment without side effects."""
    if load_dotenv is not None:
        load_dotenv()

    inference_params = _default_inference_params()

    # Validate save_json_folder before constructing any config so that an
    # invalid value never partially mutates state. On failure we raise before
    # building a new AppConfig, so the caller retains its prior
    # save_json_folder / input_dir values (Req 4.3).
    save_json_folder = inference_params.get("save_json_folder")
    if not isinstance(save_json_folder, str) or not save_json_folder.strip():
        raise ConfigError(
            "save_json_folder is invalid: it must be a non-empty, non-whitespace "
            f"path string, got {save_json_folder!r}"
        )

    # Derive input_dir from the validated save_json_folder and enforce that the
    # run spec's input_dir is character-for-character identical to it. When
    # TRAIT_EXTRACTION_RUN_SPEC_JSON is supplied, the returned dict may carry a
    # different input_dir; overwrite it so the wiring invariant always holds,
    # then verify equality defensively (Req 4.1/4.4).
    trait_extraction_run_spec = _default_trait_extraction_run_spec(
        save_json_folder, inference_params.get("trait_type", "")
    )
    trait_extraction_run_spec["input_dir"] = save_json_folder
    if trait_extraction_run_spec["input_dir"] != save_json_folder:
        raise ConfigError(
            "input_dir does not match save_json_folder: "
            f"{trait_extraction_run_spec['input_dir']!r} != {save_json_folder!r}"
        )

    return AppConfig(
        databricks_host=os.getenv("DATABRICKS_HOST") or None,
        databricks_profile=os.getenv("DATABRICKS_CONFIG_PROFILE") or None,
        inference_job_id=os.getenv("INFERENCE_JOB_ID", DEFAULT_INFERENCE_JOB_ID),
        trait_extraction_job_id=os.getenv(
            "TRAIT_EXTRACTION_JOB_ID", DEFAULT_TRAIT_EXTRACTION_JOB_ID
        ),
        poll_interval_s=_int(os.getenv("POLL_INTERVAL_SECONDS"), 30),
        poll_timeout_s=_int(os.getenv("POLL_TIMEOUT_SECONDS"), 24 * 60 * 60),
        inference_params=inference_params,
        trait_extraction_run_spec=trait_extraction_run_spec,
    )
