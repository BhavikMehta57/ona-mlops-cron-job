# ONA MLOps — End-to-End Pipeline (Databricks)

Orchestrates the full ONA pipeline on Databricks by chaining the existing jobs
in order:

```
Preprocessing - Batching
  -> Preprocessing - QScoring
    -> Preprocessing - Selection
      -> Batch Inference
        -> Trait Extraction
```

There are two ways to run the pipeline. Use whichever fits your workflow.

## Existing job IDs

| Stage                    | Job ID            |
|--------------------------|-------------------|
| Preprocessing - Batching | 884636592596793   |
| Preprocessing - QScoring | 357955527418592   |
| Preprocessing - Selection| 1059952483174194  |
| Batch Inference          | 883613979748353   |
| Trait Extraction         | 412831839181191   |

---

## Option A — Declarative Databricks Workflow (recommended)

A [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/index.html)
defines a single orchestrator job (`ONA - End to End Pipeline`) whose tasks use
`run_job_task` to trigger each existing job with `depends_on` ordering. Databricks
handles sequencing, retries, and stops the run if any stage fails.

Files:
- `databricks.yml` — bundle definition, variables (job IDs + parameters), targets.
- `resources/end_to_end_pipeline.job.yml` — the orchestrator workflow.

### Prerequisites
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) v0.218+.
- Auth configured: a CLI profile, or `DATABRICKS_HOST` + `DATABRICKS_TOKEN`.
- Set your workspace host in `databricks.yml` under the target (or via env).

### Deploy and run
```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run ona_e2e_pipeline -t dev
```

### Override parameters at run time
```bash
databricks bundle run ona_e2e_pipeline -t dev \
  --var="inference_trait_type=pods,inference_limit=100"
```

> Note: `job_parameters` passed via `run_job_task` only override a child job if
> that job declares matching **named job parameters**. If a child job hardcodes
> its own task parameters, it runs with those and the block is a harmless no-op.

---

## Option B — Imperative runner (Databricks SDK)

`main.py` triggers each job in order via the Databricks SDK and polls each run to
completion. Useful for dynamic, per-run triggering (the equivalent of the old
`gcloud run jobs execute` loop) or for running inside a notebook / another job.

### Configure
Auth is resolved by the SDK from `DATABRICKS_HOST`/`DATABRICKS_TOKEN`, a profile
(`DATABRICKS_CONFIG_PROFILE`), or the notebook/job context. Optional overrides
via environment variables (see `core/config.py`):

- Job IDs: `BATCHING_JOB_ID`, `QSCORING_JOB_ID`, `SELECTION_JOB_ID`,
  `INFERENCE_JOB_ID`, `TRAIT_EXTRACTION_JOB_ID`
- Inference: `INFERENCE_RUN_ID`, `INFERENCE_INPUT_PREFIX`, `INFERENCE_INPUT_MODE`,
  `INFERENCE_SAVE_JSON_FOLDER`, `INFERENCE_TRAIT_TYPE`, `INFERENCE_CONFIDENCE`,
  `INFERENCE_LIMIT`, `INFERENCE_NUM_PARTITIONS`
- Trait extraction: `TRAIT_EXTRACTION_RUN_SPEC_JSON` (full JSON), or the pieces
  `TRAIT_EXTRACTION_TRAIT`, `TRAIT_EXTRACTION_METHOD`, `TRAIT_EXTRACTION_VALIDATE_ONLY`

### Run
```bash
pip install -r requirements.txt

# Validate config + connectivity without triggering jobs:
python -m cron_job.main --smoke-test

# Run the full pipeline:
python -m cron_job.main --run-id my-run-001
```

Exit code is `0` when every stage succeeds, `1` on the first failure.

---

## Project layout
```
cron_job/
├── databricks.yml                       # Asset Bundle (Option A)
├── resources/
│   └── end_to_end_pipeline.job.yml       # Orchestrator workflow (Option A)
├── main.py                               # CLI entrypoint (Option B)
├── core/config.py                        # Config: job IDs + parameters
├── orchestrator/pipeline.py              # SDK runner (Option B)
├── middleware/logger.py                  # Structured JSON logging to stdout
└── requirements.txt
```
