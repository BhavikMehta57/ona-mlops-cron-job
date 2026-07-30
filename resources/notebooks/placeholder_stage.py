# Databricks notebook source
# Placeholder preprocessing stage notebook.
#
# This notebook stands in for a real preprocessing stage (Batching, QScoring,
# or Selection). It performs no external work: it only announces the job name
# it stands in for by writing it to standard log output, then completes
# normally so the orchestrator records a successful stage.
#
# The announce/print is wrapped in a broad guard so that a failure to print or
# log the stage name never raises a fatal error to the task -- the cell always
# returns normally and the placeholder reports success (Requirement 1.7).

# COMMAND ----------

try:
    dbutils.widgets.text("stage_name", "")
    stage_name = dbutils.widgets.get("stage_name")
    # Announce the job name to standard log output.
    print(stage_name)
except Exception as exc:  # noqa: BLE001 - broad guard is intentional (Req 1.7)
    # A failure to announce the stage name must never fail the placeholder.
    # Swallow the error so the cell returns normally and the stage succeeds.
    try:
        print(f"placeholder_stage: announce skipped due to error: {exc!r}")
    except Exception:
        # Even the fallback log must not raise; nothing left to do but continue.
        pass
