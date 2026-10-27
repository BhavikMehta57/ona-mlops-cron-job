# ONA MLOps end-to-end Databricks workflow

The Databricks bundle is the production source of truth for this pipeline:

```text
Batching -> Quality scoring -> Image selection -> Dynamic routing
         -> Batch inference (per crop/trait/protocol)
         -> Trait extraction (per crop/trait/protocol)
         -> Final result manifest
```

## Dynamic routing

The batching job inspects the incoming export instead of requiring an operator to
choose one trait manually. It reads:

- trait: `img_trait`, `plot_trait`, `trait`, or `Trait`
- protocol: `img_protocol`, `plot_protocol`, or `protocol`
- crop: an explicit crop column when present, otherwise the crop component of a
  season such as `2025--KEN--Cowpea--November`

Supported routes are:

| Detected value | Batch inference | Trait extraction |
|---|---|---|
| pod/pods | `pod` | `pods` |
| flower/flowering | `flower` | `flowering` |
| plant stand/emergence | `plant_stand` | `plantstand` |

Unknown or missing traits fail the routing stage by default so data is never
silently skipped. Set the bundle variable `fail_on_unsupported_traits=false` only
when partial processing is intentional.

## Run-scoped data contract

Each run writes immutable artifacts under:

```text
/Volumes/.../ona-harvest/pipeline_runs/<databricks-job-run-id>/
  batched/
  scored/
  selected/
  inference/<crop__trait__protocol>/
  traits/<crop__trait__protocol>/
  manifests/
    batching.json
    qscoring.json
    selection.json
    routes.json
    final_results.json
```

Preprocessing never deletes its input. Every downstream job consumes an explicit
manifest, making retries deterministic and preventing one run from accidentally
processing another run's "latest" CSV.

## Deploy

Deploy the parameterized preprocessing child jobs first, then the parent bundle:

```powershell
cd ..\prepops\image_batching
databricks bundle deploy -t dev

cd ..\image_qscoring
databricks bundle deploy -t dev

cd ..\image_selection
databricks bundle deploy -t dev

cd ..\..\cron_job
databricks bundle deploy -t dev
```

If a child job was recreated rather than updated, replace its corresponding job ID
in `databricks.yml` before deploying the parent.

## Run

Always pass the exact Unity Catalog Volume CSV in production:

```powershell
databricks bundle run ona_e2e_pipeline -t dev --params `
  input_csv=/Volumes/use1_prod_artemis_catalog_3718194974443840/production/data/ona-harvest/raw/export.csv
```

For backward compatibility, an empty `input_csv` makes batching choose the newest
CSV under `ona-harvest/raw`, but explicit input is strongly preferred.

The final task prints the result and exposes a Databricks task value named
`result_manifest`. The same path is always:

```text
<run_root>/manifests/final_results.json
```

That manifest contains detected crops, traits, protocols, inference output counts,
and every trait-result CSV path.

## Cost controls

- All preprocessing shards are handled within one cluster run per stage, avoiding
  a cluster startup for every plot.
- Dynamic inference and extraction concurrency defaults to 3.
- Selection runs inference only for groups containing copied images.
- `max_concurrent_runs` is 1 to prevent duplicate scheduled processing.
- Wheels and TensorFlow are pinned to reproducible versions.

The older Python SDK runner remains only for legacy compatibility; it does not
implement dynamic routing. Use the Databricks bundle workflow for production.
