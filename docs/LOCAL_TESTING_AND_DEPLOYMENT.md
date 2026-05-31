# Daily Pipeline Local Testing And Deployment

This runbook is for testing the cron job against live staging-style GCP services with isolated prefixes before enabling a scheduled production run.

## Preflight

Install Python 3.11 and create a virtual environment:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r cron_job\requirements.txt pytest
```

If `gcloud` cannot read the default config directory, use an isolated config:

```powershell
$env:CLOUDSDK_CONFIG="$HOME\.gcloud-ona-cron"
gcloud init
gcloud auth application-default login
gcloud config set project artemis-418513
```

Set local environment variables:

```powershell
$env:GCS_BUCKET="ona-harvest"
$env:PREPROCESSOR_URL="https://ona-api.ona.farm/web/pre_processing"
$env:INFERENCE_URL="https://ona-infer-888018102762.us-central1.run.app"
$env:GCP_PROJECT_ID="artemis-418513"
$env:GCP_RUN_REGION="us-central1"
$env:TRAIT_EXTRACTION_JOB_ID="ona-trait-extraction"
$env:FIRESTORE_DATABASE_ID="artemis-prod"
$env:SELECTED_IMAGES_BUCKET="artemis-revamp"
$env:USE_CLOUD_LOGGING="false"
```

## Local Mocked Checks

```powershell
python -m pytest cron_job\tests -q
python -m cron_job.main --smoke-test --run-id local-smoke-001
```

The smoke test should log `stage="smoke_test"` and `status="success"`.

## Local Step Harness

Any `--step` run uses isolated local prefixes by default:

- raw CSVs: `raw/local-test/{run_id}/...`
- inference outputs: `inference/local-test/{run_id}/...`
- extraction outputs: `extraction/local-test/{run_id}/...`

Use one known test subtrial first:

```powershell
$env:RUN_ID="local-$(Get-Date -Format yyyyMMdd-HHmmss)"
$env:TRIAL_ID="mvp-validation--Kawanda--CIAT"
$env:SUBTRIAL_ID="2025--UGA--Bushbean--September--field_1--Kawanda"
$env:RUN_DATE="2025-11-25"
```

Run discovery:

```powershell
python -m cron_job.main --step discover --run-id $env:RUN_ID --limit-subtrials 1
```

Run scan:

```powershell
python -m cron_job.main --step scan --trial-id $env:TRIAL_ID --subtrial-id $env:SUBTRIAL_ID --run-date $env:RUN_DATE --run-id $env:RUN_ID
```

Generate raw CSVs:

```powershell
python -m cron_job.main --step csv --trial-id $env:TRIAL_ID --subtrial-id $env:SUBTRIAL_ID --run-date $env:RUN_DATE --run-id $env:RUN_ID
```

Use the logged `images_csv_uri` and `classical_csv_uri` in the downstream steps.

Run preprocessing:

```powershell
python -m cron_job.main --step preprocess --image-csv-uri "gs://ona-harvest/raw/2026--TZA--Bushbean--January--TARI-Selian--Arusha_images.csv" --run-id $env:RUN_ID >cron_job/local_test_output/preprocess_step_output.txt
```

Run inference after preprocessing succeeds:

```powershell
python -m cron_job.main --step inference --trial-id $env:TRIAL_ID --subtrial-id $env:SUBTRIAL_ID --run-date $env:RUN_DATE --run-id $env:RUN_ID >cron_job/local_test_output/inference_step_output.txt
```

If the preprocessor logs a different `batch_run_id`, pass it explicitly with `--batch-run-id "<batch_run_id>"`.

Run CV extraction after inference writes JSON outputs:

```powershell
python -m cron_job.main --step extract-cv --subtrial-id $env:SUBTRIAL_ID --run-id $env:RUN_ID --inference-output-dir "gs://artemis-revamp/Artemis/Arusha--CIAT/staggered-plots-beans-trial/2026--TZA--Bushbean--January/TARI-Selian/Arusha/trait_collection/images_inference/dup_merged_1_to_15_flower_inst_seg-mhpnh-v8/0.34/" --trait flowering > cron_job/local_test_output/extract-cv_step_output.txt
```

Run classical extraction:

```powershell
python -m cron_job.main --step extract-classical --classical-csv-uri "gs://ona-harvest/raw/local-test/<run_id>/<trial_id>/<subtrial_id>_classical.csv" --trial-id $env:TRIAL_ID --subtrial-id $env:SUBTRIAL_ID --run-id $env:RUN_ID
```

Merge trait output CSVs back into the raw CSVs:

```powershell
python -m cron_job.main --step csv-writeback --image-csv-uri "gs://ona-harvest/raw/local-test/<run_id>/<trial_id>/<subtrial_id>_images.csv" --classical-csv-uri "gs://ona-harvest/raw/local-test/<run_id>/<trial_id>/<subtrial_id>_classical.csv" --subtrial-id $env:SUBTRIAL_ID --run-id $env:RUN_ID
```

Run the full pipeline with local-test prefixes:

```powershell
python -m cron_job.main --step full --trial-id $env:TRIAL_ID --subtrial-id $env:SUBTRIAL_ID --run-date $env:RUN_DATE --run-id $env:RUN_ID 2>&1 | Tee-Object -FilePath cron_job/local_test_output/full_run.txt
```

Run the production-prefix full pipeline only after the local-test run is clean:

```powershell
python -m cron_job.main --trial-id $env:TRIAL_ID --subtrial-id $env:SUBTRIAL_ID --run-date $env:RUN_DATE --run-id "manual-$env:RUN_DATE-001"
```

## Deploy

Enable required services:

```powershell
gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com cloudscheduler.googleapis.com
```

Create the Artifact Registry repository once:

```powershell
gcloud artifacts repositories create ona-jobs `
  --repository-format=docker `
  --location=us-central1
```

Build and push the image:

```powershell
gcloud builds submit . `
  --config cloudbuild.yaml `
  --substitutions _IMAGE=us-central1-docker.pkg.dev/artemis-418513/ona-jobs/daily-pipeline-cron-job:v1
```

Deploy the Cloud Run Job:

```powershell
gcloud run jobs deploy daily-pipeline-cron-job `
  --image us-central1-docker.pkg.dev/artemis-418513/ona-jobs/daily-pipeline-cron-job:v1 `
  --region us-central1 `
  --tasks 1 `
  --parallelism 1 `
  --max-retries 0 `
  --task-timeout 86400s `
  --memory 2Gi `
  --cpu 2 `
  --service-account daily-pipeline-cron@artemis-418513.iam.gserviceaccount.com `
  --set-env-vars GCS_BUCKET=ona-harvest,PREPROCESSOR_URL=https://<preprocessor>,INFERENCE_URL=https://<ona-infer>,GCP_PROJECT_ID=artemis-418513,GCP_RUN_REGION=us-central1,TRAIT_EXTRACTION_JOB_ID=ona-trait-extraction,FIRESTORE_DATABASE_ID=artemis-prod,SELECTED_IMAGES_BUCKET=artemis-revamp
```

Manually execute the job:

```powershell
gcloud run jobs execute daily-pipeline-cron-job `
  --region us-central1 `
  --wait `
  --args "--run-date=YYYY-MM-DD","--run-id=manual-YYYYMMDD-001"
```

### Date selection arguments

The job accepts three mutually-relevant date arguments. Precedence order: `--data-collected-dates` > `--data-collected-date` > `--run-date`.

| Argument | Format | Behavior |
|---|---|---|
| `--run-date` | `YYYY-MM-DD` | Filters Firestore by `upload_timestamp` UTC window. Defaults to today. Used when neither of the other two flags is supplied. |
| `--data-collected-date` | `YYYY-MM-DD` | Filters Firestore by the `data_collection` string field for a single date. Single-date pipeline. |
| `--data-collected-dates` | `YYYY-MM-DD+YYYY-MM-DD+...` | Multi-date pipeline. Phase 1 runs scan, csv upload, preprocessing, and inference per-date sequentially. Phase 2 runs trait extraction once with the combined inference results and per-trait combined classical CSVs. Use `+` (or `;`) as separator because Cloud Run splits `--args` on commas. |

Single date with the `data_collection` field filter:

```powershell
gcloud run jobs execute daily-pipeline-cron-job --region us-central1 --wait `
  --args "--data-collected-date,2025-11-15,--trial-id,naro-main-trial--Namulonge--NARO,--subtrial-id,2025--UGA--Bushbean--September--field_1--Namulonge"
```

Multiple dates in a single execution (sequential per-date phase 1, combined phase 2 trait extraction):

```powershell
gcloud run jobs execute mlops-pipeline-job --region us-central1 --wait `
  --args "--data-collected-dates,2025-11-15+2025-11-17+2025-11-19+2025-11-22,--trial-id,naro-main-trial--Namulonge--NARO,--subtrial-id,2025--UGA--Bushbean--September--field_1--Namulonge"
```

Notes:
- Always include `--wait` when chaining executions in a script. Without it, `gcloud` returns immediately and the next execution starts before the previous one finishes.
- Cloud Run splits the `--args` string on commas to form `argv`. Inside a single arg value, use `+` to join multiple dates so they reach `--data-collected-dates` as one value.

Create the scheduler only after the manual Cloud Run Job exits `0`:

```powershell
gcloud scheduler jobs create http daily-pipeline-cron-job-schedule `
  --location us-central1 `
  --schedule "0 6 * * *" `
  --time-zone "Etc/UTC" `
  --uri "https://run.googleapis.com/v2/projects/artemis-418513/locations/us-central1/jobs/daily-pipeline-cron-job:run" `
  --http-method POST `
  --oauth-service-account-email daily-pipeline-cron-scheduler@artemis-418513.iam.gserviceaccount.com
```

## IAM Checklist

Grant the Cloud Run Job service account:

- Firestore read/write access for trial, subtrial, preprocessing, and trait extraction audit documents.
- Storage object read/write for `ona-harvest`.
- Storage object read access for selected image shard CSVs and downstream output CSVs.
- Cloud Run Jobs run/read permissions for the trait extraction job.
- Identity-token permissions for downstream services if preprocessor or inference are authenticated.

## Acceptance Checks

- Unit tests pass locally.
- Smoke test logs success.
- Discovery finds the expected active `trial_layouts.isActive == True` subtrial.
- Scan counts match Firestore documents for the selected UTC date.
- Raw CSVs exist under `raw/local-test/{run_id}` during step testing.
- Preprocessor writes selected shard metadata under `preprocessing_runs/{batch_run_id}`.
- Inference creates JSON and annotated output folders.
- Trait extraction creates `trait_extraction_runs` records and output CSVs.
- CSV writeback preserves raw columns and appends trait output columns.
- Manual Cloud Run Job exits `0` before Scheduler is enabled.


## Deployment Updates

- docker build -t "us-central1-docker.pkg.dev/artemis-418513/ona-jobs/daily-pipeline-cron-job:v4" .

- docker push "us-central1-docker.pkg.dev/artemis-418513/ona-jobs/daily-pipeline-cron-job:v4"

- gcloud run jobs update daily-pipeline-cron-job --image us-central1-docker.pkg.dev/artemis-418513/ona-jobs/daily-pipeline-cron-job:v4 --region us-central1

## Example Powershell Scripts and Runs

.\naro-main-trial\runs.ps1 2>&1 | Tee-Object -FilePath cron_job/naro-main-trial/runs2.txt