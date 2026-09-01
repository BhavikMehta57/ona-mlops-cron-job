## MLOPS-PIPELINE JOB

# Docker Run Commands

# docker build -t "us-central1-docker.pkg.dev/artemis-418513/ona-jobs/mlops-pipeline-job:latest" .

# docker push "us-central1-docker.pkg.dev/artemis-418513/ona-jobs/mlops-pipeline-job:latest"

# gcloud run jobs update mlops-pipeline-job --image us-central1-docker.pkg.dev/artemis-418513/ona-jobs/mlops-pipeline-job:latest --region us-central1

gcloud run jobs execute mlops-pipeline-job --project artemis-418513 --region us-central1 --args="--data-collected-dates,2026-01-15+2026-01-16,--trial-id,mvp-validation--Arusha--CIAT,--subtrial-id,2025--TZA--Bushbean--November--TARI Selian--Arusha"
