# Daily Pipeline Cron Job - Mermaid Diagrams

## 1. System context

```mermaid
flowchart LR
    SCH[Cloud Scheduler\nDaily CRON] -->|HTTP POST + OIDC| JOB[Cloud Run Job\ncron_job]

    JOB --> FS[(Firestore\nartemis-prod)]
    JOB --> GCS[(GCS Bucket\nona-harvest)]
    JOB --> LOG[Cloud Logging]
    JOB --> SM[Secret Manager]

    JOB --> PRE[e2e_prerops\nPreprocessor Service]
    JOB --> INF[ona-infer\nBatch Inference Service]
    JOB --> EXT[Trait Extraction CLI\nsubprocess in Job container]
```

## 2. Component architecture

```mermaid
flowchart TD
    MAIN[main.py\nOrchestrator] --> CFG[config/bootstrap]
    MAIN --> DISC[firestore_client.py\nDiscovery + Scanning]
    MAIN --> CSV[csv_assembler.py]
    MAIN --> GCS[gcs_client.py]
    MAIN --> PRE[preprocessor_client.py]
    MAIN --> PTR[protocol_trait_resolver.py]
    MAIN --> INF[inference_client.py]
    MAIN --> EXT[trait_extractor.py]
    MAIN --> WB[csv_writeback.py]
    MAIN --> FSWB[firestore_writeback.py]
    MAIN --> LOG[logger.py]

    DISC --> FS[(Firestore)]
    CSV --> GCS
    GCS --> OBJ[(GCS Objects)]
    PRE --> PP[e2e_prerops]
    INF --> OI[ona-infer]
    EXT --> CLI[Trait Extraction CLI]
    FSWB --> FS
    LOG --> CL[Cloud Logging]
```

## 3. Per-subtrial orchestration

```mermaid
flowchart TD
    A[Start Subtrial] --> B[Scan Firestore Documents]
    B --> C{Any documents?}
    C -->|No| Z[Mark Subtrial Skipped]
    C -->|Yes| D[Split image vs classical docs]

    D --> ICSV{Image docs exist?}
    ICSV -->|No| ISKIP[Skip CV path]
    ICSV -->|Yes| I1[Create + upload Images CSV]
    I1 --> I2{Upload OK?}
    I2 -->|No| IF[CV path failed]
    I2 -->|Yes| I3[Start preprocessing]
    I3 --> I4{Preprocess terminal state}
    I4 -->|Failed/Timeout| I5[Mark image docs preprocessing_status=skipped]
    I4 -->|Success| I6[Write preprocessing statuses]
    I6 --> I7[Resolve image protocol-trait groups]
    I7 --> I8[Submit inference jobs concurrently]
    I8 --> I9[Poll inference jobs independently]
    I9 --> I10[Write successful inference metadata to Firestore]
    I10 --> I11[Run CV extraction per unique output path]
    I11 --> I12[Merge extraction results into Images CSV]

    D --> CCSV{Classical docs exist?}
    CCSV -->|No| CSKIP[Skip classical path]
    CCSV -->|Yes| C1[Create + upload Classical CSV]
    C1 --> C2{Upload OK?}
    C2 -->|No| CF[Classical path failed]
    C2 -->|Yes| C3[Resolve classical protocol-trait groups]
    C3 --> C4[Run classical extraction per group]
    C4 --> C5[Merge extraction results into Classical CSV]

    ISKIP --> END[Finalize Subtrial Status]
    IF --> END
    I5 --> END
    I12 --> END
    CSKIP --> END
    CF --> END
    C5 --> END
    Z --> DONE[Continue Next Subtrial]
    END --> DONE
```

## 4. CV path sequence

```mermaid
sequenceDiagram
    participant M as main.py
    participant CSV as csv_assembler.py
    participant G as GCS
    participant P as e2e_prerops
    participant R as protocol_trait_resolver.py
    participant I as inference_client.py
    participant O as ona-infer
    participant E as trait_extractor.py
    participant W as csv_writeback.py
    participant F as Firestore

    M->>CSV: assemble_images_csv(image_docs)
    CSV->>G: upload images CSV
    G-->>CSV: images_csv_uri
    CSV-->>M: images_csv_uri

    M->>P: POST /start(batch_run_id)
    loop until terminal state
        M->>P: GET /status/{batch_run_id}
        P-->>M: running or success or failed
    end

    alt preprocessing success
        M->>F: write preprocessing statuses using manifest
        M->>R: resolve image protocol-trait groups
        R-->>M: valid groups

        M->>I: run_inference(groups)
        par group 1
            I->>O: POST /batch
            loop poll job
                I->>O: GET /batch-status/{job_id}
                O-->>I: status
            end
        and group N
            I->>O: POST /batch
            loop poll job
                I->>O: GET /batch-status/{job_id}
                O-->>I: status
            end
        end

        I-->>M: inference results
        M->>F: write successful inference metadata
        M->>E: run CV extraction per unique output path
        E-->>M: extraction results
        M->>W: merge CV extraction into Images CSV
        W->>G: overwrite enriched images CSV
    else preprocessing failed or timed out
        M->>F: mark image docs preprocessing_status=skipped
    end
```

## 5. Classical path sequence

```mermaid
sequenceDiagram
    participant M as main.py
    participant CSV as csv_assembler.py
    participant G as GCS
    participant R as protocol_trait_resolver.py
    participant E as trait_extractor.py
    participant W as csv_writeback.py

    M->>CSV: assemble_classical_csv(classical_docs)
    CSV->>G: upload classical CSV
    G-->>CSV: classical_csv_uri
    CSV-->>M: classical_csv_uri

    M->>R: resolve classical protocol-trait groups
    R-->>M: valid groups

    loop each group
        M->>E: run classical extraction(input_csv)
        E-->>M: extraction result
    end

    M->>W: merge extraction results into Classical CSV
    W->>G: overwrite enriched classical CSV
```

## 6. Firestore logical model

```mermaid
flowchart TD
    T[trials/trial_id] --> S[subtrials/subtrial_id]

    S --> IMG[images/protocol_date_id]
    IMG --> IMGP[Plot/document_id]

    S --> TWO[two_images_with_count/protocol_date_id]
    TWO --> TWOP[Plot/document_id]

    S --> FLW[flowering_data/document_id]
    S --> NUM[numeric_data/document_id]

    S --> DC[data_collections/document_id\nStub only, not actively scanned]
```
