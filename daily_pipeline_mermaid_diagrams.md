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

## 3. Per-subtrial orchestration (single-date mode)

```mermaid
flowchart TD
    A[Start Subtrial] --> B[Scan Firestore Documents]
    B --> C{Any documents?}
    C -->|No| Z[Mark Subtrial Skipped]
    C -->|Yes| D[Split image vs classical docs]

    D --> ICSV{Image docs exist?}
    ICSV -->|No| ISKIP[Skip CV path]
    ICSV -->|Yes| I1[Create + upload Images CSV per protocol]
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

    D --> CCSV{Classical docs exist?}
    CCSV -->|No| CSKIP[Skip classical path]
    CCSV -->|Yes| C0[Resolve classical protocol-trait groups]
    C0 --> C1[For each group: filter docs + upload trait-specific CSV]
    C1 --> C2{Any group CSV uploaded?}
    C2 -->|No| CF[Classical path failed]
    C2 -->|Yes| C4[Run classical extraction per group with its own CSV]

    ISKIP --> END[Finalize Subtrial Status]
    IF --> END
    I5 --> END
    I11 --> END
    CSKIP --> END
    CF --> END
    C4 --> END
    Z --> DONE[Continue Next Subtrial]
    END --> DONE
```

## 3.1 Multi-date orchestration

```mermaid
flowchart TD
    M0[Start Multi-Date Run] --> M1[Load single subtrial]
    M1 --> P1[Phase 1: per-date loop]

    P1 --> D1[Scan docs for date N]
    D1 --> D2{Any docs?}
    D2 -->|No| D9[Skip this date]
    D2 -->|Yes| D3[Upload date-suffixed images CSV]
    D3 --> D4[Run preprocessing for date N]
    D4 --> D5{Preprocess OK?}
    D5 -->|No| D9
    D5 -->|Yes| D6[Run inference for date N]
    D6 --> D7[Append successful inference results to combined list]
    D7 --> D8[Append classical docs to combined list]
    D8 --> DN{More dates?}
    D9 --> DN
    DN -->|Yes| D1
    DN -->|No| P2[Phase 2: combined extraction]

    P2 --> P3[Run CV extraction with combined inference results]
    P3 --> P4[Group combined classical docs by protocol-trait]
    P4 --> P5[For each trait group: filter docs + upload combined classical CSV]
    P5 --> P6[Run classical extraction per group]
    P6 --> P7[Finalize subtrial status]
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
    participant R as protocol_trait_resolver.py
    participant CSV as csv_assembler.py
    participant G as GCS
    participant E as trait_extractor.py

    M->>R: resolve classical protocol-trait groups
    R-->>M: valid groups

    loop each group
        M->>M: filter docs to group source_document_ids
        M->>CSV: upload_classical_csv(filtered_docs)
        CSV->>G: upload trait-specific classical CSV
        G-->>CSV: group_classical_csv_uri
        CSV-->>M: group_classical_csv_uri
        M->>E: run classical extraction with this group's CSV
        E-->>M: extraction result
    end
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
