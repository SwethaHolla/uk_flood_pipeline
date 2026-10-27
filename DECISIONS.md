# Decisions

- D-001: dlt daily quota is the backstop for BigQuery bytes billed (settings.max_bytes_billed is inert). 
- D-013: Load job labels and backfill verification (added); confirm per label (load_job_ids/gcs_path/dir) on merge/verification.
- Elementary CLI pin/fallback: CLI in env reports 0.17.0 (dbt package <0.18 pinned to 0.17.0 in packages.yml/package-lock.yml; uv.lock pins elementary-data 0.17.0). CLI upgrade note exists (0.26.1 available) but cap follows CLI/dbt package constraints; fallback decision: keep Elementary at 0.17.0 due to compatibility cap <0.18.0. README stack table already states 0.17.0.

