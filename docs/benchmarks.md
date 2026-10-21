# Performance Benchmarks

**Run Date:** 2026-10-21T22:38:25.351136

**Project:** ukfloodpipeline

**Pricing:** BigQuery on-demand $5.00/TB scanned

## Query Performance

| Query | Time (s) | Bytes Processed | Bytes Billed | Est. Cost (USD) |
|-------|----------|-----------------|---------------|-----------------|
| Daily readings last 7 days | 1.83 | 603,911 | 10,485,760 | $0.000048 |
| Station completeness | 1.23 | 931,085 | 10,485,760 | $0.000048 |
| Active flood warnings | 1.21 | 192 | 10,485,760 | $0.000048 |
| River level trends | 1.32 | 3,360,013 | 41,943,040 | $0.000191 |
| **TOTAL** | - | - | **73,400,320** | **$0.000334** |

## Optimization Notes

- **Partitioning:** `fct_readings` partitioned by `DATE(dateTime)` for time-based pruning
- **Clustering:** Clustered by `measure_id` to optimize joins and filters
- **Incremental:** `fct_readings` uses incremental materialization (merge on BQ, delete+insert on DuckDB)
- **Staging:** Views for fast development, materialized where beneficial
