"""Performance benchmark for UK Flood Pipeline.

Captures before/after metrics for partitioning/clustering/incremental optimization:
- Bytes scanned
- Execution time
- Estimated cost (BigQuery on-demand pricing)

Run against BigQuery with real data.
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path

from google.cloud import bigquery

RESULTS_FILE = Path("docs/benchmarks.md")


QUERIES = [
    {
        "name": "Daily readings last 7 days",
        "sql": """
            SELECT measure_id, DATE(dateTime) as d, COUNT(*) as cnt, AVG(CAST(value AS FLOAT64)) as avg_val
            FROM `uk_flood.fct_readings`
            WHERE DATE(dateTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
            GROUP BY measure_id, d
            ORDER BY d DESC
        """,
    },
    {
        "name": "Station completeness",
        "sql": """
            SELECT station_id, COUNT(DISTINCT measure_id) as measures, COUNT(*) as readings
            FROM `uk_flood.fct_readings`
            WHERE DATE(dateTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            GROUP BY station_id
            ORDER BY readings DESC
        """,
    },
    {
        "name": "Active flood warnings",
        "sql": """
            SELECT * FROM `uk_flood.mart_flood_warning_history`
            WHERE is_current = TRUE
        """,
    },
    {
        "name": "River level trends",
        "sql": """
            SELECT station_id, DATE_TRUNC(dateTime, HOUR) as hour, AVG(CAST(value AS FLOAT64)) as avg_level
            FROM `uk_flood.mart_river_level_trends`
            WHERE DATE(dateTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 HOUR)
            GROUP BY station_id, hour
            ORDER BY hour DESC
        """,
    },
]


def run_benchmarks(project_id: str = "ukfloodpipeline") -> None:
    """Run benchmark queries and capture metrics."""
    print(f"Running benchmarks against project: {project_id}")
    client = bigquery.Client(project=project_id)

    results = []
    for q in QUERIES:
        print(f"Query: {q['name']}")
        job_config = bigquery.QueryJobConfig(use_query_cache=False)
        start = time.time()
        query_job = client.query(q["sql"], job_config=job_config)
        query_job.result()
        elapsed = time.time() - start

        bytes_billed = query_job.total_bytes_billed or 0
        bytes_processed = query_job.total_bytes_processed or 0
        # BigQuery on-demand: $5/TB
        cost_usd = (bytes_billed / (1024**4)) * 5.0

        print(f"  Time: {elapsed:.2f}s, Bytes billed: {bytes_billed:,}, Cost: ${cost_usd:.6f}")
        results.append(
            {
                "name": q["name"],
                "elapsed_s": elapsed,
                "bytes_billed": bytes_billed,
                "bytes_processed": bytes_processed,
                "cost_usd": cost_usd,
            }
        )
        print()

    # Write results
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS_FILE.open("w") as f:
        f.write("# Performance Benchmarks\n\n")
        f.write(f"**Run Date:** {datetime.now().isoformat()}\n\n")
        f.write(f"**Project:** {project_id}\n\n")
        f.write("**Pricing:** BigQuery on-demand $5.00/TB scanned\n\n")
        f.write("## Query Performance\n\n")
        f.write("| Query | Time (s) | Bytes Processed | Bytes Billed | Est. Cost (USD) |\n")
        f.write("|-------|----------|-----------------|---------------|-----------------|\n")
        for r in results:
            f.write(
                f"| {r['name']} | {r['elapsed_s']:.2f} | {r['bytes_processed']:,} | {r['bytes_billed']:,} | ${r['cost_usd']:.6f} |\n"
            )
        total_cost = sum(r["cost_usd"] for r in results)
        total_bytes = sum(r["bytes_billed"] for r in results)
        f.write(f"| **TOTAL** | - | - | **{total_bytes:,}** | **${total_cost:.6f}** |\n")
        f.write("\n## Optimization Notes\n\n")
        f.write("- **Partitioning:** `fct_readings` partitioned by `DATE(dateTime)` for time-based pruning\n")
        f.write("- **Clustering:** Clustered by `measure_id` to optimize joins and filters\n")
        f.write("- **Incremental:** `fct_readings` uses incremental materialization (merge on BQ, delete+insert on DuckDB)\n")
        f.write("- **Staging:** Views for fast development, materialized where beneficial\n")

    print(f"Results written to {RESULTS_FILE}")


if __name__ == "__main__":
    import sys

    project = sys.argv[1] if len(sys.argv) > 1 else "ukfloodpipeline"
    run_benchmarks(project)
