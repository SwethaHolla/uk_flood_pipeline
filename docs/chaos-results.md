# Chaos Test Results

**Run Date:** 2026-10-21 22:29:10

**Target:** http://localhost:8000

## Scenarios

| # | Scenario | Endpoint | Expected | Observed | Status |
|---|----------|----------|----------|----------|--------|
| 1 | API Timeout | /timeout | timeout/retry | error | [OK] Handled |
| 2 | Rate Limit 429 | /rate-limit | 429 with retry-after | error | [OK] Handled |
| 3 | Server Error 500 | /error | 500 with retry | error | [OK] Handled |
| 4 | Malformed JSON | /malformed | parse error, quarantined | error | [OK] Handled |
| 5 | Missing Fields | /bad-data | validation failure, quarantined | error | [OK] Handled |
| 6 | Network Error | /network-error | connection error, retry with backoff | error | [OK] Handled |
| 7 | Slow Response | /slow | timeout, retry/backoff | error | [OK] Handled |

## Notes

- All scenarios tested against local mock FastAPI server
- Client implements retries with exponential backoff (tenacity)
- Contract validation routes bad data to quarantine table
- Pipeline continues on individual resource failures
