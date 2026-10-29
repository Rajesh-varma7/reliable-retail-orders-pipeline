# Reliable Retail Orders Data Pipeline

A local-first Data Engineering project demonstrating ingestion, schema and business validation, deterministic transformations, quarantine, idempotent publishing, audit metrics, reconciliation, tests, and a simple performance benchmark.

## Architecture

```text
CSV source + customer reference
          |
          v
  Ingestion / schema checks
          |
          v
 Row-level validation -----> quarantine/rejects.csv
          |
          v
 deterministic transform
          |
          v
 SQLite target (upsert by order\_id) + run audit
          |
          +----> reconciliation report / SQL checks
          +----> pytest tests / benchmark
```

## Requirements

* Python 3.10+
* Install dependencies: `python -m pip install -r requirements.txt`

## Quick start

```bash
python -m venv .venv
# Windows: .venv\\Scripts\\activate
# macOS/Linux: source .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/generate\_data.py --rows 1000 --seed 42 --output data/orders.csv
python -m src.pipeline --input data/orders.csv --customers data/customers.csv --db outputs/retail.db --output-dir outputs
pytest -q
python scripts/benchmark.py --rows 100000
```

For a tiny reproducible sample, use `data/sample\_orders.csv` and `data/customers.csv`:

```bash
python -m src.pipeline --input data/sample\_orders.csv --customers data/customers.csv --db outputs/retail.db --output-dir outputs
```

The pipeline writes:

* `outputs/clean\_orders.csv`: accepted and transformed rows from the current batch
* `outputs/rejects.csv`: rejected rows with source file, run ID, reason codes, and original fields
* `outputs/run\_report.json`: counts, timing, status, and reconciliation values
* `outputs/retail.db`: published target and audit tables

## Data model and rules

### Source entity: orders

**Grain:** one order per `order\_id`.  
**Primary key:** `order\_id` (non-empty string).  
Fields:

* `order\_id`: required string, stable business key
* `customer\_id`: required string; must exist in customer reference data
* `order\_ts`: ISO-8601 timestamp with timezone or naive timestamp interpreted as UTC
* `amount`: decimal-like numeric value, finite and >= 0
* `status`: one of `PLACED`, `PAID`, `SHIPPED`, `CANCELLED`
* `source\_file`: assigned by the pipeline for traceability

### Reference entity: customers

**Grain:** one row per `customer\_id`; key must be unique and non-null.

### Target: `fact\_orders`

One current record per `order\_id`; fields include normalized UTC timestamp, amount, status, `source\_file`, `run\_id`, and `loaded\_at`. Re-running a batch uses SQLite `ON CONFLICT(order\_id) DO UPDATE`, so it does not create extra target rows. This is a current-state table, not a full event-history store.

### Validation / assumptions

* `order\_id`, `customer\_id`, `order\_ts`, `amount`, and `status` are required.
* Amount zero is allowed; negative and non-finite amounts are rejected.
* Timestamps are normalized to UTC ISO-8601.
* Exact duplicate rows for an order are deduplicated; if the same `order\_id` has conflicting content within one batch, all rows for that key are rejected as `DUPLICATE\_KEY\_CONFLICT`.
* Unknown customer keys are rejected as `INVALID\_CUSTOMER\_REFERENCE`.
* Unknown columns are treated as an unexpected schema change and fail the batch before publishing. Missing required columns also fail the batch.
* Empty input is a successful no-op run with zero counts.
* Row-level bad data is quarantined; batch-level schema errors fail the run.
* Late/out-of-order timestamps are accepted and retained as event timestamps. No watermark or late-arrival cutoff is imposed in this starter project.
* Deterministic transform: timestamps normalized to UTC and amount rounded to two decimal places.
* Synthetic data only; no credentials or external services are required.

## Idempotency, retry, restart, and backfill

* The target's primary key and upsert make repeated processing of the same `order\_id` safe from accidental row multiplication.
* The same input can be rerun. The target remains one row per order key, while run audit records each attempt.
* Batch-level schema failures do not publish target rows. A process interruption during target publication is protected by a SQLite transaction.
* Row rejects are written separately and can be inspected without losing source values.
* For backfills, pass the historical file through the same CLI. Because this target stores current state, a backfill with an existing `order\_id` updates that row. A production event-history design would use an append-only event table plus a deduplication key.

## Useful checks

```sql
SELECT COUNT(\*) AS target\_rows FROM fact\_orders;
SELECT status, COUNT(\*) AS order\_count, ROUND(SUM(amount), 2) AS amount\_total
FROM fact\_orders GROUP BY status ORDER BY status;
SELECT run\_id, status, input\_count, accepted\_count, rejected\_count, error\_message
FROM pipeline\_runs ORDER BY started\_at DESC;
```

## Performance

\## Performance



Run the benchmark with:



```bash

python scripts/benchmark.py --rows 10000

python scripts/benchmark.py --rows 100000

```



The benchmark generates synthetic data with a fixed seed, runs the pipeline in an isolated temporary directory, and reports elapsed time, throughput, and reconciliation metrics.



\### Measured results



Requested rows	Actual input rows	Accepted	Rejected	Exact duplicates collapsed	Elapsed time	Throughput

10,000	10,100	9,803	201	96	11.855 s	844 rows/s

1,00,000	1,01,000	98,021	2,000	979	110.519 s	905 rows/s



Data reconciliation: Both benchmark runs reported a reconciliation difference of zero and `reconciled: true`. Every input row was accounted for as accepted, rejected, or an exact duplicate collapsed.



These are observed results from one local Windows environment, not guaranteed performance targets. Execution time varies with hardware, Python version, and system load.



A potential next optimization is chunked CSV reading and batched processing for larger-than-memory datasets.



## Limitations / next steps

* SQLite is for a local demo, not concurrent production-scale ingestion.
* Row-level validation is intentionally simple and explicit.
* No orchestration, cloud object storage, secrets, or alerting is included.
* A production version could add schema versioning, structured logs, partitioned storage, CI, a scheduler, and append-only history.

