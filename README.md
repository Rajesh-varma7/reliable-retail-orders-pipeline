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
 SQLite target (upsert by order_id) + run audit
          |
          +----> reconciliation report / SQL checks
          +----> pytest tests / benchmark
```

## Requirements

- Python 3.10+
- Install dependencies: `python -m pip install -r requirements.txt`

## Quick start

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/generate_data.py --rows 1000 --seed 42 --output data/orders.csv
python -m src.pipeline --input data/orders.csv --customers data/customers.csv --db outputs/retail.db --output-dir outputs
pytest -q
python scripts/benchmark.py --rows 100000
```

For a tiny reproducible sample, use `data/sample_orders.csv` and `data/customers.csv`:
```bash
python -m src.pipeline --input data/sample_orders.csv --customers data/customers.csv --db outputs/retail.db --output-dir outputs
```

The pipeline writes:
- `outputs/clean_orders.csv`: accepted and transformed rows from the current batch
- `outputs/rejects.csv`: rejected rows with source file, run ID, reason codes, and original fields
- `outputs/run_report.json`: counts, timing, status, and reconciliation values
- `outputs/retail.db`: published target and audit tables

## Data model and rules

### Source entity: orders
**Grain:** one order per `order_id`.  
**Primary key:** `order_id` (non-empty string).  
Fields:
- `order_id`: required string, stable business key
- `customer_id`: required string; must exist in customer reference data
- `order_ts`: ISO-8601 timestamp with timezone or naive timestamp interpreted as UTC
- `amount`: decimal-like numeric value, finite and >= 0
- `status`: one of `PLACED`, `PAID`, `SHIPPED`, `CANCELLED`
- `source_file`: assigned by the pipeline for traceability

### Reference entity: customers
**Grain:** one row per `customer_id`; key must be unique and non-null.

### Target: `fact_orders`
One current record per `order_id`; fields include normalized UTC timestamp, amount, status, `source_file`, `run_id`, and `loaded_at`. Re-running a batch uses SQLite `ON CONFLICT(order_id) DO UPDATE`, so it does not create extra target rows. This is a current-state table, not a full event-history store.

### Validation / assumptions
- `order_id`, `customer_id`, `order_ts`, `amount`, and `status` are required.
- Amount zero is allowed; negative and non-finite amounts are rejected.
- Timestamps are normalized to UTC ISO-8601.
- Exact duplicate rows for an order are deduplicated; if the same `order_id` has conflicting content within one batch, all rows for that key are rejected as `DUPLICATE_KEY_CONFLICT`.
- Unknown customer keys are rejected as `INVALID_CUSTOMER_REFERENCE`.
- Unknown columns are treated as an unexpected schema change and fail the batch before publishing. Missing required columns also fail the batch.
- Empty input is a successful no-op run with zero counts.
- Row-level bad data is quarantined; batch-level schema errors fail the run.
- Late/out-of-order timestamps are accepted and retained as event timestamps. No watermark or late-arrival cutoff is imposed in this starter project.
- Deterministic transform: timestamps normalized to UTC and amount rounded to two decimal places.
- Synthetic data only; no credentials or external services are required.

## Idempotency, retry, restart, and backfill

- The target's primary key and upsert make repeated processing of the same `order_id` safe from accidental row multiplication.
- The same input can be rerun. The target remains one row per order key, while run audit records each attempt.
- Batch-level schema failures do not publish target rows. A process interruption during target publication is protected by a SQLite transaction.
- Row rejects are written separately and can be inspected without losing source values.
- For backfills, pass the historical file through the same CLI. Because this target stores current state, a backfill with an existing `order_id` updates that row. A production event-history design would use an append-only event table plus a deduplication key.

## Useful checks

```sql
SELECT COUNT(*) AS target_rows FROM fact_orders;
SELECT status, COUNT(*) AS order_count, ROUND(SUM(amount), 2) AS amount_total
FROM fact_orders GROUP BY status ORDER BY status;
SELECT run_id, status, input_count, accepted_count, rejected_count, error_message
FROM pipeline_runs ORDER BY started_at DESC;
```

## Performance

`python scripts/benchmark.py --rows 100000` generates a fixed-seed dataset and times the local pipeline. Results are environment-dependent and are printed to the terminal. The generator can introduce a configurable number of bad records and duplicates. A reasonable next optimization is chunked CSV reading and batched inserts for datasets larger than memory; for this assignment, vectorized generation and SQLite transactions keep the design understandable.

## Tests

`pytest -q` covers happy path, empty batch, missing columns, malformed values, nulls, exact duplicates, conflicting duplicate keys, invalid references, boundary amount zero, negative amount, unknown status, deterministic timestamp normalization, rerun idempotency, and schema-change failure.

## Limitations / next steps

- SQLite is for a local demo, not concurrent production-scale ingestion.
- Row-level validation is intentionally simple and explicit.
- No orchestration, cloud object storage, secrets, or alerting is included.
- A production version could add schema versioning, structured logs, partitioned storage, CI, a scheduler, and append-only history.
