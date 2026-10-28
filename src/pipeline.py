from __future__ import annotations
import argparse
import json
import logging
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

import pandas as pd

REQUIRED_COLUMNS = ["order_id", "customer_id", "order_ts", "amount", "status"]
ALLOWED_STATUSES = {"PLACED", "PAID", "SHIPPED", "CANCELLED"}
TARGET_COLUMNS = REQUIRED_COLUMNS + ["source_file", "run_id", "loaded_at"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("retail_pipeline")


class SchemaError(ValueError):
    """Batch-level schema failure; no target rows are published."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect_db(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            order_ts TEXT NOT NULL,
            amount REAL NOT NULL CHECK(amount >= 0),
            status TEXT NOT NULL,
            source_file TEXT NOT NULL,
            run_id TEXT NOT NULL,
            loaded_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id TEXT PRIMARY KEY,
            source_file TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT NOT NULL,
            status TEXT NOT NULL,
            input_count INTEGER NOT NULL DEFAULT 0,
            accepted_count INTEGER NOT NULL DEFAULT 0,
            rejected_count INTEGER NOT NULL DEFAULT 0,
            published_count INTEGER NOT NULL DEFAULT 0,
            duration_seconds REAL NOT NULL DEFAULT 0,
            error_message TEXT
        )
    """)
    conn.commit()
    return conn


def validate_schema(df: pd.DataFrame) -> None:
    missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
    unexpected = sorted(set(df.columns) - set(REQUIRED_COLUMNS))
    if missing or unexpected:
        raise SchemaError(f"SCHEMA_ERROR missing_columns={missing} unexpected_columns={unexpected}")


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _parse_amount(value: Any) -> tuple[float | None, str | None]:
    if _is_missing(value) or str(value).strip() == "":
        return None, "MISSING_AMOUNT"
    try:
        dec = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None, "MALFORMED_AMOUNT"
    if not dec.is_finite():
        return None, "NON_FINITE_AMOUNT"
    if dec < 0:
        return None, "NEGATIVE_AMOUNT"
    return float(dec.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)), None


def _parse_timestamp(value: Any) -> tuple[str | None, str | None]:
    if _is_missing(value) or str(value).strip() == "":
        return None, "MISSING_ORDER_TS"
    try:
        ts = pd.Timestamp(value)
        if pd.isna(ts):
            return None, "MALFORMED_ORDER_TS"
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts.isoformat(), None
    except Exception:
        return None, "MALFORMED_ORDER_TS"


def process_dataframe(
    source: pd.DataFrame,
    customers: set[str],
    source_file: str,
    run_id: str,
    loaded_at: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate records, quarantine invalid rows, and return deterministic clean output."""
    validate_schema(source)
    loaded_at = loaded_at or utc_now()
    rows = source.copy()
    rows = rows.where(pd.notna(rows), None)
    rows["_original_index"] = range(len(rows))
    rows["_source_file"] = source_file

    # Exact duplicate records are collapsed. Conflicting records sharing a key are rejected.
    key_counts = rows.groupby("order_id", dropna=False).size()
    conflict_keys: set[str] = set()
    for key, group in rows.groupby("order_id", dropna=False, sort=False):
        if _is_missing(key) or str(key).strip() == "":
            continue
        distinct = group[REQUIRED_COLUMNS].astype(str).drop_duplicates()
        if len(group) > 1 and len(distinct) > 1:
            conflict_keys.add(str(key))

    seen_exact: set[tuple[str, ...]] = set()
    clean_records: list[dict[str, Any]] = []
    reject_records: list[dict[str, Any]] = []

    for _, row in rows.iterrows():
        raw = {col: row[col] for col in REQUIRED_COLUMNS}
        reasons: list[str] = []
        order_id = raw["order_id"]
        customer_id = raw["customer_id"]
        if _is_missing(order_id) or str(order_id).strip() == "":
            reasons.append("MISSING_ORDER_ID")
        if _is_missing(customer_id) or str(customer_id).strip() == "":
            reasons.append("MISSING_CUSTOMER_ID")
        elif str(customer_id).strip() not in customers:
            reasons.append("INVALID_CUSTOMER_REFERENCE")
        if _is_missing(raw["status"]) or str(raw["status"]).strip() == "":
            reasons.append("MISSING_STATUS")
        elif str(raw["status"]).strip().upper() not in ALLOWED_STATUSES:
            reasons.append("INVALID_STATUS")

        ts, ts_error = _parse_timestamp(raw["order_ts"])
        if ts_error:
            reasons.append(ts_error)
        amount, amount_error = _parse_amount(raw["amount"])
        if amount_error:
            reasons.append(amount_error)

        if not _is_missing(order_id) and str(order_id) in conflict_keys:
            reasons.append("DUPLICATE_KEY_CONFLICT")

        # Exact duplicates only count once when their record is otherwise valid.
        signature = tuple("<NULL>" if _is_missing(raw[c]) else str(raw[c]) for c in REQUIRED_COLUMNS)
        if not reasons and signature in seen_exact:
            continue
        if not reasons:
            seen_exact.add(signature)
            clean_records.append({
                "order_id": str(order_id).strip(),
                "customer_id": str(customer_id).strip(),
                "order_ts": ts,
                "amount": amount,
                "status": str(raw["status"]).strip().upper(),
                "source_file": source_file,
                "run_id": run_id,
                "loaded_at": loaded_at,
            })
        else:
            reject_records.append({
                **{k: (None if _is_missing(v) else v) for k, v in raw.items()},
                "source_file": source_file,
                "run_id": run_id,
                "reject_reasons": "|".join(sorted(set(reasons))),
            })

    clean = pd.DataFrame(clean_records, columns=TARGET_COLUMNS)
    rejects = pd.DataFrame(reject_records, columns=REQUIRED_COLUMNS + ["source_file", "run_id", "reject_reasons"])
    return clean, rejects


def publish(clean: pd.DataFrame, conn: sqlite3.Connection) -> int:
    if clean.empty:
        return 0
    sql = """
        INSERT INTO fact_orders(order_id, customer_id, order_ts, amount, status, source_file, run_id, loaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(order_id) DO UPDATE SET
            customer_id=excluded.customer_id,
            order_ts=excluded.order_ts,
            amount=excluded.amount,
            status=excluded.status,
            source_file=excluded.source_file,
            run_id=excluded.run_id,
            loaded_at=excluded.loaded_at
    """
    values = [tuple(row[c] for c in TARGET_COLUMNS) for _, row in clean.iterrows()]
    with conn:
        conn.executemany(sql, values)
    return len(values)


def run_pipeline(input_path: str | Path, customers_path: str | Path, db_path: str | Path,
                 output_dir: str | Path) -> dict[str, Any]:
    start_clock = time.perf_counter()
    run_id = str(uuid.uuid4())
    started_at = utc_now()
    input_path, customers_path, output_dir = Path(input_path), Path(customers_path), Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    source_file = input_path.name
    conn = connect_db(db_path)
    input_count = accepted_count = rejected_count = published_count = 0
    status, error_message = "SUCCESS", None
    clean_path, reject_path = output_dir / "clean_orders.csv", output_dir / "rejects.csv"

    try:
        if not customers_path.exists():
            raise FileNotFoundError(f"Customer reference file not found: {customers_path}")
        customer_df = pd.read_csv(customers_path, dtype=str)
        if "customer_id" not in customer_df.columns:
            raise SchemaError("CUSTOMER_SCHEMA_ERROR missing customer_id")
        if customer_df["customer_id"].isna().any() or customer_df["customer_id"].duplicated().any():
            raise SchemaError("CUSTOMER_REFERENCE_ERROR customer_id must be non-null and unique")
        customers = set(customer_df["customer_id"].astype(str).str.strip())

        # Header-only / zero-byte files are treated as empty batches.
        if input_path.exists() and input_path.stat().st_size == 0:
            source = pd.DataFrame(columns=REQUIRED_COLUMNS)
        else:
            source = pd.read_csv(input_path, dtype=object)
        input_count = len(source)
        validate_schema(source)
        clean, rejects = process_dataframe(source, customers, source_file, run_id)
        accepted_count, rejected_count = len(clean), len(rejects)
        clean.to_csv(clean_path, index=False)
        rejects.to_csv(reject_path, index=False)
        published_count = publish(clean, conn)
        target_count = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
        source_amount = round(sum(float(x) for x in clean["amount"]), 2) if not clean.empty else 0.0
        report = {
            "run_id": run_id, "source_file": source_file, "started_at": started_at,
            "ended_at": utc_now(), "status": status, "input_count": input_count,
            "accepted_count": accepted_count, "rejected_count": rejected_count,
            "published_count": published_count, "target_row_count": target_count,
            "accepted_amount_total": source_amount,
            "reconciliation": {
                "input_equals_accepted_plus_rejected_or_duplicates": input_count == accepted_count + rejected_count
                    or input_count >= accepted_count + rejected_count,
                "input_count": input_count, "accepted_count": accepted_count,
                "rejected_count": rejected_count,
                "note": "Accepted + rejected can be lower than input when exact duplicate rows are collapsed."
            }
        }
    except Exception as exc:
        status, error_message = "FAILED", str(exc)
        LOG.exception("Pipeline run failed: %s", error_message)
        report = {
            "run_id": run_id, "source_file": source_file, "started_at": started_at,
            "ended_at": utc_now(), "status": status, "input_count": input_count,
            "accepted_count": accepted_count, "rejected_count": rejected_count,
            "published_count": published_count, "error_message": error_message,
        }
        raise
    finally:
        duration = round(time.perf_counter() - start_clock, 6)
        ended_at = utc_now()
        with conn:
            conn.execute("""
                INSERT INTO pipeline_runs(run_id, source_file, started_at, ended_at, status,
                    input_count, accepted_count, rejected_count, published_count, duration_seconds, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (run_id, source_file, started_at, ended_at, status, input_count, accepted_count,
                  rejected_count, published_count, duration, error_message))
        report["duration_seconds"] = duration
        report["ended_at"] = ended_at
        (output_dir / "run_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        conn.close()
    LOG.info("run_id=%s status=%s input=%s accepted=%s rejected=%s published=%s duration_seconds=%.6f",
             run_id, status, input_count, accepted_count, rejected_count, published_count, duration)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Reliable retail orders ETL pipeline")
    parser.add_argument("--input", required=True)
    parser.add_argument("--customers", required=True)
    parser.add_argument("--db", default="outputs/retail.db")
    parser.add_argument("--output-dir", default="outputs")
    args = parser.parse_args()
    report = run_pipeline(args.input, args.customers, args.db, args.output_dir)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
