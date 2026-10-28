import json
import sqlite3
import pandas as pd
import pytest
from src.pipeline import process_dataframe, run_pipeline, SchemaError, REQUIRED_COLUMNS

CUSTOMERS = {"C1", "C2"}
def base_row(**kwargs):
    row = {"order_id":"O1", "customer_id":"C1", "order_ts":"2025-01-01T12:00:00+00:00",
           "amount":"12.345", "status":"paid"}
    row.update(kwargs)
    return row

def test_happy_path_normalizes_and_rounds():
    clean, rejects = process_dataframe(pd.DataFrame([base_row()]), CUSTOMERS, "x.csv", "run-1", "fixed")
    assert len(clean) == 1 and rejects.empty
    assert clean.iloc[0]["amount"] == 12.35
    assert clean.iloc[0]["status"] == "PAID"
    assert clean.iloc[0]["order_ts"] == "2025-01-01T12:00:00+00:00"

def test_boundary_zero_amount_is_valid():
    clean, rejects = process_dataframe(pd.DataFrame([base_row(amount="0")]), CUSTOMERS, "x.csv", "r", "fixed")
    assert len(clean) == 1 and clean.iloc[0]["amount"] == 0

@pytest.mark.parametrize("amount,reason", [("-0.01","NEGATIVE_AMOUNT"), ("abc","MALFORMED_AMOUNT"),
                                            ("","MISSING_AMOUNT"), ("inf","NON_FINITE_AMOUNT")])
def test_bad_amounts_are_rejected(amount, reason):
    clean, rejects = process_dataframe(pd.DataFrame([base_row(amount=amount)]), CUSTOMERS, "x.csv", "r", "fixed")
    assert clean.empty
    assert reason in rejects.iloc[0]["reject_reasons"]

@pytest.mark.parametrize("field,reason", [("order_id","MISSING_ORDER_ID"), ("customer_id","MISSING_CUSTOMER_ID"),
                                          ("order_ts","MISSING_ORDER_TS"), ("status","MISSING_STATUS")])
def test_missing_required_values(field, reason):
    clean, rejects = process_dataframe(pd.DataFrame([base_row(**{field:None})]), CUSTOMERS, "x.csv", "r", "fixed")
    assert clean.empty and reason in rejects.iloc[0]["reject_reasons"]

def test_malformed_timestamp_and_invalid_status():
    rows = [base_row(order_ts="yesterday"), base_row(order_id="O2", status="MYSTERY")]
    clean, rejects = process_dataframe(pd.DataFrame(rows), CUSTOMERS, "x.csv", "r", "fixed")
    assert clean.empty and len(rejects) == 2
    assert "MALFORMED_ORDER_TS" in rejects.iloc[0]["reject_reasons"]
    assert "INVALID_STATUS" in rejects.iloc[1]["reject_reasons"]

def test_invalid_reference_rejected():
    clean, rejects = process_dataframe(pd.DataFrame([base_row(customer_id="NOPE")]), CUSTOMERS, "x.csv", "r", "fixed")
    assert clean.empty and "INVALID_CUSTOMER_REFERENCE" in rejects.iloc[0]["reject_reasons"]

def test_exact_duplicate_collapsed():
    df = pd.DataFrame([base_row(), base_row()])
    clean, rejects = process_dataframe(df, CUSTOMERS, "x.csv", "r", "fixed")
    assert len(clean) == 1 and rejects.empty

def test_conflicting_duplicate_key_rejects_all_rows_for_key():
    df = pd.DataFrame([base_row(), base_row(amount="99")])
    clean, rejects = process_dataframe(df, CUSTOMERS, "x.csv", "r", "fixed")
    assert clean.empty and len(rejects) == 2
    assert all("DUPLICATE_KEY_CONFLICT" in x for x in rejects["reject_reasons"])

def test_schema_change_fails_before_publish(tmp_path):
    inp = tmp_path / "orders.csv"; customers = tmp_path / "customers.csv"
    pd.DataFrame([base_row()]).assign(unexpected="x").to_csv(inp, index=False)
    pd.DataFrame({"customer_id":["C1"]}).to_csv(customers, index=False)
    with pytest.raises(SchemaError):
        run_pipeline(inp, customers, tmp_path/"db.sqlite", tmp_path/"out")
    conn = sqlite3.connect(tmp_path/"db.sqlite")
    assert conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0] == 0
    assert conn.execute("SELECT status FROM pipeline_runs").fetchone()[0] == "FAILED"

def test_empty_input_is_successful_noop(tmp_path):
    inp = tmp_path / "orders.csv"; customers = tmp_path / "customers.csv"
    pd.DataFrame(columns=REQUIRED_COLUMNS).to_csv(inp, index=False)
    pd.DataFrame({"customer_id":["C1"]}).to_csv(customers, index=False)
    report = run_pipeline(inp, customers, tmp_path/"db.sqlite", tmp_path/"out")
    assert report["status"] == "SUCCESS"
    assert report["input_count"] == report["accepted_count"] == report["rejected_count"] == 0

def test_rerun_is_idempotent_for_target_rows(tmp_path):
    inp = tmp_path / "orders.csv"; customers = tmp_path / "customers.csv"
    pd.DataFrame([base_row()]).to_csv(inp, index=False)
    pd.DataFrame({"customer_id":["C1"]}).to_csv(customers, index=False)
    db = tmp_path/"db.sqlite"; out = tmp_path/"out"
    run_pipeline(inp, customers, db, out)
    run_pipeline(inp, customers, db, out)
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0] == 2

def test_late_event_is_retained_and_timezone_normalized():
    row = base_row(order_ts="2020-01-01T07:00:00-05:00")
    clean, rejects = process_dataframe(pd.DataFrame([row]), CUSTOMERS, "x.csv", "r", "fixed")
    assert rejects.empty
    assert clean.iloc[0]["order_ts"] == "2020-01-01T12:00:00+00:00"

def test_publish_failure_rolls_back_target_transaction(tmp_path):
    import sqlite3
    from src.pipeline import connect_db, publish, TARGET_COLUMNS

    db = tmp_path / "failure.db"
    conn = connect_db(db)

    clean = pd.DataFrame([
        {
            "order_id": "O1",
            "customer_id": "C1",
            "order_ts": "2025-01-01T00:00:00+00:00",
            "amount": 10.0,
            "status": "PAID",
            "source_file": "test.csv",
            "run_id": "run-1",
            "loaded_at": "2025-01-01T00:00:00+00:00",
        },
        {
            "order_id": "O2",
            "customer_id": "C1",
            "order_ts": "2025-01-01T00:00:00+00:00",
            "amount": -1.0,
            "status": "PAID",
            "source_file": "test.csv",
            "run_id": "run-1",
            "loaded_at": "2025-01-01T00:00:00+00:00",
        },
    ], columns=TARGET_COLUMNS)

    with pytest.raises(sqlite3.IntegrityError):
        publish(clean, conn)

    count = conn.execute(
        "SELECT COUNT(*) FROM fact_orders"
    ).fetchone()[0]

    assert count == 0
    conn.close()
