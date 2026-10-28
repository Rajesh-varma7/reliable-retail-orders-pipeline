import argparse
from pathlib import Path
import random
from datetime import datetime, timedelta, timezone
import pandas as pd

STATUSES = ["PLACED", "PAID", "SHIPPED", "CANCELLED"]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", default="data/orders.csv")
    parser.add_argument("--bad-rate", type=float, default=0.02)
    parser.add_argument("--duplicate-rate", type=float, default=0.01)
    args = parser.parse_args()
    rng = random.Random(args.seed)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    customer_ids = [f"C{i:05d}" for i in range(max(10, args.rows // 20))]
    Path("data").mkdir(exist_ok=True)
    pd.DataFrame({"customer_id": customer_ids}).to_csv("data/customers.csv", index=False)
    rows = []
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    for i in range(args.rows):
        dt = start + timedelta(minutes=rng.randint(0, max(1, args.rows * 5)))
        amount = round(rng.uniform(0, 5000), 2)
        row = {
            "order_id": f"O{i:08d}",
            "customer_id": rng.choice(customer_ids),
            "order_ts": dt.isoformat(),
            "amount": amount,
            "status": rng.choice(STATUSES),
        }
        rows.append(row)
    bad_count = int(args.rows * args.bad_rate)
    for i in range(bad_count):
        if not rows: break
        row = rows[rng.randrange(len(rows))]
        kind = i % 5
        if kind == 0: row["amount"] = -abs(row["amount"])
        elif kind == 1: row["customer_id"] = "UNKNOWN_CUSTOMER"
        elif kind == 2: row["order_ts"] = "not-a-timestamp"
        elif kind == 3: row["status"] = "UNKNOWN"
        else: row["amount"] = "not-a-number"
    dup_count = int(args.rows * args.duplicate_rate)
    rows.extend(dict(rows[i]) for i in range(min(dup_count, len(rows))))
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"Generated {len(rows)} rows at {out}; customers={len(customer_ids)}; seed={args.seed}")

if __name__ == "__main__":
    main()
