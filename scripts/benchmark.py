import argparse, tempfile, time, json
from pathlib import Path
import subprocess, sys
from generate_data import main as _unused

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100000)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        data = tmp / "orders.csv"
        customers = tmp / "customers.csv"
        db = tmp / "retail.db"
        out = tmp / "outputs"
        subprocess.run([sys.executable, str(root / "scripts/generate_data.py"),
                        "--rows", str(args.rows), "--seed", "42", "--output", str(data)],
                       cwd=tmp, check=True)
        # generator writes data/customers.csv relative to cwd; copy to temp so benchmark is isolated
        import shutil
        shutil.copy(tmp / "data/customers.csv", customers)
        started = time.perf_counter()
        subprocess.run([sys.executable, "-m", "src.pipeline", "--input", str(data),
                        "--customers", str(customers), "--db", str(db), "--output-dir", str(out)],
                       cwd=root, check=True, stdout=subprocess.DEVNULL)
        report = json.loads(
            (out / "run_report.json").read_text(encoding="utf-8")
        )
        print("Reconciliation:", report["reconciliation"])

        elapsed = time.perf_counter() - started
        print(f"rows_requested={args.rows} elapsed_seconds={elapsed:.3f} rows_per_second={args.rows/elapsed:.0f}")

if __name__ == "__main__":
    run()
