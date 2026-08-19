#!/usr/bin/env python3
"""
Read target/run_results.json after `dbt run` / `dbt build` and surface rows_affected.

dbt stores row counts under each result's adapter_response (not at the top level).
See: https://docs.getdbt.com/reference/artifacts/run-results-json

Usage:
  python scripts/extract_rows_affected_from_run_results.py
  python scripts/extract_rows_affected_from_run_results.py --print
  python scripts/extract_rows_affected_from_run_results.py --inject
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def _rows_from_adapter_response(ar: Dict[str, Any]) -> Optional[int]:
    if not ar:
        return None
    if "rows_affected" in ar:
        return ar["rows_affected"]
    if "query_rows_affected" in ar:
        return ar["query_rows_affected"]
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract rows_affected from dbt run_results.json")
    parser.add_argument(
        "--target-dir",
        type=Path,
        default=Path("target"),
        help="dbt target directory (default: ./target)",
    )
    parser.add_argument(
        "--print",
        action="store_true",
        dest="print_table",
        help="Print relation_name, rows_affected, status to stdout (human-readable)",
    )
    parser.add_argument(
        "--inject",
        action="store_true",
        help="Add top-level rows_affected on each result in run_results.json (backup created)",
    )
    args = parser.parse_args()

    run_results_path = args.target_dir / "run_results.json"
    if not run_results_path.is_file():
        print(f"Missing {run_results_path}. Run dbt from the project root first.", file=sys.stderr)
        sys.exit(1)

    data = json.loads(run_results_path.read_text(encoding="utf-8"))
    results = data.get("results") or []

    flat: List[Dict[str, Any]] = []
    for r in results:
        ar = r.get("adapter_response") or {}
        flat.append(
            {
                "unique_id": r.get("unique_id"),
                "status": r.get("status"),
                "relation_name": r.get("relation_name"),
                "rows_affected": _rows_from_adapter_response(ar),
                "adapter_response": ar,
            }
        )

    summary_path = args.target_dir / "rows_affected_summary.json"
    summary = {
        "metadata": data.get("metadata"),
        "elapsed_time": data.get("elapsed_time"),
        "results": flat,
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote {summary_path} ({len(flat)} result rows).")

    if args.print_table:
        print("relation_name\trows_affected\tstatus\tunique_id")
        for r, row in zip(results, flat):
            rel = r.get("relation_name") or ""
            print(f"{rel}\t{row['rows_affected']}\t{r.get('status')}\t{r.get('unique_id')}")

    if args.inject:
        backup = args.target_dir / "run_results.json.bak"
        backup.write_text(json.dumps(data, indent=2), encoding="utf-8")
        for r, row in zip(results, flat):
            r["rows_affected"] = row["rows_affected"]
        run_results_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        print(f"Injected top-level rows_affected into {run_results_path} (backup: {backup}).")


if __name__ == "__main__":
    main()
