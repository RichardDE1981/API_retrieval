# jobs/ezb_fx.py
from contracts.ezb_fx_contract import EZB_FX_CONTRACT
from api.ezb_api import EZBApi
import pandas as pd
from datetime import datetime
import os
import uuid


def run_ezb_fx(contract=EZB_FX_CONTRACT, mock=True):
    """
    CSV-mode ETL for ECB FX rates, using wildcard API or mock mode.
    """
    api = EZBApi(contract, mock=mock)
    rows = api.fetch_all()

    # If not mock, add source_id and load_dts like other ETLs
    if not mock:
        for row in rows:
            row["source_id"] = uuid.uuid4().int >> 64
            row["load_dts"] = datetime.now()

    df = pd.DataFrame(rows)

    # Write main CSV
    df.to_csv(contract.target.data_csv_file, index=False)
    print(f"[{datetime.now()}] Data CSV written to: {contract.target.data_csv_file}")

    # Update log
    append_log(contract, len(df))
    append_csv_dump_log(contract, len(df))


def append_log(contract, rows_fetched: int):
    log_row = pd.DataFrame([{
        "run_dts": datetime.now(),
        "rows_fetched": rows_fetched,
        "file_written": os.path.basename(contract.target.data_csv_file)
    }])
    log_path = contract.target.log_file
    if os.path.exists(log_path):
        log_row.to_csv(log_path, mode="a", header=False, index=False)
    else:
        log_row.to_csv(log_path, index=False)
    print(f"[{datetime.now()}] Log written to: {log_path}")


def append_csv_dump_log(contract, rows: int):
    dump_row = pd.DataFrame([{
        "file": os.path.basename(contract.target.data_csv_file),
        "rows": rows,
        "dump_dts": datetime.now()
    }])
    dump_log_path = contract.target.csv_dump_log_file
    if os.path.exists(dump_log_path):
        dump_row.to_csv(dump_log_path, mode="a", header=False, index=False)
    else:
        dump_row.to_csv(dump_log_path, index=False)
    print(f"[{datetime.now()}] CSV dump log written to: {dump_log_path}")


if __name__ == "__main__":
    run_ezb_fx(mock=True)  # set mock=False once network access is available