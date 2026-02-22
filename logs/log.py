"""
@module: logs.log
@layer: logging
@responsibility:
    Central logging for ETL jobs with optional CSV dump.
    Handles structured messages, row counts, and prevents recursion.
"""

import os
from contracts.base import LOGS_DIR

def _write_log_csv(contract, step, row_count=None, csv_file=None):
    """
    Low-level CSV writer for logging.
    Does NOT call log_execution to avoid recursion.
    Safe even if contract is None.
    """
    import pandas as pd

    if csv_file is None:
        csv_file = os.path.join(LOGS_DIR, "log_execution.csv")

    os.makedirs(os.path.dirname(csv_file), exist_ok=True)

    contract_name = getattr(getattr(contract, "source", None), "contract_name", str(contract) if contract else "")

    log_row = {
        "step": step,
        "row_count": row_count,
        "contract": contract_name
    }

    df = pd.DataFrame([log_row])

    if os.path.exists(csv_file):
        df.to_csv(csv_file, mode="a", header=False, index=False)
    else:
        df.to_csv(csv_file, mode="w", header=True, index=False)


def log_execution(step: str, contract=None, row_count=None, csv_file=None, csv_dump=True):
    """
    High-level logging function: prints + optional CSV.
    Safe to call anywhere in orchestration, even if contract is None.

    Parameters:
        step (str): description
        contract: contract object (optional)
        row_count (int): optional row count
        csv_file (str): optional CSV path
        csv_dump (bool): write CSV if True
    """
    contract_name = getattr(getattr(contract, "source", None), "contract_name", "UNKNOWN_CONTRACT")
    msg = f"[INFO] {step}"
    if row_count is not None:
        msg += f" | rows: {row_count}"
    msg += f" | contract: {contract_name}"

    print(msg)

    if csv_dump:
        _write_log_csv(contract=contract, step=step, row_count=row_count, csv_file=csv_file)