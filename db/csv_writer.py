"""
@module: db.csv_writer
@layer: database/utility
@responsibility:
    Handles contract-driven CSV dumping in ETL orchestration.

@handles:
    - Prepares DataFrame aligned to contract columns
    - Adds timestamps and mandatory columns
    - Ensures CSV folder exists
    - Writes CSV safely (truncate or append)
    - Logs row count and step execution

@used_by:
    - jobs.weather_daily
    - any ETL job requiring CSV export

@depends_on:
    - pandas
    - os
    - datetime
    - db.mssql (for log_execution)

@side_effects:
    - Creates directories
    - Writes CSV files
    - Writes ETL execution logs
"""

import os
import pandas as pd
from datetime import datetime
from db.mssql import log_execution


def prepare_csv_dataframe(df: pd.DataFrame, contract):
    """
    @function: prepare_csv_dataframe
    @role: data preparation
    @layer: utility
    @description:
        Aligns a pandas DataFrame to the contract columns.
        Adds mandatory timestamp column 'dts'.
        Ensures proper type casting and fills missing columns with NA.
        Reorders first 3 columns as ['date', 'city', 'dts'].
    @input:
        - df (pd.DataFrame): source DataFrame
        - contract: Contract object containing model columns
    @output:
        - pd.DataFrame: aligned DataFrame ready for CSV dumping
    @side_effects:
        - None
    """
    all_cols = contract.model.business_keys + contract.model.attributes + contract.model.measures
    dtype_map = {
        "int": "Int64",
        "float": "float",
        "string": "string",
        "date": "datetime64[ns]",
        "datetime": "datetime64[ns]"
    }

    aligned = pd.DataFrame()

    for col in all_cols:
        base_type = col.data_type.split("(")[0].lower()

        if col.name in df.columns:
            try:
                if base_type == "int":
                    aligned[col.name] = pd.to_numeric(df[col.name], errors="coerce").astype("Int64")
                elif base_type == "float":
                    aligned[col.name] = pd.to_numeric(df[col.name], errors="coerce")
                elif base_type in ["date", "datetime"]:
                    aligned[col.name] = pd.to_datetime(df[col.name], errors="coerce")
                else:
                    aligned[col.name] = df[col.name].astype(str)
            except (ValueError, TypeError) as e:
                # fallback if casting fails
                aligned[col.name] = pd.Series([pd.NA] * len(df), dtype=dtype_map.get(base_type, "object"))
                log_execution(step=f"Column '{col.name}' cast failed ({base_type}): {e}", contract=contract)
        else:
            # Column missing in source DataFrame
            aligned[col.name] = pd.Series([pd.NA] * len(df), dtype=dtype_map.get(base_type, "object"))
            log_execution(step=f"Column '{col.name}' missing in source DataFrame, filled with NA", contract=contract)

    # Add timestamp column
    aligned.insert(2, "dts", datetime.now())

    # Reorder first 3 columns if they exist
    final_cols = ["date", "city", "dts"] + [c for c in aligned.columns if c not in ["date", "city", "dts"]]
    return aligned[final_cols]


def write_dataframe_to_csv(
    df: pd.DataFrame,
    csv_path: str,
    contract,
    step_description: str = "CSV dump",
    index: bool = False,
    csv_dump: bool = True,
    truncate: bool = True
):
    """
    @function: write_dataframe_to_csv
    @role: CSV output
    @layer: utility
    @description:
        Writes a DataFrame to CSV, aligned to the contract columns.
        Creates target folder if missing.
        Supports truncate or append mode.
        Logs row count and execution step.
    @input:
        - df (pd.DataFrame): DataFrame to write
        - csv_path (str): full path to target CSV file
        - contract: Contract object for alignment and logging
        - step_description (str): description for logging
        - index (bool): include index column in CSV
        - csv_dump (bool): whether to log CSV dump
        - truncate (bool): if True, overwrite CSV; else append
    @output:
        - None
    @side_effects:
        - Creates directories
        - Writes CSV files
        - Logs execution
    """
    try:
        # Align columns before dumping
        df_prepared = prepare_csv_dataframe(df, contract)

        # Ensure folder exists
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)

        # Write CSV
        if truncate or not os.path.exists(csv_path):
            df_prepared.to_csv(csv_path, index=index)
        else:
            df_prepared.to_csv(csv_path, mode='a', header=False, index=index)

        # Log execution
        log_execution(
            step=f"{step_description} written to {csv_path} ({len(df_prepared)} rows)",
            contract=contract,
            df=df_prepared,
            csv_dump=csv_dump
        )

    except Exception as e:
        log_execution(
            step=f"CSV dump failed: {e}",
            contract=contract
        )
        raise SystemExit(f"CSV write failed: {e}")