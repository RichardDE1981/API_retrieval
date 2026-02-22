"""
@module: jobs.weather_daily
@layer: orchestration
@description:
    Full orchestration job for WEATHER_DAILY dataset.
    - Ensures SQL Server connectivity & calendar table
    - Fetches daily weather data from Open-Meteo API
    - Aligns DataFrame to contract
    - Inserts into staging (bulk insert)
    - Executes MERGE into final table
    - Optional CSV dump

@used_by:
    - Run from PyCharm / command line
    - jobs orchestrator for nightly API loads

@depends_on:
    - db.mssql
    - db.csv_writer
    - logs.log
    - api.weather_api
    - contracts.weather_daily_contract
"""

import pandas as pd
from datetime import date
from db import mssql
from logs.log import log_execution
from api.weather_api import OpenMeteoDailyClient
from contracts.weather_daily_contract import WEATHER_DAILY_CONTRACT
from db.csv_writer import write_dataframe_to_csv


# ------------------------
# JOB ENTRY POINT
# ------------------------
if __name__ == "__main__":
    """
    @execution: main orchestration
    @description:
        Executes the full WEATHER_DAILY ETL job in DB or CSV mode.
        Performs detailed logging at each step.
    """
    print("=== Weather Daily Job: START ===")
    contract = WEATHER_DAILY_CONTRACT

    # ------------------------
    # JOB START LOG
    # ------------------------
    log_execution(step="JOB START", contract=contract)

    # ------------------------
    # SQL Server Connectivity & Calendar Table
    # ------------------------
    try:
        log_execution(step="Ensuring calendar table and SQL objects", contract=contract)
        mssql.ensure_calendar_table(contract)
        mssql.ensure_db_objects(contract)
    except Exception as e:
        log_execution(step=f"DB setup error: {e}", contract=contract)
        raise SystemExit(f"Terminating orchestration: {e}")

    # ------------------------
    # Contract Parameters Extraction
    # ------------------------
    param = getattr(contract.source, "parameters", None)
    if not param:
        raise ValueError("Contract.source.parameters must be defined.")

    locations = param.get("locations")
    start_date = param.get("date_start")
    end_date = param.get("date_end")
    if not locations or start_date is None or end_date is None:
        raise ValueError("Contract parameters 'locations', 'start_date', 'end_date' missing.")

    # Safe end date (avoid fetching future data)
    today_safe = date.today() - pd.Timedelta(days=5)

    # ------------------------
    # API DATA FETCH LOOP
    # ------------------------
    client = OpenMeteoDailyClient(contract)
    df_list = []

    for loc in locations:
        # Adjust end_date for safety
        if pd.to_datetime(end_date) > pd.to_datetime(today_safe):
            end_date = today_safe.strftime("%Y-%m-%d")

        df = client.fetch(start_date=start_date, end_date=end_date, location=loc)
        df_list.append(df)
        log_execution(
            step=f"Fetched {len(df)} rows for {loc['city']} ({start_date} -> {end_date})",
            contract=contract
        )

    # Concatenate all locations into single DataFrame
    df_weather = pd.concat(df_list, ignore_index=True)
    log_execution(step=f"Total rows fetched across all locations: {len(df_weather)}", contract=contract)

    # ------------------------
    # DEBUG OUTPUT
    # ------------------------
    print("\n=== Weather DataFrame Preview ===")
    print(df_weather.head(10))

    # ------------------------
    # DATABASE MODE
    # ------------------------
    if getattr(contract.target, "medium", "DB").upper() == "DB":
        staging_target = contract.target.db_targets["staging"]

        # TRUNCATE STAGING TABLE
        if staging_target.truncate_before_load:
            truncate_sql = f"TRUNCATE TABLE [{staging_target.schema}].[{staging_target.table}]"
            try:
                mssql.execute(truncate_sql, database=staging_target.database)
                log_execution(step=f"Truncated table {staging_target.schema}.{staging_target.table}", contract=contract)
            except Exception as e:
                log_execution(step=f"TRUNCATE FAILED: {e}", contract=contract)
                raise SystemExit(f"Truncation failed: {e}")

        # ALIGN DATAFRAME TO CONTRACT
        df_weather_aligned = mssql.align_dataframe_to_contract(df_weather, contract, staging=True)

        # Ensure 'source' column exists
        if "source" not in df_weather_aligned.columns or df_weather_aligned["source"].isnull().all():
            df_weather_aligned["source"] = f"{contract.source.source_system} | {contract.source.contract_name}"

        # BULK INSERT TO STAGING
        try:
            engine = mssql.get_sqlalchemy_engine(staging_target.database)

            mssql.write_dataframe_to_sql_bulk(
                df=df_weather_aligned,
                db_target=staging_target,
                engine=engine,
                contract=contract,
                staging=True,
                debug=True
            )

            log_execution(
                step=f"Inserted {len(df_weather_aligned)} rows into {staging_target.schema}.{staging_target.table} (bulk insert)",
                contract=contract
            )
        except Exception as e:
            log_execution(step=f"DB insert error (BULK INSERT): {e}", contract=contract)
            raise SystemExit(f"Terminating orchestration: {e}")

        # MERGE STAGING -> FINAL
        try:
            log_execution(step=f"START MERGE SP for contract {contract.source.contract_name}", contract=contract)
            mssql.ensure_data_merge(contract=contract)
            log_execution(step=f"Data MERGE completed successfully for contract {contract.source.contract_name}", contract=contract)
        except Exception as e:
            log_execution(step=f"Data MERGE failed: {e}", contract=contract)
            raise SystemExit(f"Terminating orchestration due to MERGE SP failure: {e}")

    # ------------------------
    # CSV MODE
    # ------------------------
    elif getattr(contract.target, "medium", "DB").upper() == "CSV" and getattr(contract.target, "data_csv_file", None):
        write_dataframe_to_csv(
            df=df_weather,
            csv_path=contract.target.data_csv_file,
            contract=contract,
            step_description="Weather data CSV dump",
            index=False
        )

    # ------------------------
    # JOB END LOG
    # ------------------------
    log_execution(step="JOB END", contract=contract)
    print(f"=== Weather Daily Job: END ({len(df_weather)} rows) ===")