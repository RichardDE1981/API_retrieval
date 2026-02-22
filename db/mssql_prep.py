"""
@module: db.mssql_prep
@layer: database/orchestration
@responsibility:
    SQL Server calendar table preparation & merge orchestration.

@handles:
    - Ensure calendar table exists
    - Merge missing years using dbo.usp_merge_calendar
    - Logging of all steps
    - Reusable across multiple ETL jobs

@used_by:
    - jobs.weather_daily
    - other jobs requiring calendar table

@depends_on:
    - db.mssql
    - contracts.calendar_contract
    - logs.log
    - sqlalchemy

@database_objects_managed:
    - dbo.CALENDAR
    - dbo.LOG_CALENDAR_MERGE

@side_effects:
    - Executes calendar merge stored procedure
    - Inserts missing calendar years
    - Writes execution logs
"""

from contracts.calendar_contract import CALENDAR_CONTRACT
from db import mssql
from logs.log import log_execution


def ensure_calendar_merge(contract):
    """
    @function: ensure_calendar_merge
    @role: orchestration
    @layer: database
    @description:
        Ensures the calendar table exists in dbo schema.
        If missing, creates and populates for full calendar range.
        If partial, merges missing years before/after existing data.
        Logs all actions for traceability.
    @input:
        - contract: Contract object containing target DB info
    @side_effects:
        - Executes dbo.usp_merge_calendar stored procedure
        - Creates LOG_CALENDAR_MERGE table if missing
        - Inserts missing calendar years
        - Logs every step
    """
    # Filter dbo targets
    dbo_target_list = [t for t in contract.target.db_targets.values() if t.schema.lower() == "dbo"]
    if not dbo_target_list:
        raise SystemExit(f"No dbo-schema target defined in contract {contract.source.contract_name}")
    dbo_target = dbo_target_list[0]  # only one dbo target expected

    database = dbo_target.database
    schema, table, usp = dbo_target.schema, "CALENDAR", "usp_merge_calendar"

    # Log start of merge
    log_execution(step=f"Ensuring calendar table {schema}.{table}", contract=contract)

    # Get SQLAlchemy engine
    try:
        engine = mssql.get_sqlalchemy_engine(database)
    except Exception as e:
        raise RuntimeError(f"Failed to get SQL engine for {database}: {e}")

    # Ensure schema exists
    mssql.ensure_schema(engine, schema)

    # Ensure log table for merge logging
    mssql.create_log_table(engine, contract, schema=schema, log_table="LOG_CALENDAR_MERGE")

    with engine.begin() as conn:
        # Check if calendar table exists
        exists_sql = f"""
            SELECT COUNT(*) AS cnt
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}'
        """
        table_exists = conn.execute(mssql.text(exists_sql)).scalar() > 0

        if not table_exists:
            # Full creation if missing
            log_execution(
                step=f"Calendar table {schema}.{table} does not exist, creating and populating",
                contract=contract
            )
            conn.execute(
                mssql.text(f"EXEC {schema}.{usp} @StartYear={CALENDAR_CONTRACT.start_year}, @EndYear={CALENDAR_CONTRACT.end_year}")
            )
            log_execution(
                step=f"Calendar table {schema}.{table} created for years {CALENDAR_CONTRACT.start_year}-{CALENDAR_CONTRACT.end_year}",
                contract=contract
            )
            return

        # Table exists — check min and max year
        minmax_sql = f"""
            SELECT MIN(YEAR(DateID)) AS MinYear, MAX(YEAR(DateID)) AS MaxYear
            FROM {schema}.{table}
        """
        result = conn.execute(mssql.text(minmax_sql)).mappings().first()
        min_year, max_year = result["MinYear"], result["MaxYear"]

        # Determine if merge is required
        merge_needed = False

        # Merge before existing data
        if CALENDAR_CONTRACT.start_year < min_year:
            merge_needed = True
            merge_start, merge_end = CALENDAR_CONTRACT.start_year, min_year - 1
            log_execution(step=f"Merge required for years {merge_start}-{merge_end} (before existing data)", contract=contract)
            conn.execute(mssql.text(f"EXEC {schema}.{usp} @StartYear={merge_start}, @EndYear={merge_end}"))

        # Merge after existing data
        if CALENDAR_CONTRACT.end_year > max_year:
            merge_needed = True
            merge_start, merge_end = max_year + 1, CALENDAR_CONTRACT.end_year
            log_execution(step=f"Merge required for years {merge_start}-{merge_end} (after existing data)", contract=contract)
            conn.execute(mssql.text(f"EXEC {schema}.{usp} @StartYear={merge_start}, @EndYear={merge_end}"))

        if not merge_needed:
            log_execution(step=f"Calendar table {schema}.{table} already up to date ({min_year}-{max_year})", contract=contract)