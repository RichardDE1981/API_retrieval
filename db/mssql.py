"""
@module: db.mssql
@layer: database
@responsibility:
    SQL Server infrastructure layer for contract-driven ETL processing.

@handles:
    - SQLAlchemy engine creation
    - Schema creation
    - DIM_SOURCE management
    - Log table management
    - Contract-based table creation
    - Metadata validation
    - Calendar dimension validation
    - DataFrame alignment to contract
    - Staging insert (standard + bulk)
    - MERGE stored procedure execution

@used_by:
    - jobs.weather_daily

@depends_on:
    - contracts.base
    - contracts.calendar_contract
    - logs.log
    - sqlalchemy
    - pyodbc
    - pandas

@database_objects_managed:
    - dbo.DIM_SOURCE
    - dbo.LOG_API_PROCESSING
    - dbo.CALENDAR
    - stg.<contract_table>
    - dbo.<contract_table>
    - dbo.usp_MergeWeatherData

@side_effects:
    - Creates schemas and tables
    - Executes stored procedures
    - Inserts and truncates data
    - Writes CSV logs
"""

import os
import csv
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from contracts.base import DBTarget, ColumnDefinition, Contract
from contracts.calendar_contract import CALENDAR_CONTRACT
from logs.log import log_execution

# ------------------------

# ENGINE
# ------------------------
def get_sqlalchemy_engine(database: str, server: str = "localhost"):
    """
    @function: get_sqlalchemy_engine
    @role: connectivity
    @layer: database
    @description:
        Creates SQLAlchemy engine for SQL Server using pyodbc driver.
    @input:
        - database (str)
        - server (str, default=localhost)
    @output:
        - SQLAlchemy Engine
    @side_effects:
        - Opens DB connections on demand
    """
    conn_str = f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    engine = create_engine(conn_str, fast_executemany=True)
    return engine


def derive_source_id(engine) -> int:
    """
    @function: derive_source_id
    @role: utility
    @layer: database
    @description:
        Derives next SourceID from dbo.DIM_SOURCE table.
    @output:
        - int: next SourceID, defaults to 1 if table empty
    @side_effects:
        - None
    """
    try:
        with engine.begin() as conn:
            result = conn.execute(text("SELECT MAX(SourceID) FROM dbo.DIM_SOURCE"))
            max_source_id = result.scalar()
        return int(max_source_id) if max_source_id is not None else 1
    except Exception as e:
        log_execution(step=f"[WARN] Could not fetch MAX(SourceID) from DIM_SOURCE: {e}", contract=None)
        return 1

# ------------------------
# LOGGING
# ------------------------
def log_to_csv(contract: Contract, step: str, row_count: int = None, csv_file: str = None, level="INFO"):
    """
    @function: log_to_csv
    @role: logging
    @layer: database
    @description:
        Logs ETL steps to CSV file and prints WARN/ERROR messages.
    @input:
        - contract: Contract object for metadata
        - step: description of step
        - row_count: optional number of rows affected
        - csv_file: path to CSV log
        - level: INFO / WARN / ERROR
    @side_effects:
        - Writes to CSV
        - Prints warnings/errors
    """
    if not csv_file:
        return

    file_exists = os.path.exists(csv_file)
    with open(csv_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["ExecutionDate", "Level", "ContractName", "SourceSystem", "Step", "RowCount"])
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            level,
            getattr(contract.source, "contract_name", "N/A"),
            getattr(contract.source, "source_system", "N/A"),
            step,
            row_count if row_count is not None else ""
        ])

    if level in ["WARN", "ERROR"]:
        print(f"[{level}] {step} ({row_count if row_count is not None else ''})")
    else:
        log_execution(step=f"[INFO] {step} ({row_count if row_count is not None else ''})", contract=contract)


# ------------------------
# DATATYPE MAPPING
# ------------------------
def map_datatype(datatype: str):
    """
    @function: map_datatype
    @role: utility
    @layer: database
    @description:
        Maps abstract data type to SQL Server datatype.
    @input:
        - datatype (str): e.g., 'string', 'int'
    @output:
        - str: SQL Server datatype string
    """
    mapping = {
        "string": "NVARCHAR(255)",
        "int": "INT",
        "float": "FLOAT",
        "date": "DATE",
        "datetime": "DATETIME"
    }
    return mapping.get(datatype.lower(), "NVARCHAR(255)")


# ------------------------
# SCHEMA
# ------------------------
def ensure_schema(engine, schema: str):
    """
    @function: ensure_schema
    @role: infrastructure
    @layer: database
    @description:
        Creates schema if it does not exist.
    @input:
        - engine: SQLAlchemy engine
        - schema: schema name
    @side_effects:
        - Creates schema in SQL Server
    """
    sql = f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name='{schema}') BEGIN EXEC('CREATE SCHEMA [{schema}]'); END"
    with engine.begin() as conn:
        conn.execute(text(sql))
    log_execution(step=f"Schema [{schema}] ensured.", contract=None)


# ------------------------
# DIM_SOURCE
# ------------------------
def create_dim_source(engine, contract: Contract):
    """
    @function: create_dim_source
    @role: infrastructure
    @layer: database
    @description:
        Creates dbo.DIM_SOURCE table if missing.
    @side_effects:
        - Creates table DIM_SOURCE
    """
    sql = f"""
    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'DIM_SOURCE'
    )
    BEGIN
        CREATE TABLE [dbo].[DIM_SOURCE] (
            [SourceID] INT IDENTITY(1,1) PRIMARY KEY,
            [SourceSystem] NVARCHAR(100) NOT NULL,
            [Description] NVARCHAR(255) NULL,
            [CreatedDate] DATETIME DEFAULT GETDATE()
        );
    END
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    log_execution(step="DIM_SOURCE table ensured in schema [dbo].", contract=contract)


# ------------------------
# LOG TABLE
# ------------------------
def create_log_table(engine, contract: Contract, schema="dbo", log_table="LOG_API_PROCESSING"):
    """
    @function: create_log_table
    @role: infrastructure
    @layer: database
    @description:
        Creates log table if missing.
    @side_effects:
        - Creates table LOG_API_PROCESSING
    """
    sql = f"""
    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{log_table}'
    )
    BEGIN
        CREATE TABLE [{schema}].[{log_table}] (
            [LogID] INT IDENTITY(1,1) PRIMARY KEY,
            [ContractName] NVARCHAR(100) NOT NULL,
            [SourceSystem] NVARCHAR(100) NOT NULL,
            [TargetDB] NVARCHAR(50),
            [TargetSchema] NVARCHAR(50),
            [TargetTable] NVARCHAR(100),
            [SourceID] NVARCHAR(100),
            [RowCount] INT,
            [ExecutionDate] DATETIME DEFAULT GETDATE()
        );
    END
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    log_execution(step=f"Log table [{schema}].[{log_table}] ensured.", contract=contract)


# ------------------------
# CREATE TABLE FROM CONTRACT
# ------------------------
def create_table_from_contract(engine, contract: Contract, db_target: DBTarget, columns: list[ColumnDefinition]):
    """
    @function: create_table_from_contract
    @role: infrastructure
    @layer: database
    @description:
        Creates staging or final table based on contract columns.
        Adds mandatory DB columns for final (dbo) tables.
    @input:
        - engine: SQLAlchemy engine
        - contract: Contract object
        - db_target: DBTarget object
        - columns: list of ColumnDefinition objects
    @side_effects:
        - Creates SQL Server table if missing
        - Adds load_dts / SourceID for dbo
    """
    col_defs = []

    # Identity column only for final dbo table
    if db_target.schema.lower() == "dbo":
        col_defs.append("[ID] INT IDENTITY(1,1) PRIMARY KEY")

    # Contract columns
    for col in columns:
        col_defs.append(
            f"[{col.name}] {map_datatype(col.data_type)} {'NULL' if col.nullable else 'NOT NULL'}"
        )

    # Mandatory DB columns
    if db_target.schema.lower() == "dbo":
        col_defs.append("[load_dts] DATETIME2(7) NOT NULL")
        col_defs.append("[SourceID] INT NOT NULL")
    elif db_target.schema.lower() == "stg":
        col_defs.append("[source] NVARCHAR(255) NOT NULL")  # staging-only descriptive column

    col_defs_sql = ",\n    ".join(col_defs)

    sql = f"""
    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{db_target.schema}' AND TABLE_NAME = '{db_target.table}'
    )
    BEGIN
        CREATE TABLE [{db_target.schema}].[{db_target.table}] (
            {col_defs_sql}
        );
    END
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    log_execution(step=f"Table [{db_target.schema}].[{db_target.table}] ensured with mandatory DB columns if dbo.", contract=contract)


# ------------------------
# VALIDATE TABLE COLUMNS
# ------------------------
def validate_table_columns(engine, db_target: DBTarget, contract_columns: list[ColumnDefinition], contract: Contract):
    """
    @function: validate_table_columns
    @role: validation
    @layer: database
    @description:
        Validates that table columns match contract metadata.
        Checks datatype and nullability.
    @side_effects:
        - Logs warnings/errors if validation fails
    """
    try:
        sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='{db_target.schema}' AND TABLE_NAME='{db_target.table}';
        """
        with engine.begin() as conn:
            result = conn.execute(text(sql)).mappings().all()
        db_cols = {row['COLUMN_NAME']: (row['DATA_TYPE'], row['IS_NULLABLE']) for row in result}

        for col in contract_columns:
            if col.name.upper() == "ID" and db_target.schema.lower() != "dbo":
                continue

            expected_type = map_datatype(col.data_type).split('(')[0].lower()
            db_type, db_nullable = db_cols.get(col.name, (None, None))
            if db_type is None:
                error_msg = f"Column [{col.name}] missing in table {db_target.schema}.{db_target.table}"
                log_to_csv(contract, step=error_msg, row_count=None, csv_file=contract.target.log_file, level="ERROR")
                raise Exception(error_msg)
            if expected_type != db_type.lower():
                error_msg = f"Column [{col.name}] type mismatch: expected {expected_type}, found {db_type}"
                log_to_csv(contract, step=error_msg, row_count=None, csv_file=contract.target.log_file, level="ERROR")
                raise Exception(error_msg)
            expected_null = "YES" if col.nullable else "NO"
            if db_nullable != expected_null:
                error_msg = f"Column [{col.name}] nullability mismatch: expected {expected_null}, found {db_nullable}"
                log_to_csv(contract, step=error_msg, row_count=None, csv_file=contract.target.log_file, level="ERROR")
                raise Exception(error_msg)
    except SQLAlchemyError as e:
        log_to_csv(contract, step=f"METADATA VALIDATION FAILED: {e}", row_count=None, csv_file=contract.target.log_file, level="ERROR")
        raise SystemExit(f"Metadata validation failed: {e}")


# ------------------------
# ENSURE CALENDAR TABLE
# ------------------------
def ensure_calendar_table(contract: Contract):
    """
    @function: ensure_calendar_table
    @role: infrastructure
    @layer: database
    @description:
        Checks that dbo.CALENDAR table exists and covers contract calendar range.
        If missing or incomplete, triggers dbo.MergeDimDate stored procedure.
    @side_effects:
        - Executes MergeDimDate stored procedure if needed
        - Logs execution
    """
    any_target = next(iter(contract.target.db_targets.values()))
    engine = get_sqlalchemy_engine(any_target.database)
    schema, table = "dbo", "CALENDAR"
    log_to_csv(contract, step=f"Ensuring calendar table {schema}.{table}", csv_file=contract.target.log_file)

    with engine.begin() as conn:
        table_exists = conn.execute(
            text(f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}'")
        ).scalar()
        if not table_exists:
            log_to_csv(contract, step=f"Calendar table {schema}.{table} does not exist. Please create it using dim_date.sql",
                       csv_file=contract.target.log_file, level="WARN")
            return

        result = conn.execute(text(f"SELECT MIN([Year]), MAX([Year]) FROM [{schema}].[{table}]")).fetchone()
        min_year, max_year = result[0], result[1]

    start_year, end_year = CALENDAR_CONTRACT.start_year, CALENDAR_CONTRACT.end_year
    if (min_year is None) or (max_year is None) or (min_year > start_year) or (max_year < end_year):
        log_to_csv(contract, step=f"Calendar table requires update: {min_year}-{max_year} vs {start_year}-{end_year}",
                   csv_file=contract.target.log_file, level="INFO")
        with engine.begin() as conn:
            conn.execute(text(f"EXEC dbo.MergeDimDate @StartYear={start_year}, @EndYear={end_year}"))
        log_to_csv(contract, step="Calendar table merged successfully", csv_file=contract.target.log_file)
    else:
        log_to_csv(contract, step=f"Calendar table is up-to-date: {min_year}-{max_year}", csv_file=contract.target.log_file)


# ------------------------
# ENSURE DB OBJECTS
# ------------------------
def ensure_db_objects(contract: Contract):
    """
    @function: ensure_db_objects
    @role: infrastructure
    @layer: database
    @description:
        Ensures schemas, DIM_SOURCE, log tables, staging and final tables.
        Validates metadata.
    @side_effects:
        - Creates schemas, tables
        - Validates contract metadata
        - Logs all operations
    """
    log_to_csv(contract, step="JOB START", csv_file=contract.target.log_file)
    any_target = next(iter(contract.target.db_targets.values()))
    engine = get_sqlalchemy_engine(any_target.database)
    log_to_csv(contract, step=f"Connected to {any_target.database}", csv_file=contract.target.log_file)

    ensure_schema(engine, "dbo")
    create_dim_source(engine, contract)
    create_log_table(engine, contract)

    all_columns = contract.model.business_keys + contract.model.attributes + contract.model.measures

    for name, target in contract.target.db_targets.items():
        ensure_schema(engine, target.schema)
        create_table_from_contract(engine, contract, target, all_columns)
        validate_table_columns(engine, target, all_columns, contract)
        log_to_csv(contract, step=f"{name.capitalize()} table ensured and validated", csv_file=contract.target.log_file)

    log_to_csv(contract, step="JOB END", csv_file=contract.target.log_file)


# ------------------------
# EXECUTE RAW SQL
# ------------------------
def execute(sql: str, database: str):
    """
    @function: execute
    @role: utility
    @layer: database
    @description:
        Executes arbitrary SQL in the specified database.
    @input:
        - sql: SQL string
        - database: target database
    @side_effects:
        - Executes SQL statements
    """
    if not database:
        raise ValueError("Database name must be provided to execute SQL.")
    engine = get_sqlalchemy_engine(database)
    with engine.begin() as conn:
        conn.execute(text(sql))


# ------------------------
# CONTRACT-DRIVEN DATAFRAME ALIGNMENT
# ------------------------
def align_dataframe_to_contract(df: pd.DataFrame, contract: Contract, staging: bool = True) -> pd.DataFrame:
    """
    @function: align_dataframe_to_contract
    @role: transformation
    @layer: database
    @description:
        Aligns pandas DataFrame to contract schema.
        Adds staging or dbo mandatory columns.
    @input:
        - df: raw DataFrame
        - contract: Contract object
        - staging: True for staging table, False for final table
    @output:
        - DataFrame aligned to contract
    @side_effects:
        - None
    """
    medium = getattr(contract.target, "medium", "DB").upper()
    if medium == "CSV":
        allowed_cols = [col.name for col in
                        (contract.model.business_keys + contract.model.attributes + contract.model.measures)]
        df_aligned = df[[c for c in df.columns if c in allowed_cols]].copy()
        return df_aligned

    all_cols = contract.model.business_keys + contract.model.attributes + contract.model.measures
    aligned = pd.DataFrame()
    dtype_map = {"string": "object", "int": "Int64", "float": "float64",
                 "date": "datetime64[ns]", "datetime": "datetime64[ns]", "bit": "boolean"}

    if staging:
        aligned["source"] = f"{contract.source.source_system} | {contract.source.contract_name}"
    else:
        aligned["load_dts"] = pd.Timestamp.now()

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
            except Exception as e:
                log_execution(step=f"Casting column {col.name} failed ({base_type}): {e}", contract=contract)
                aligned[col.name] = pd.Series([pd.NA] * len(df), dtype=dtype_map.get(base_type, "object"))
        else:
            aligned[col.name] = pd.Series([pd.NA] * len(df), dtype=dtype_map.get(base_type, "object"))

    return aligned


# ------------------------
# TRUNCATE STAGING TABLE
# ------------------------
def truncate_staging_table(db_target: DBTarget, engine, contract: Contract):
    """
    @function: truncate_staging_table
    @role: infrastructure
    @layer: database
    @description:
        Truncates a staging table before bulk insert.
        Logs start and completion in CSV logging.
    @input:
        - db_target: DBTarget object for the staging table
        - engine: SQLAlchemy engine
        - contract: Contract object for logging
    @side_effects:
        - Truncates SQL Server table
        - Logs operation in CSV
    """
    table_full_name = f"{db_target.schema}.{db_target.table}"

    try:
        log_to_csv(contract, step=f"TRUNCATE start: {table_full_name}", csv_file=contract.target.log_file)
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE [{db_target.schema}].[{db_target.table}]"))
        log_to_csv(contract, step=f"TRUNCATE completed: {table_full_name}", csv_file=contract.target.log_file)
    except Exception as e:
        log_to_csv(contract, step=f"TRUNCATE failed: {table_full_name} ({e})", level="ERROR",
                   csv_file=contract.target.log_file)
        raise RuntimeError(f"TRUNCATE failed for {table_full_name}: {e}")


# ------------------------
# EXECUTE DATA MERGE SP
# ------------------------
def ensure_data_merge(contract: Contract):
    """
    @function: ensure_data_merge
    @role: transformation
    @layer: database
    @description:
        Executes the MERGE stored procedure for the contract.
        Resolves SourceID and logs execution details.
    @input:
        - contract: Contract object containing target DB info
    @side_effects:
        - Executes dbo.usp_MergeWeatherData
        - Logs success/failure in CSV
    """
    # Find staging target explicitly
    staging_targets = [t for t in contract.target.db_targets.values() if t.schema.lower() == "stg"]
    if not staging_targets:
        raise SystemExit(f"No staging target defined in contract {contract.source.contract_name}")
    staging_target = staging_targets[0]
    database = staging_target.database

    # Get SQLAlchemy engine
    try:
        engine = get_sqlalchemy_engine(database)
    except Exception as e:
        raise RuntimeError(f"Failed to get SQL engine for {database}: {e}")

    # Execute MERGE SP
    try:
        with engine.begin() as conn:
            sql = text(f"""
                DECLARE @return_value INT, @ResolvedSourceID INT;

                EXEC @return_value = [dbo].[usp_MergeWeatherData]
                    @ContractName = :contract_name,
                    @ResolvedSourceID = @ResolvedSourceID OUTPUT;

                SELECT @ResolvedSourceID AS ResolvedSourceID, @return_value AS ReturnValue;
            """)
            result = conn.execute(sql, {"contract_name": contract.source.contract_name}).mappings().all()

        resolved_id = result[0]["ResolvedSourceID"] if result else None
        return_value = result[0]["ReturnValue"] if result else None

        log_execution(
            step=f"MERGE SP executed: ResolvedSourceID={resolved_id}, ReturnValue={return_value}",
            contract=contract
        )

    except Exception as e:
        log_execution(step=f"MERGE SP execution error: {e}", contract=contract)
        raise SystemExit(f"Terminating: MERGE SP failed: {e}")


# ------------------------
# BULK INSERT DATAFRAME TO SQL SERVER
# ------------------------
def write_dataframe_to_sql_bulk(df: pd.DataFrame, db_target: DBTarget, engine, contract: Contract,
                                staging: bool = True, debug: bool = False):
    """
    @function: write_dataframe_to_sql_bulk
    @role: ETL load
    @layer: database
    @description:
        Performs fast bulk insert of a pandas DataFrame into SQL Server.
        Only for staging tables (assumes truncation before insert).
        Converts pandas types to native Python types compatible with pyodbc.
    @input:
        - df: aligned pandas DataFrame
        - db_target: DBTarget object
        - engine: SQLAlchemy engine
        - contract: Contract object for logging
        - staging: bool, True if inserting staging data
        - debug: bool, prints first few rows if True
    @side_effects:
        - Inserts data into SQL Server
        - Logs insertion
    """
    df_to_insert = df.copy()

    # Convert pandas types to Python types for pyodbc fast_executemany
    for col in df_to_insert.columns:
        if pd.api.types.is_integer_dtype(df_to_insert[col].dtype):
            df_to_insert[col] = df_to_insert[col].replace({pd.NA: None}).astype(object)
        elif pd.api.types.is_float_dtype(df_to_insert[col].dtype):
            df_to_insert[col] = df_to_insert[col].replace({pd.NA: None}).astype(object)
        elif pd.api.types.is_datetime64_any_dtype(df_to_insert[col].dtype):
            df_to_insert[col] = df_to_insert[col].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)
        else:
            df_to_insert[col] = df_to_insert[col].replace({pd.NA: None}).astype(object)

    cols = list(df_to_insert.columns)
    placeholders = ", ".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO [{db_target.schema}].[{db_target.table}] ({', '.join(cols)}) VALUES ({placeholders})"

    with engine.begin() as conn:
        raw_conn = conn.connection
        cursor = raw_conn.cursor()
        cursor.fast_executemany = True

        data = [tuple(row) for row in df_to_insert.itertuples(index=False, name=None)]

        if debug:
            log_execution(step=f"[DEBUG] Bulk inserting {len(data)} rows into {db_target.schema}.{db_target.table}", contract=contract)

        cursor.executemany(insert_sql, data)
        cursor.commit()
        log_execution(step=f"Bulk insert completed: {len(data)} rows into {db_target.schema}.{db_target.table}", contract=contract)