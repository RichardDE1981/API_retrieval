"""
@module: contracts.ezb_fx_contract
@layer: contract_definition
@contract_name: ezb_fx

@description:
    Defines the full contract for ECB FX Rates dataset:
    - Source system configuration (ECB SDW API)
    - Target database definitions (staging + final)
    - Data model (business keys, attributes, measures)
    - Runtime parameters (currencies + date range)

@used_by:
    - jobs.ezb_fx
    - api.ezb_api
    - db.mssql

@external_system:
    - European Central Bank SDW API

@database_objects:
    - stg.FACT_ECB_FX
    - dbo.FACT_ECB_FX
    - usp_MergeEcbFx
"""

from contracts.base import (
    ColumnDefinition,
    SourceDefinition,
    DBTarget,
    TargetDefinition,
    ModelDefinition,
    Contract,
    LOGS_DIR
)
import os


EZB_FX_CONTRACT = Contract(
    source=SourceDefinition(
        source_system="ECB",
        source_name="SDW FX Rates",
        contract_name="ezb_fx",
        description="Daily foreign exchange reference rates from the European Central Bank SDW API.",
        contract_url="https://sdw-wsrest.ecb.europa.eu/help/",

        parameters={
            "base_currency": "EUR",  # ECB rates are always EUR-based
            "currencies": ["USD", "GBP"],
            "date_start": "2025-01-01",
            "date_end": "2025-01-31",
            "frequency": "D"  # daily rates
        }
    ),
    target=TargetDefinition(
        db_targets={
            "staging": DBTarget(
                db_type="MSSQL",
                database="DWH",
                schema="stg",
                table="FACT_ECB_FX",
                truncate_before_load=True
            ),
            "final": DBTarget(
                db_type="MSSQL",
                database="DWH",
                schema="dbo",
                table="FACT_ECB_FX",
                merge_procedure="usp_MergeEcbFx",
                truncate_before_load=False
            )
        },
        medium="CSV",
        log_file=os.path.join(LOGS_DIR, "ezb_fx.csv"),
        csv_dump_log_file=os.path.join(LOGS_DIR, "log_csv_dump.csv"),
        data_csv_file=os.path.join(LOGS_DIR, "ezb_fx_data.csv")
    ),
    model=ModelDefinition(
        business_keys=[
            ColumnDefinition("date", "date", nullable=False),
            ColumnDefinition("currency", "string", nullable=False)
        ],
        attributes=[
            ColumnDefinition("base_currency", "string", nullable=False)
        ],
        measures=[
            ColumnDefinition("exchange_rate", "float")
        ],
        staging_only=[
            ColumnDefinition("source", "string", nullable=False)
        ],
        dbo_only=[
            ColumnDefinition("source_id", "int", nullable=False),
            ColumnDefinition("load_dts", "datetime", nullable=False)
        ]
    )
)