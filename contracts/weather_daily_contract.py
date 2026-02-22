"""
@module: contracts.weather_daily_contract
@layer: contract_definition
@contract_name: weather_daily

@description:
    Defines the full contract for WEATHER_DAILY dataset:
    - Source system configuration
    - Target database definitions (staging + final)
    - Data model (business keys, attributes, measures)
    - Runtime parameters (locations + date range)

@used_by:
    - jobs.weather_daily
    - api.weather_api
    - db.mssql

@external_system:
    - Open-Meteo Archive API

@database_objects:
    - stg.FACT_WEATHER_DAILY
    - dbo.FACT_WEATHER_DAILY
    - usp_MergeWeatherDaily
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


WEATHER_DAILY_CONTRACT = Contract(
    source=SourceDefinition(
        source_system="Open-Meteo",
        source_name="Weather Daily",
        contract_name="weather_daily",
        description="Daily historical weather data retrieved from Open-Meteo Archive API.",
        contract_url="https://archive-api.open-meteo.com/v1/archive",

        parameters={
            "locations": [
                {"city": "Vienna", "latitude": 48.2082, "longitude": 16.3738, "timezone": "Europe/Vienna"},
                {"city": "Berlin", "latitude": 52.52, "longitude": 13.4050, "timezone": "Europe/Berlin"}
            ],
            "date_start": "2025-02-01",
            "date_end": "2025-03-31"
        }
    ),
    target=TargetDefinition(
        db_targets={
            "staging": DBTarget(
                db_type="MSSQL",
                database="DWH",
                schema="stg",
                table="FACT_WEATHER_DAILY",
                truncate_before_load=True
            ),
            "final": DBTarget(
                db_type="MSSQL",
                database="DWH",
                schema="dbo",
                table="FACT_WEATHER_DAILY",
                merge_procedure="usp_MergeWeatherDaily",
                truncate_before_load=False
            )
        },
        medium="DB",
        log_file=os.path.join(LOGS_DIR, "weather_daily.csv"),
        csv_dump_log_file=os.path.join(LOGS_DIR, "log_csv_dump.csv"),
        data_csv_file=os.path.join(LOGS_DIR, "weather_daily_data.csv")
    ),
    model=ModelDefinition(
        business_keys=[
            ColumnDefinition("city", "string", nullable=False),
            ColumnDefinition("date", "date", nullable=False)
        ],
        attributes=[
            ColumnDefinition("weather_code", "int"),
        ],
        measures=[
            ColumnDefinition("temperature_2m_max", "float"),
            ColumnDefinition("temperature_2m_min", "float"),
            ColumnDefinition("temperature_2m_mean", "float"),
            ColumnDefinition("precipitation_sum", "float"),
            ColumnDefinition("rain_sum", "float"),
            ColumnDefinition("snowfall_sum", "float"),
            ColumnDefinition("sunrise", "datetime"),
            ColumnDefinition("sunset", "datetime"),
            ColumnDefinition("daylight_duration", "float"),
            ColumnDefinition("wind_speed_10m_max", "float"),
            ColumnDefinition("wind_gusts_10m_max", "float")
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