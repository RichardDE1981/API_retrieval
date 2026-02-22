"""
@module: contracts.base
@layer: metadata
@execution_order: 1_1 (must be imported before any concrete contract)

@responsibility:
    Core metadata definitions for the contract-driven ETL architecture.

@provides:
    - Column definition abstraction
    - Source system metadata structure
    - Target system metadata structure
    - Data model definition (business keys / attributes / measures)
    - Full Contract container object
    - Project root and logs directory resolution

@design_principle:
    This module contains ZERO execution logic.
    It only defines structural metadata classes used across:
        - API layer
        - DB layer
        - Job orchestration
        - Logging
        - CSV export

@side_effects:
    - Ensures existence of /logs directory
------------------------------------------------------
"""

from typing import List, Optional
from dataclasses import dataclass, field
import os

# ------------------------
# Project root
# ------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# ------------------------
# Logs folder
# ------------------------
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)


# ------------------------------------------------------------------
# DATA CONTRACT CLASSES
# ------------------------------------------------------------------

@dataclass
class ColumnDefinition:
    """
    @class: ColumnDefinition
    @layer: metadata
    @description: Defines a single column in the contract (API / staging / final / CSV)
    @attributes:
        - name: column name
        - data_type: logical type (int, string, float, date, datetime)
        - nullable: whether column allows NULL
        - description: optional human-readable explanation
    """
    name: str
    data_type: str
    nullable: bool = True
    description: Optional[str] = None


@dataclass
class SourceDefinition:
    """
    @class: SourceDefinition
    @layer: metadata
    @description:
        Defines the origin of the data.

        Contains:
            - Source system identity
            - API endpoint reference
            - Runtime parameters (city, date range, etc.)

    @note:
        Parameters are dynamic inputs and may vary per job execution.
    """
    source_system: str
    source_name: str
    contract_name: str
    description: str
    contract_url: str
    parameters: dict = field(default_factory=dict)  # dynamic runtime params


@dataclass
class DBTarget:
    """
    @class: DBTarget
    @layer: metadata
    @description:
        Defines a database target configuration.

        Used for:
            - Staging tables
            - Final (dbo) tables

    @attributes:
        - db_type: type of database (e.g., MSSQL)
        - database: database name
        - schema: schema name
        - table: table name
        - merge_procedure: optional stored procedure for merge
        - truncate_before_load: whether table should be truncated before insert
    """
    db_type: str
    database: str
    schema: str
    table: str
    merge_procedure: Optional[str] = None
    truncate_before_load: bool = True


@dataclass
class ModelDefinition:
    """
    @class: ModelDefinition
    @layer: metadata
    @description:
        Defines the structural data model of the contract.

        Structure:
            - business_keys: natural identifiers
            - attributes: descriptive fields
            - measures: numeric values

        Optional:
            - staging_only: columns existing only in staging tables
            - dbo_only: columns existing only in final tables
    """
    business_keys: List[ColumnDefinition]
    attributes: List[ColumnDefinition]
    measures: List[ColumnDefinition]
    staging_only: List[ColumnDefinition] = field(default_factory=list)
    dbo_only: List[ColumnDefinition] = field(default_factory=list)


@dataclass
class TargetDefinition:
    """
    @class: TargetDefinition
    @layer: metadata
    @description:
        Defines where/how contract data is written.

    @attributes:
        - db_targets: dictionary of DBTarget objects
        - medium: "DB" or "CSV"
        - log_file: path for ETL log CSV
        - csv_dump_log_file: path for CSV dump log
        - data_csv_file: optional path for CSV export of data
    """
    db_targets: dict = None
    medium: str = "DB"  # "DB" or "CSV"
    log_file: str = None
    csv_dump_log_file: str = None
    data_csv_file: str = None


@dataclass
class Contract:
    """
    @class: Contract
    @layer: metadata
    @description:
        Complete contract definition aggregating source, target, and model.

    @attributes:
        - source: SourceDefinition object
        - target: TargetDefinition object
        - model: ModelDefinition object
    """
    source: SourceDefinition
    target: TargetDefinition
    model: ModelDefinition