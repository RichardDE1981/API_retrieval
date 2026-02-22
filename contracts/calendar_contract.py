"""
@module: contracts.calendar_contract
@layer: metadata
@execution_order: standalone (can be imported independently)

@responsibility:
    Defines the temporal range for calendar generation in ETL jobs.

@design_principle:
    - DB-agnostic
    - Schema-agnostic
    - Reusable across multiple jobs
    - Minimalistic by intention

@provides:
    - start_year
    - end_year
    - contract_name identifier

@usage:
    Used by calendar generation jobs to determine:
        - lower year boundary
        - upper year boundary

@dynamic_behavior:
    Year range is calculated relative to current system year
    using configurable offsets.

------------------------------------------------------
"""

from types import SimpleNamespace
from datetime import datetime

current_year = datetime.now().year

# --------------------------------------------------
# Configuration Section
# --------------------------------------------------
# START_YEAR_OFFSET:
#     Number of years before current_year.
#
# END_YEAR_OFFSET:
#     Number of years after current_year.
#
# Example:
#     If current_year = 2026
#     START_YEAR_OFFSET = -12  → start_year = 2014
#     END_YEAR_OFFSET = 2      → end_year = 2028
START_YEAR_OFFSET = -12   # years before current year
END_YEAR_OFFSET = 2       # years after current year

# --------------------------------------------------
# Calendar Contract Object
# --------------------------------------------------
# Lightweight contract container.
# Uses SimpleNamespace intentionally to avoid
# dependency on full Contract class structure.
#
# Attributes:
#     contract_name: identifier for orchestration/logging
#     start_year: lower calendar boundary (inclusive)
#     end_year: upper calendar boundary (inclusive)
CALENDAR_CONTRACT = SimpleNamespace(
    contract_name="calendar",
    start_year=current_year + START_YEAR_OFFSET,
    end_year=current_year + END_YEAR_OFFSET
)
