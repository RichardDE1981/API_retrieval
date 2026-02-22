"""
@module: api.base_api
@layer: api_client
@responsibility:
    Thin API client base. Returns pandas DataFrames for ETL consumption.
@side_effects:
    None
@used_by:
    - jobs.weather_daily
    - api.weather_api (concrete clients)
"""

import pandas as pd


class BaseAPIClient:
    source_system = None

    def fetch(self, **kwargs) -> pd.DataFrame:
        """
            @function: fetch
            @role: api_client
            @description:
                Fetches raw data from the API and returns a pandas DataFrame.
                The returned DataFrame should:
                    - Contain all columns defined in the associated contract
                    - Use types compatible with the ETL ingestion process
            @input:
                - kwargs: API-specific runtime parameters (e.g., locations, date range)
            @output:
                - pd.DataFrame ready for DB load
            @raises:
                - NotImplementedError: must be implemented by subclass
            """
        raise NotImplementedError
