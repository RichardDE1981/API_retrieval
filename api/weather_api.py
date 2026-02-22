# weather_api.py
"""
Module: weather_api
Layer: API Client
Responsibility:
    Contract-driven client for fetching daily historical weather data
    from the Open-Meteo Archive API. Supports multiple locations and
    ensures data aligns with the contract's defined business keys,
    attributes, and measures.

Classes:
    - OpenMeteoDailyClient: Fetches daily weather data for locations defined in a contract.

Dependencies:
    - requests
    - pandas
    - contracts.base.Contract
"""

import requests
import pandas as pd
from contracts.base import Contract


class OpenMeteoDailyClient:
    """
    Contract-driven client for Open-Meteo Daily API.

    Responsibilities:
        - Fetch daily weather data per location using contract metadata.
        - Enforce contract-defined columns (business_keys, attributes, measures).
        - Handle timezone-specific data per location.
    """

    def __init__(self, contract: Contract):
        """
        Initialize the client with a contract.

        Args:
            contract (Contract): Full ETL contract defining source, target, and model metadata.

        Raises:
            ValueError: If the contract does not define a valid 'contract_url'.
        """
        self.contract = contract
        self.endpoint = contract.source.contract_url
        if not self.endpoint:
            raise ValueError("Contract must define 'contract_url' in contract.source")

    def fetch(self, start_date: str, end_date: str, location: dict) -> pd.DataFrame:
        """
        Fetch daily weather data for a single location.

        Args:
            start_date (str): Start date in "YYYY-MM-DD" format.
            end_date (str): End date in "YYYY-MM-DD" format.
            location (dict): Dictionary with keys:
                - "city" (str)
                - "latitude" (float)
                - "longitude" (float)
                - "timezone" (str)

        Returns:
            pd.DataFrame: Daily weather data with columns aligned to the contract.

        Raises:
            ValueError: If location lacks 'timezone' or API response is invalid.
            requests.HTTPError: If the API request fails.
        """
        # Ensure timezone is defined for this location
        timezone = location.get("timezone")
        if not timezone:
            raise ValueError(f"Location {location.get('city', '?')} must have a 'timezone' defined")

        # Determine the contract-driven daily variables to request
        daily_vars = [
            col.name for col in (self.contract.model.attributes + self.contract.model.measures)
            if col.name not in ("city", "latitude", "longitude", "date")  # Exclude business keys
        ]

        # Prepare API parameters
        params = {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "start_date": start_date,
            "end_date": end_date,
            "timezone": timezone,
            "daily": ",".join(daily_vars)
        }

        # Execute the API request
        response = requests.get(self.endpoint, params=params)
        response.raise_for_status()
        data = response.json()

        if "daily" not in data:
            raise ValueError(f"No 'daily' key in API response for {location['city']}: {data}")

        # Convert API response to DataFrame
        df = pd.DataFrame(data["daily"])
        df["city"] = location["city"]
        df["latitude"] = location["latitude"]
        df["longitude"] = location["longitude"]

        # Rename 'time' to business key 'date'
        df.rename(columns={"time": "date"}, inplace=True)

        # Keep only columns defined in the contract
        allowed_cols = {col.name for col in (
            self.contract.model.business_keys +
            self.contract.model.attributes +
            self.contract.model.measures
        )}
        df = df[[c for c in df.columns if c in allowed_cols]]

        return df