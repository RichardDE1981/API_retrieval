# api/ezb_api.py
from contracts.base import Contract
import requests
from typing import List, Dict


class EZBApi:
    """
    ECB SDW REST API wrapper using wildcard series key.
    Can fetch multiple currencies in one call.
    """

    BASE_URL = "https://data-api.ecb.europa.eu/service/data/EXR"

    def __init__(self, contract: Contract, mock: bool = False):
        self.contract = contract
        self.mock = mock
        self.base_currency = contract.source.parameters["base_currency"]
        self.currencies = set(contract.source.parameters["currencies"])
        self.start_date = contract.source.parameters["date_start"]
        self.end_date = contract.source.parameters["date_end"]
        self.frequency = contract.source.parameters.get("frequency", "D")

    def fetch_all(self) -> List[Dict]:
        """
        Fetch FX rates for all currencies in contract.
        If mock=True, generates fake data for testing.
        """
        if self.mock:
            return self._mock_data()

        url = f"{self.BASE_URL}/{self.frequency}..{self.base_currency}.SP00.A"
        params = {
            "startPeriod": self.start_date,
            "endPeriod": self.end_date,
            "format": "jsondata"
        }

        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for obs in data["data"]["observations"]:
            currency = obs["seriesKey"]["CURRENCY"]
            if currency not in self.currencies:
                continue
            results.append({
                "date": obs["obsDimension"],
                "currency": currency,
                "base_currency": self.base_currency,
                "exchange_rate": obs["obsValue"],
                "source": self.contract.source.source_system
            })
        return results

    def _mock_data(self) -> List[Dict]:
        """Generate fake FX rates for testing without network access"""
        from datetime import datetime, timedelta
        import uuid

        start = datetime.strptime(self.start_date, "%Y-%m-%d")
        end = datetime.strptime(self.end_date, "%Y-%m-%d")
        delta_days = (end - start).days + 1

        rows = []
        for currency in self.currencies:
            for i in range(delta_days):
                date = (start + timedelta(days=i)).strftime("%Y-%m-%d")
                rows.append({
                    "date": date,
                    "currency": currency,
                    "base_currency": self.base_currency,
                    "exchange_rate": round(0.8 + i * 0.001, 4),  # fake rate
                    "source": self.contract.source.source_system,
                    "source_id": uuid.uuid4().int >> 64,
                    "load_dts": datetime.now()
                })
        return rows