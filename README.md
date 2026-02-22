# API-to-DB

## Project Overview
**API-to-DB** is a Python-based ETL project designed to retrieve data from external APIs, optionally export CSVs, and load the data into a database. 

This project provides a **quick and lightweight solution** for obtaining required master data or other datasets on a small scale, while ensuring **proper and reliable data handling** throughout the ETL process. It balances speed of deployment with solid, contract-driven data management.

## Key Features
- Fetch data from multiple APIs.  
- Optional CSV export for backup or further analysis.  
- Load weather data directly into a SQL database.  
- Modular and contract-driven architecture.  
- Logs ETL execution with optional CSV logging for traceability.  

## Project Structure

```
project_root/
├─ api/        # API clients for weather and ECB FX
├─ contracts/  # Data contracts and schema definitions
├─ db/         # Database operations and CSV export
├─ db/ddl/     # Required SQL objects
├─ jobs/       # ETL workflows (weather_daily, ezb_fx)
├─ logs/       # Log files and CSV dumps
├─ README.md   # Project description
└─ SCOPE.txt   # script generated project scope
```

## Scope
For technical details of the project structure, see **SCOPE.txt**.  
Highlights include:

- **Weather ETL**: `OpenMeteoDailyClient` in `api/weather_api.py`, loaded into DB via contracts in `contracts/weather_daily_contract.py`.  
- **FX ETL**: `EZBApi` in `api/ezb_api.py`, CSV export only (`jobs/ezb_fx.py`).  
- **Database support**: full staging, logging, CSV writing, and schema alignment (`db/mssql.py`, `db/csv_writer.py`).  
- **Logging**: `logs/log.py` manages execution logs and CSV dumps.

## Prerequisites / Required Database Objects
To push API data into the database, the following SQL objects must be created **in this order**:

1. `db/ddl/create_calendar.sql` – Creates the calendar table.  
2. `db/ddl/ufn_GetDates.sql` – Creates the date-generating function.  
3. `db/ddl/merge_calendar.sql` – Applies merge logic to update the calendar.  

> Ensure these objects are created before running ETL jobs, otherwise workflows like `weather_daily` may fail.

## Getting Started

1. Run SQL scripts in the proper order as listed above.  
2. Configure database connection in `contracts/base.py` or via a `.env` file.  
3. Run ETL jobs:

```bash
python jobs/weather_daily.py    # Fetch and load daily weather data
python jobs/ezb_fx.py          # Fetch ECB FX rates (CSV export)


## Dependencies
# Python 3.12
pandas==3.0.0
requests==2.32.5
sqlalchemy==2.0.46
