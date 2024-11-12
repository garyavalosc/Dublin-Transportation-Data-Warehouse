# Dublin Transportation Data Pipeline

## Overview

This project is a data pipeline designed to ingest, clean, and store data from multiple public transportation sources in Dublin, Ireland. The pipeline fetches data daily from five sources:

- Luas Passenger Numbers
- Dublin Bus Passenger Numbers
- Weather Data (Met Éireann) for Merrion Square Station
- Dublin Bikes (last 3 months of historical data)
- Cycle Counts

The ingested data is stored in Snowflake under the `RAW` schema, where it's then ready for further transformation and modeling, such as using dbt to materialize views and clean data based on specific business requirements.

## Requirements

- **Airflow**: Used to orchestrate daily data extraction and loading tasks.
- **Snowflake**: Used as the data warehouse to store raw and transformed data.
- **Python Libraries**:
  - `pandas`: For data manipulation.
  - `requests`: To fetch data from the APIs.
  - `snowflake-connector-python`: To connect and load data into Snowflake.

## Project Structure

```plaintext
├── dags
│   ├── cycle_counts_dag.py      # DAG for Cycle Counts data pipeline
│   ├── dublin_bus_dag.py        # DAG for Dublin Bus data pipeline
│   ├── luas_dag.py              # DAG for Luas data pipeline
│   ├── weather_dag.py           # DAG for Weather data pipeline
│   ├── bikes_dag.py             # DAG for Dublin Bikes data pipeline
├── plugins
│   ├── __init__.py
│   ├── transport_etl.py         # Extract and load functions for each data source
│   ├── transport_utils.py       # Utility functions for logging and data fetching
├── sql
│   └── create-tables.sql        # SQL script to create initial Snowflake tables
├── README.md                    # Project documentation
└── additional_documentation.md  # Explanation of approach, decisions, and challenges
  ```
# Setup Instructions
 
1. Clone the Repository
```
   git clone https://github.com/garyavalosc/dublin-transportation-pipeline
   ```

2. Set Up Python Environment
   It is recommended to use a virtual environment to manage dependencies.
   ```
   python3 -m venv venv
   source venv/bin/activate # On Windows, use `venv\Scripts\activate`
   ```

3. Install Dependencies
   Install all required Python packages.
   ```
   pip install -r requirements.txt
   ```
   Note: Ensure that the Snowflake connector, Pandas, and Apache Airflow are included in your `requirements.txt`.

4. Configure Apache Airflow
   - Initialize Airflow: Run the following commands to initialize Airflow.
     ```
     airflow db init
     ```
   - Create Airflow Connections:
     - Snowflake Connection: In the Airflow UI, go to Admin > Connections and add a new connection.
       - Conn ID: `snowflake_default`
       - Conn Type: Snowflake
       - Account: Snowflake account identifier
       - Username: Snowflake username
       - Password: Snowflake password
       - Database: `DUBLIN_TRANSPORTATION`
       - Warehouse: Snowflake warehouse
     - HTTP Connection (for data sources): Set up an HTTP connection if required by the sources.

5. Set Up Snowflake Database and Tables
   - Log in to your Snowflake account.
   - Create a database and schema if they do not exist:
     ```
     CREATE DATABASE DUBLIN_TRANSPORTATION;
     CREATE SCHEMA RAW;
     ```
   - Run the `sql/create-tables.sql` script to create the required tables in Snowflake.
     You can execute this script directly in the Snowflake UI or use the Snowflake CLI.

6. Configure Airflow DAGs
   Place the DAG files (from the `dags` folder) in your Airflow `dags` directory.
   Edit each DAG as necessary to ensure it points to the correct URLs for data sources.

7. Run the Airflow Scheduler
   Start the Airflow scheduler and web server to activate your DAGs.
   ```
   airflow scheduler &
   airflow webserver --port 8080
   ```

8. Trigger the DAGs
   - Go to the Airflow UI at http://localhost:8080.
   - Trigger each DAG manually or set them to run on a schedule.

## Data Pipeline Workflow

1. Extract: Each DAG fetches data from a respective source (e.g., Cycle Counts, Dublin Bus).
2. Transform: Data is processed and cleaned (if needed) before loading.
3. Load: Raw data is loaded to the RAW schema in Snowflake.

Note: The pipeline currently loads raw data only. Transformation steps for analysis-ready data can be added later using a tool like DBT for data modeling and creating materialized views in Snowflake.

## Future Enhancements

- Transformation and Modeling: Use DBT to define models and transformations on the ingested data, creating a CLEAN schema or materialized views in Snowflake.
- Monitoring and Alerts: Set up monitoring and alerts in Airflow for better pipeline observability.
- Data Quality Checks: Integrate data validation checks (e.g., null checks, data type validation) in the transformation steps.
