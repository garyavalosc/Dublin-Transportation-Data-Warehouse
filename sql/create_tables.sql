-- Create schemas
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS PROCESSED;

-- Raw tables for staging
CREATE OR REPLACE TABLE RAW.LUAS_PASSENGERS (
    date DATE,
    passenger_count INTEGER,
    year INTEGER,
    month INTEGER,
    created_at TIMESTAMP_NTZ,
    source VARCHAR(50)
);

CREATE OR REPLACE TABLE RAW.DUBLIN_BUS_PASSENGERS (
    date DATE,
    passenger_count INTEGER,
    year INTEGER,
    month INTEGER,
    created_at TIMESTAMP_NTZ,
    source VARCHAR(50)
);

CREATE OR REPLACE TABLE RAW.WEATHER_DATA (
    date DATE,
    rainfall_mm FLOAT,
    indicator INTEGER,
    created_at TIMESTAMP_NTZ,
    source VARCHAR(50),
    station VARCHAR(100)
);

CREATE OR REPLACE TABLE RAW.DUBLIN_BIKES (
    station_id STRING,
    timestamp TIMESTAMP_NTZ,
    bikes_available INTEGER,
    docks_available INTEGER,
    is_installed BOOLEAN,
    is_renting BOOLEAN,
    is_returning BOOLEAN,
    created_at TIMESTAMP_NTZ,
    source VARCHAR(50)
);

CREATE OR REPLACE TABLE RAW.CYCLE_COUNTS (
    timestamp TIMESTAMP_NTZ,
    location VARCHAR(100),
    direction VARCHAR(50),
    count INTEGER,
    created_at TIMESTAMP_NTZ,
    source VARCHAR(50)
);

-- Create processed tables for analytics
CREATE OR REPLACE TABLE PROCESSED.DAILY_TRANSPORT_METRICS (
    date DATE,
    transport_type VARCHAR(50),
    passenger_count INTEGER,
    rainfall_mm FLOAT,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

-- Create views for easy querying
CREATE OR REPLACE VIEW PROCESSED.VW_MONTHLY_METRICS AS
SELECT 
    DATE_TRUNC('MONTH', date) as month,
    transport_type,
    SUM(passenger_count) as total_passengers,
    AVG(rainfall_mm) as avg_rainfall
FROM PROCESSED.DAILY_TRANSPORT_METRICS
GROUP BY 1, 2;
