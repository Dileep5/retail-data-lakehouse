-- Runs once when the postgres volume is first created.
CREATE DATABASE retail_source;   -- operational source system (OLTP)
CREATE DATABASE retail_dw;       -- analytics warehouse (lake_silver + dbt schemas)
CREATE DATABASE airflow;         -- Airflow metadata
