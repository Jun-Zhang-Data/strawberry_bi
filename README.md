# Strawberry BI – dbt + Snowflake Project

This repository implements the Strawberry Business Intelligence Platform data model and pipelines
for Snowflake using dbt.

Layers (medallion-style):

- RAW    – external to dbt, files landed & copied into Snowflake tables.
- BRONZE – staging models over RAW (schema enforcement, basic cleaning).
- SILVER – conformed dimensions and facts (DIM_MEMBER SCD2, FACT_RESERVATIONS, survey EAV).
- GOLD   – business marts for Finance, Marketing, Sales, Operations.

The design follows the architecture described in the Strawberry BI case study, including:

- DIM_STATUS, DIM_HOTEL, DIM_MEMBER (SCD2)
- FACT_RESERVATIONS with 48h merge window on PMS
- SURVEY_RESPONSE, SURVEY_QUESTION, SURVEY_ANSWER (EAV-style survey model)
- Gold marts for daily revenue, key account bookings, detractor alerts, and member history.

## Quickstart

1. Install dbt & dbt-snowflake in your environment.
2. Create a `profiles.yml` entry named `strawberry_bi` (see `profiles.example.yml`).
3. In Snowflake, create database, schemas, and RAW tables + stages (see case notes).
4. Run:

```bash
dbt deps
dbt debug

dbt seed
dbt run --select tag:bronze
dbt snapshot
dbt run --select tag:silver
dbt run --select tag:gold

dbt test
```

Adapt warehouse/database/schema names as needed.






## Snowflake setup (one-time)

> Run the following in a Snowflake Worksheet as a sufficiently privileged role (example uses `ACCOUNTADMIN` for simplicity).

### 1) Create database, schemas, warehouse, and RAW tables

```sql
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS STRAWBERRY;

CREATE SCHEMA IF NOT EXISTS STRAWBERRY.RAW;
CREATE SCHEMA IF NOT EXISTS STRAWBERRY.BRONZE;
CREATE SCHEMA IF NOT EXISTS STRAWBERRY.SILVER;
CREATE SCHEMA IF NOT EXISTS STRAWBERRY.GOLD;

CREATE WAREHOUSE IF NOT EXISTS WH_STRAWBERRY
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

USE DATABASE STRAWBERRY;
USE SCHEMA RAW;

-- External API landing table
CREATE TABLE IF NOT EXISTS EXTERNAL_LOYALTY_EVENTS (
  EVENT_ID        STRING,
  MEMBER_ID       STRING,
  EVENT_TYPE      STRING,
  POINTS          NUMBER,
  CREATED_AT      TIMESTAMP_TZ,
  RAW_PAYLOAD     STRING,
  LOAD_TS_UTC     TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- RAW landing tables used by dbt staging models
-- PMS + Survey expect a VARIANT column named RECORD, plus SRC_FILE_NAME + LOAD_TS_UTC
CREATE TABLE IF NOT EXISTS PMS_RAW (
  RECORD        VARIANT,
  SRC_FILE_NAME STRING,
  LOAD_TS_UTC   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SURVEY_RAW (
  RECORD        VARIANT,
  SRC_FILE_NAME STRING,
  LOAD_TS_UTC   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Membership is modeled as typed columns in this project
CREATE TABLE IF NOT EXISTS MEMBERSHIP_RAW (
  ID            STRING,
  FIRST_NAME    STRING,
  LAST_NAME     STRING,
  STATUS        STRING,
  IS_ACTIVE     BOOLEAN,
  LOAD_TS_UTC   TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  SRC_FILE_NAME STRING
);

SHOW TABLES LIKE '%_RAW' IN SCHEMA STRAWBERRY.RAW;

