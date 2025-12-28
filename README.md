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






SNOWFLAKE SETUP

USE ROLE ACCOUNTADMIN;

create database if not exists STRAWBERRY;

create schema if not exists STRAWBERRY.RAW;
create schema if not exists STRAWBERRY.BRONZE;
create schema if not exists STRAWBERRY.SILVER;
create schema if not exists STRAWBERRY.GOLD;


use database STRAWBERRY;
use schema RAW;



create table if not exists RAW.EXTERNAL_LOYALTY_EVENTS (
    EVENT_ID        string,
    MEMBER_ID       string,
    EVENT_TYPE      string,
    POINTS          number,
    CREATED_AT      timestamp_tz,
    RAW_PAYLOAD     string,
    LOAD_TS_UTC     timestamp_tz default current_timestamp()
);

CREATE WAREHOUSE IF NOT EXISTS WH_STRAWBERRY
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

GRANT USAGE ON WAREHOUSE WH_STRAWBERRY TO ROLE ACCOUNTADMIN;
GRANT USAGE ON DATABASE STRAWBERRY TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA STRAWBERRY.RAW TO ROLE ACCOUNTADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE STRAWBERRY.RAW.EXTERNAL_LOYALTY_EVENTS TO ROLE ACCOUNTADMIN;

GRANT SELECT ON ALL TABLES IN SCHEMA STRAWBERRY.RAW TO ROLE ACCOUNTADMIN;

-- so future tables also work:
GRANT SELECT ON FUTURE TABLES IN SCHEMA STRAWBERRY.RAW TO ROLE ACCOUNTADMIN;

GRANT USAGE ON DATABASE STRAWBERRY TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA STRAWBERRY.SILVER TO ROLE ACCOUNTADMIN;

-- snapshots create tables in the target schema
GRANT CREATE TABLE ON SCHEMA STRAWBERRY.SILVER TO ROLE ACCOUNTADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA STRAWBERRY.SILVER TO ROLE ACCOUNTADMIN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA STRAWBERRY.SILVER TO ROLE ACCOUNTADMIN;


DESCRIBE TABLE EXTERNAL_LOYALTY_EVENTS;
SELECT * FROM EXTERNAL_LOYALTY_EVENTS ORDER BY LOAD_TS_UTC DESC;




USE SCHEMA SILVER;

SHOW TABLES LIKE 'DIM_MEMBER' IN SCHEMA STRAWBERRY.SILVER;
SHOW VIEWS  LIKE 'DIM_MEMBER' IN SCHEMA STRAWBERRY.SILVER;

-- also check if it exists somewhere else:
SHOW TABLES LIKE 'DIM_MEMBER' IN DATABASE STRAWBERRY;
SHOW VIEWS  LIKE 'DIM_MEMBER' IN DATABASE STRAWBERRY;



DROP TABLE IF EXISTS STRAWBERRY.RAW.PMS_RAW;
DROP TABLE IF EXISTS STRAWBERRY.RAW.SURVEY_RAW;
DROP TABLE IF EXISTS STRAWBERRY.RAW.MEMBERSHIP_RAW;

-- then run the CREATE TABLE statements again

USE DATABASE STRAWBERRY;
USE SCHEMA RAW;

-- PMS: expects record:"..." + src_file_name + load_ts_utc
CREATE TABLE IF NOT EXISTS PMS_RAW (
  RECORD        VARIANT,
  SRC_FILE_NAME STRING,
  LOAD_TS_UTC   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Survey: expects record:"..." + src_file_name + load_ts_utc
CREATE TABLE IF NOT EXISTS SURVEY_RAW (
  RECORD        VARIANT,
  SRC_FILE_NAME STRING,
  LOAD_TS_UTC   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Membership: your stg_membership selects typed columns directly
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

USE DATABASE STRAWBERRY;
USE SCHEMA RAW;

INSERT INTO PMS_RAW (RECORD, SRC_FILE_NAME)
SELECT
  PARSE_JSON('{
    "ID": 1,
    "Reservation_no": "RES-001",
    "Reservation_date": "2025-12-27",
    "Updated_time_utc": "2025-12-27T08:00:00",
    "Member_id": "M001",
    "Hotel_id": 101,
    "Booking_start_date": "2025-12-27",
    "Booking_end_date": "2025-12-28",
    "Status_code": "BOOKED",
    "Room_rate": 1200.00,
    "Total_amount_gross": 1200.00
  }'),
  'pms_seed.json';

INSERT INTO SURVEY_RAW (RECORD, SRC_FILE_NAME)
SELECT
  PARSE_JSON('{
    "ID": 10,
    "Member_id": "M001",
    "Is_anonymous": false,
    "Submitted_on_utc": "2025-12-27T10:00:00",
    "Reservation_no": "RES-001",
    "Hotel_id": 101
  }'),
  'survey_seed.json';

INSERT INTO MEMBERSHIP_RAW (ID, FIRST_NAME, LAST_NAME, STATUS, IS_ACTIVE, SRC_FILE_NAME)
VALUES ('M001', 'Test', 'User', 'GOLD', TRUE, 'membership_seed.csv');
