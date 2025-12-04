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
