# Healthcare Analytics Datamart (dbt + Snowflake)

This project demonstrates a healthcare analytics engineering pattern for
implementing HEDIS gap closure logic using dbt and Snowflake.

## Key Features
- Medallion architecture (Stage ? Intermediate ? Marts)
- Tri-state gap logic (Qualified / Closed / Not Qualified)
- Reusable gap rules engine implemented via dbt macros
- Gap violation detection and centralized exception fact table
- Snowflake-native execution

## Implemented Measures
- Colorectal Cancer Screening (COL)

## Technologies
- dbt
- Snowflake
- SQL

This repository is intended as a portfolio demonstration of analytics
engineering patterns in healthcare.
