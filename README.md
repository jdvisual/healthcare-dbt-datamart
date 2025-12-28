# Healthcare dbt Analytics Datamart  
### HEDIS Colorectal Cancer (COL) Gap Closure Engine

This project demonstrates a production-style analytics engineering workflow for healthcare quality measurement, implemented using dbt and Snowflake.

The focus of this repository is a HEDIS-compliant gap closure engine for the Colorectal Cancer Screening (COL) measure, designed to be auditable, extensible, interview-ready, and representative of real payer analytics work in Medicare and Medicaid environments.

---

## Project Highlights

- Tri-state gap logic (Closed / Open / Not Eligible)
- Scenario-based closure rules (Colonoscopy, FIT, Stool DNA, etc.)
- Lookback window enforcement
- Durability-based evidence ranking
- Separation of concerns (stage → intermediate → marts)
- Synthetic demo data enabling full execution without PHI

---

## Measure Logic Overview (COL)

### Eligibility

Members are eligible if:
- Age 45–75 as of measurement year end
- Not excluded due to:
  - Colorectal cancer history
  - Total colectomy
  - Hospice enrollment

### Closure Scenarios

A member’s COL gap is closed if qualifying evidence exists within the allowed lookback window:

| Scenario            | Lookback Window |
|---------------------|-----------------|
| Colonoscopy         | 10 years        |
| Sigmoidoscopy       | 5 years         |
| CT Colonography     | 5 years         |
| Stool DNA (FIT-DNA) | 3 years         |
| FIT / FOBT          | Measurement year|

If multiple screening events exist, the engine selects:
1. The most durable screening method
2. The most recent qualifying event

---

## dbt Model Architecture



This repository is intended as a portfolio demonstration of analytics
engineering patterns in healthcare.
