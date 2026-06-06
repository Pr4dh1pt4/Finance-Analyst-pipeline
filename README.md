# DustiniaDelixia Groceria — Finance Analyst Data Pipeline

End-to-end analytics pipeline for **Persona 1: Finance Analyst** of the MCI 2026 Final Project.

> **Business question.** Who are DustiniaDelixia's high-value customers (HVCs), how do they pay, where are they located geographically, and how much revenue is the company leaving on the table by not optimising payment options (installments)?

The stack is fully containerised and reproducible: **Airflow** orchestrates an idempotent ELT, **ClickHouse** is the analytical warehouse, and **Metabase** serves the executive dashboard. A custom **Payment Optimization Revenue Simulator** is included as the value-add component.

---

## 1. Architecture

```
Raw CSVs ──> Airflow DAG ───────────────────────────────> ClickHouse ──> Metabase
              │  extract + validate (per table)              │  dustinia_raw.*      dashboard
              │  load -> dustinia_raw (staging)              │  dustinia_marts.*    + simulator
              │  DQ gate (row counts, orphans)               │
              │  build marts (dim + fct + aggregates)        │
              │  DQ gate (HVC share, negatives)              │
              └  refresh Metabase cache (add-on)             │
```

Layered modelling:

| Layer | Database | Contents |
|-------|----------|----------|
| Staging | `dustinia_raw` | 1:1 copies of the source CSVs |
| Marts | `dustinia_marts` | `dim_customer`, `fct_order_payments`, `dim_geolocation`, and four analytical aggregates |

The marts include `mart_hvc_payment_profile`, `mart_geo_value`, and the value-add `mart_payment_uplift`.

---

## 2. Quick start

```bash
# 1. place the dataset CSVs in ./include/data/  (already staged in this repo)
# 2. bring the whole stack up
docker compose up -d

# 3. wait for health checks, then:
#    Airflow   -> http://localhost:8080   (admin / admin)
#    Metabase  -> http://localhost:3000
#    ClickHouse-> http://localhost:8123
```

Trigger the pipeline:

```bash
# from the Airflow UI, unpause + trigger "dustinia_finance_elt"
# or via CLI:
docker compose exec airflow-scheduler airflow dags trigger dustinia_finance_elt
```

The DAG runs end-to-end in a couple of minutes and is **fully idempotent** — every run rebuilds the marts from scratch, so re-runs never duplicate data.

---

## 3. The DAG (`dustinia_finance_elt`)

| Task | Purpose |
|------|---------|
| `create_databases` | Creates `dustinia_raw` / `dustinia_marts` + staging DDL |
| `extract_validate_*` | Reads each CSV, checks it parses and is non-empty |
| `load_staging_*` | Truncates + bulk-inserts into staging (type coercion for dates/zips) |
| `dq_gate_staging` | **Hard gate**: minimum row counts + orphan-payment check |
| `build_marts` | Executes the dimensional + aggregate SQL |
| `dq_gate_marts` | **Hard gate**: HVC share in 5–15% band, no negative order values |
| `refresh_metabase_cache` | Add-on: pings Metabase so the demo shows fresh data |

Data-quality gates are real `AirflowFailException`s — a bad load stops the pipeline instead of silently shipping wrong numbers to the dashboard.

---

## 4. Metabase dashboard

Connect Metabase to ClickHouse (`host=clickhouse, port=8123, db=dustinia_marts`), then paste each query from [`metabase/dashboard_questions.sql`](metabase/dashboard_questions.sql) into a new SQL question. Cards:

1. KPI — total HVC revenue & share
2. AOV by value segment (the ~5x gap)
3. Payment-method mix within HVCs
4. Installment behaviour by segment
5. Geographic HVC concentration (BR state map)
6. **The money slide** — payment-optimisation uplift
7. HVC revenue trend over time
8. Top HVC cities (filterable)

The dashboard is built for a **non-technical audience** — every card answers a question the Head of Finance actually asked.

---

## 5. Value-add — Payment Optimization Revenue Simulator

`mart_payment_uplift` quantifies the revenue gap between installment and non-installment orders, then projects the uplift if more credit-card orders adopted installments. The accompanying interactive simulator (`metabase/revenue_simulator.html`) lets the Head of Finance drag two levers live during the demo:

- **Installment adoption rate** — what % of addressable orders convert
- **AOV lift capture** — how much of the observed AOV gap is realised

This turns a static finding into a defensible, tunable business case.

---

## 6. Headline findings (from the actual dataset)

- HVCs (top decile by lifetime spend) make up **~10% of customers** but spend **~5.6x more per order** (R$607 vs R$112 AOV).
- **Credit card dominates** HVC revenue (~82%); installment orders carry a **64% higher AOV** than single-payment orders (R$198 vs R$121).
- HVC revenue is geographically concentrated — **SP, RJ, MG** account for the bulk of HVC revenue, but states like **RS and RJ show higher HVC *penetration*** (~10%+), signalling expansion targets.
- A conservative model (30% adoption of the observed AOV gap) projects **~R$84k** of recoverable annual revenue from payment optimisation, with the **Standard and Mid-Value segments holding the largest absolute opportunity** by volume.

---

## 7. Repository layout

```
dustinia_finance/
├── docker-compose.yml
├── dags/
│   └── dustinia_finance_elt.py        # the Airflow DAG
├── sql/
│   ├── staging/01_create_staging.sql  # raw landing tables
│   └── marts/02_build_marts.sql       # dimensional model + aggregates
├── metabase/
│   ├── dashboard_questions.sql        # paste-ready dashboard SQL
│   └── revenue_simulator.html         # value-add interactive tool
├── include/
│   ├── data/                          # source CSVs
│   └── scripts/init_metabase_db.sql
└── docs/
    └── DustiniaDelixia_Finance_Paper.docx  # IEEE-format paper
```
