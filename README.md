# Willow Dagster — asset-centric orchestration for private-markets data

A runnable **Dagster** project on the same synthetic private-markets dataset as
the SQL / Python / dbt repos. It orchestrates a small ELT pipeline as
**software-defined assets**, with **asset checks** for data quality and
**partitions** for incremental loading — covering interview sections 3.5
(warehousing/ELT), 3.6 (orchestration), and 3.7 (data quality) in one project.

Runs locally on **dagster + DuckDB**. No cloud needed.

---

## Dagster in one minute (vs Airflow / Kestra)

If you know Airflow or Kestra, here's the mental shift:

- **Airflow/Kestra are task-centric**: you define *operations* and the order they
  run in (a DAG of tasks).
- **Dagster is asset-centric**: you define the **data assets** you want to exist
  (tables, files, models). Each `@asset` function produces one asset; Dagster
  reads the dependencies between them and builds the run order + a live
  **lineage graph** for you. dbt models are assets; so are these DuckDB tables.

**UI vs CLI (a common question):** Dagster is **UI-first for development and
operations** — you run `dagster dev`, then materialize assets, watch the lineage
graph, inspect metadata/logs, trigger backfills, and monitor schedules/sensors
in the browser. The **CLI** is for scaffolding and headless automation (CI,
containers). This mirrors how you've used Airflow's and Kestra's web UIs — the
transfer is mostly conceptual (assets, not tasks), not the interface.

---

## Setup — pick ONE path

### Path A — uv (recommended)
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh    # once per machine
uv sync --extra dev
uv run dagster dev                                 # opens the UI at http://localhost:3000
```
In a Codespace, when you run `dagster dev` a "port 3000" popup offers to open the
UI in your browser — accept it.

### Path B — plain pip + venv
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
dagster dev
```

### Path C — Docker
```bash
docker build -t willow-dagster .
docker run --rm -p 3000:3000 willow-dagster        # UI at http://localhost:3000
```

### Run it headlessly (no UI) — good for a first smoke test
```bash
# build the core warehouse assets and run their quality checks:
uv run dagster asset materialize \
  --select "raw_transactions,raw_funds,raw_investors,raw_nav_monthly,dim_fund,fct_cash_flow" \
  -m willow_dagster.definitions

# materialize ONE monthly partition of the incremental asset:
uv run dagster asset materialize --select nav_by_month --partition 2023-05-01 \
  -m willow_dagster.definitions
```

---

## What to click in the UI (a guided tour)

1. **Assets → lineage graph.** See raw_* → dim_fund → fct_cash_flow wired
   automatically from the `deps=[...]` in the code. Click an asset to see its
   metadata (row counts, grain).
2. **Materialize.** Click "Materialize all" (or pick assets). Watch the run
   stream logs live.
3. **Asset checks.** On `fct_cash_flow`, see the three checks — grain uniqueness,
   valid txn types, and **source-to-target reconciliation** — pass/fail with
   metadata. This is section 3.7 made visible.
4. **Partitions.** Open `nav_by_month` to see the monthly partition grid; select
   a range and backfill just those months (section 3.6 incremental/backfill).
5. **Automation.** Under Automation, see the `daily_warehouse_build` **schedule**
   (cron) and the `new_data_sensor` **sensor** (event-based). Toggle them on to
   watch them evaluate.

To fire the sensor manually: create a file named `TRIGGER_RUN` in the repo root;
the sensor requests a run within ~30s, then consumes the file.

---

## Layout & interview-concept map

```
data/                            the shared CSV dataset
willow_dagster/
  resources.py                   DuckDBWarehouse resource (Snowflake-resource analog)
  assets/
    raw.py         (3.6) ingestion assets; idempotent CREATE OR REPLACE loads
    marts.py       (3.5) dim_fund + fct_cash_flow star schema; deps => lineage
    incremental.py (3.6) MonthlyPartitioned asset; delete+insert per month (watermark idea)
    checks.py      (3.7) asset checks: validation, grain, source->target reconciliation
  definitions.py                 assets + checks + job + schedule + sensor (the entry point)
pyproject.toml                   deps + [tool.dagster] module pointer; uv reads this
uv.lock                          pinned versions (commit it)
Dockerfile / docker-compose.yml / .dockerignore
```

### How this maps to your prep
- **3.5 ELT & warehousing** — assets Extract+Load raw CSVs then Transform *in the
  warehouse* (DuckDB), the ELT shape cloud warehouses made standard. The
  `DuckDBWarehouse` resource is where a **Snowflake** resource would go
  (compute/storage separation, warehouse sizing, etc. live at that boundary).
- **3.6 orchestration** — software-defined assets + lineage, `deps` for DAG
  ordering, **partitions/backfills** for incremental & late data, **schedule**
  (time) and **sensor** (event) triggers, idempotent re-runnable loads.
- **3.7 data quality** — **asset checks** for validation + the **source-to-target
  reconciliation** financial teams require, surfaced in the UI next to the data.

## Notes
- Dagster stores run history under `DAGSTER_HOME`; if unset, `dagster dev` uses a
  temp dir (fine for practice). To persist, `export DAGSTER_HOME=$PWD/.dagster_home`.
- `willow_warehouse.duckdb`, `.venv/`, and Dagster's local storage are gitignored.
- IMPORTANT: these modules deliberately do **not** use
  `from __future__ import annotations` — it stringifies type hints and breaks
  Dagster's context/resource type detection.
