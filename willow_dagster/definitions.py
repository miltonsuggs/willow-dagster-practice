"""
definitions.py — the Dagster entry point (`dagster dev` loads this).

A Definitions object bundles everything Dagster serves in the UI:
  * assets + asset_checks (your data + its quality gates),
  * resources (the warehouse),
  * jobs (a selectable set of assets to run),
  * a schedule (time-based) and a sensor (event-based) — the two ways runs kick
    off automatically (section 3.6).
"""

from dagster import (
    Definitions, define_asset_job, AssetSelection,
    ScheduleDefinition, RunRequest, SkipReason, sensor, SensorEvaluationContext,
    load_assets_from_package_module, load_asset_checks_from_modules,
)

from . import assets
from .assets import checks as checks_module
from .resources import DuckDBWarehouse

# Auto-discover every @asset in the assets package and every @asset_check.
all_assets = load_assets_from_package_module(assets)
all_checks = load_asset_checks_from_modules([checks_module])

# A JOB is a named selection of assets you can run/schedule. Here: the core
# warehouse build (raw + marts), excluding the partitioned incremental asset.
core_job = define_asset_job(
    name="build_warehouse",
    selection=AssetSelection.groups("raw", "marts"),
)

# SCHEDULE: run the core build every weekday at 6am (cron). Time-based trigger.
daily_schedule = ScheduleDefinition(
    name="daily_warehouse_build",
    job=core_job,
    cron_schedule="0 6 * * 1-5",
    execution_timezone="America/New_York",
)


# SENSOR: event-based trigger. This one is a simple heartbeat example that
# requests a run only when a flag file exists — in real life a sensor watches
# for a new file landing in S3, a table updating, or an upstream job finishing.
@sensor(job=core_job, minimum_interval_seconds=30)
def new_data_sensor(context: SensorEvaluationContext):
    import os
    flag = os.path.join(os.path.dirname(__file__), "..", "TRIGGER_RUN")
    if os.path.exists(flag):
        os.remove(flag)  # consume the trigger so it fires once
        yield RunRequest(run_key=None)
    else:
        yield SkipReason("no TRIGGER_RUN file present")


defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    jobs=[core_job],
    schedules=[daily_schedule],
    sensors=[new_data_sensor],
    resources={"warehouse": DuckDBWarehouse()},
)
