"""Dagster definitions for Sell Pipelines.

This module defines the Dagster Definitions object that registers:
- All dlt assets (Shopee, Redmart, Lazada)
- Asset checks for data quality validation
- Jobs for manual/grouped execution
- Schedules for automated pipeline runs
- The DagsterDltResource for running dlt pipelines
"""

from dagster import Definitions, load_assets_from_modules
from dagster_dlt import DagsterDltResource

from sellpipelines import assets
from sellpipelines.checks import ALL_CHECKS
from sellpipelines.jobs import ALL_JOBS, ALL_SCHEDULES

# Load all assets from the assets module
all_assets = load_assets_from_modules([assets])

# Create the DagsterDltResource
dlt_resource = DagsterDltResource()

# Define the Dagster Definitions
defs = Definitions(
    assets=all_assets,
    asset_checks=ALL_CHECKS,
    jobs=ALL_JOBS,
    schedules=ALL_SCHEDULES,
    resources={
        "dlt": dlt_resource,
    },
)
