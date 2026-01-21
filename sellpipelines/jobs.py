"""Dagster jobs and schedules for e-commerce pipelines.

This module defines:
- Jobs for manual/grouped execution
- Schedules for automated pipeline runs
"""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
)

# =============================================================================
# JOBS
# =============================================================================

# Job to sync all e-commerce platforms
sync_all_ecommerce_job = define_asset_job(
    name="sync_all_ecommerce",
    selection=AssetSelection.groups("ecommerce"),
    description="Sync products from all e-commerce platforms (Shopee, Redmart, Lazada)",
    tags={
        "team": "data",
        "priority": "high",
    },
)

# Individual platform jobs for targeted runs
sync_shopee_job = define_asset_job(
    name="sync_shopee",
    selection=AssetSelection.assets("dlt_shopee_source_products"),
    description="Sync products from Shopee only",
    tags={
        "platform": "shopee",
        "team": "data",
    },
)

sync_redmart_job = define_asset_job(
    name="sync_redmart",
    selection=AssetSelection.assets("dlt_redmart_source_products"),
    description="Sync products from Redmart only",
    tags={
        "platform": "redmart",
        "team": "data",
    },
)

sync_lazada_job = define_asset_job(
    name="sync_lazada",
    selection=AssetSelection.assets("dlt_lazada_source_products"),
    description="Sync products from Lazada only",
    tags={
        "platform": "lazada",
        "team": "data",
    },
)


# =============================================================================
# SCHEDULES
# =============================================================================

# Daily sync schedule - runs at 6 AM Singapore time (UTC+8)
daily_sync_schedule = ScheduleDefinition(
    name="daily_ecommerce_sync",
    job=sync_all_ecommerce_job,
    cron_schedule="0 6 * * *",  # 6:00 AM daily
    execution_timezone="Asia/Singapore",
    description="Daily sync of all e-commerce platforms at 6 AM SGT",
    default_status=DefaultScheduleStatus.STOPPED,  # Start stopped, enable manually
)

# Hourly sync schedule for high-frequency updates
hourly_sync_schedule = ScheduleDefinition(
    name="hourly_ecommerce_sync",
    job=sync_all_ecommerce_job,
    cron_schedule="0 * * * *",  # Every hour
    execution_timezone="Asia/Singapore",
    description="Hourly sync of all e-commerce platforms",
    default_status=DefaultScheduleStatus.STOPPED,  # Start stopped, enable manually
)

# Individual platform schedules with staggered times to avoid API rate limits
shopee_sync_schedule = ScheduleDefinition(
    name="daily_shopee_sync",
    job=sync_shopee_job,
    cron_schedule="0 6 * * *",  # 6:00 AM daily
    execution_timezone="Asia/Singapore",
    description="Daily Shopee sync at 6 AM SGT",
    default_status=DefaultScheduleStatus.STOPPED,
)

redmart_sync_schedule = ScheduleDefinition(
    name="daily_redmart_sync",
    job=sync_redmart_job,
    cron_schedule="15 6 * * *",  # 6:15 AM daily (15 min after Shopee)
    execution_timezone="Asia/Singapore",
    description="Daily Redmart sync at 6:15 AM SGT",
    default_status=DefaultScheduleStatus.STOPPED,
)

lazada_sync_schedule = ScheduleDefinition(
    name="daily_lazada_sync",
    job=sync_lazada_job,
    cron_schedule="30 6 * * *",  # 6:30 AM daily (30 min after Shopee)
    execution_timezone="Asia/Singapore",
    description="Daily Lazada sync at 6:30 AM SGT",
    default_status=DefaultScheduleStatus.STOPPED,
)


# Export all jobs and schedules
ALL_JOBS = [
    sync_all_ecommerce_job,
    sync_shopee_job,
    sync_redmart_job,
    sync_lazada_job,
]

ALL_SCHEDULES = [
    daily_sync_schedule,
    hourly_sync_schedule,
    shopee_sync_schedule,
    redmart_sync_schedule,
    lazada_sync_schedule,
]
