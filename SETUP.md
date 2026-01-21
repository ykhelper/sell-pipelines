# Dagster + dlt Pipeline Setup Guide

This guide explains how to set up and run the e-commerce data pipelines (Shopee, Redmart, Lazada) using Dagster for orchestration and dlt for data loading.

## Project Structure

```
sell-pipelines/
├── sellpipelines/           # Dagster package
│   ├── __init__.py
│   ├── assets.py            # Dagster dlt_assets definitions
│   ├── definitions.py       # Dagster Definitions entry point
│   └── sources.py           # dlt sources for all platforms
├── shopeeclient/            # Shopee API client
├── lazadaclient/            # Lazada/Redmart API client
├── .env                     # Environment variables (credentials)
├── pyproject.toml           # Project dependencies
└── SETUP.md                 # This file
```

## Prerequisites

1. **Python 3.13+**
2. **uv** package manager (install via `curl -LsSf https://astral.sh/uv/install.sh | sh`)

## Step 1: Install Dependencies

```bash
uv sync
```

## Step 2: Configure Environment Variables

Create or update `.env` file with your API credentials:

```bash
# Shopee credentials
SHOPEE_APP_ID=your_partner_id
SHOPEE_APP_KEY=your_partner_key
SHOPEE_SHOP_ID=your_shop_id
SHOPEE_ACCESS_TOKEN=your_access_token

# Redmart credentials
REDMART_APP_KEY=your_app_key
REDMART_APP_SECRET=your_app_secret
REDMART_ACCESS_TOKEN=your_access_token
REDMART_STORE_ID=your_store_id

# Lazada credentials
LAZADA_APP_KEY=your_app_key
LAZADA_APP_SECRET=your_app_secret
LAZADA_ACCESS_TOKEN=your_access_token
```

## Step 3: Run Dagster Dev Server

Start the Dagster development server:

```bash
uv run dagster dev
```

This will start:
- **Dagster UI** at http://localhost:3000

## Step 4: Materialize Assets

1. Open http://localhost:3000 in your browser
2. Navigate to **Assets** in the left sidebar
3. You'll see three asset groups under `ecommerce`:
   - `shopee` - Shopee products
   - `redmart` - Redmart products
   - `lazada` - Lazada products
4. Click on an asset and click **Materialize** to run the pipeline

## Running Individual Pipelines (Without Dagster)

You can still run the original pipelines directly:

```bash
# Run Shopee pipeline
uv run python shopee_pipeline.py

# Run Redmart pipeline
uv run python redmart_pipeline.py

# Run Lazada pipeline
uv run python lazada_pipeline.py
```

## Data Output

All pipelines load data into DuckDB:
- **Shopee**: `shopee_data` dataset
- **Redmart**: `redmart_data` dataset
- **Lazada**: `lazada_data` dataset

DuckDB files are stored in the `.dlt` directory.

## Troubleshooting

### Missing credentials error
Ensure all required environment variables are set in `.env` file.

### Module not found errors
Run `uv sync` to install all dependencies.

### Port already in use
Kill existing Dagster process or use a different port:
```bash
uv run dagster dev --port 3001
```

┌──────────┬──────────────┬───────────────┐
│ Platform │ Access Token │ Refresh Token │
├──────────┼──────────────┼───────────────┤
│ Shopee   │ 4 hours      │ ~30 days      │
├──────────┼──────────────┼───────────────┤
│ Lazada   │ 7 days       │ ~30 days      │
├──────────┼──────────────┼───────────────┤
│ Redmart  │ 1 day        │ ~30 days      │
└──────────┴──────────────┴───────────────┘
