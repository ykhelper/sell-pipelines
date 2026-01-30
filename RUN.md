# How to Run Sell Pipelines

This guide will help you run the e-commerce data pipeline server that extracts product data from Shopee, Redmart, and Lazada.

## Prerequisites

- Python 3.13 or higher
- uv (Python package manager) - [Install here](https://docs.astral.sh/uv/)
- API credentials for the platforms you want to use (Shopee, Redmart, and/or Lazada)

## Quick Start

### 1. Install Dependencies

Open your terminal in the project directory and run:

```bash
uv sync
```

This will install all required dependencies including:
- Dagster (orchestration framework)
- DLT (data load tool)
- DuckDB (local database)
- Platform-specific API clients

### 2. Configure Environment Variables

Create a `.env` file in the project root with your API credentials:

```bash
# Shopee Configuration
SHOPEE_APP_ID=your_shopee_app_id
SHOPEE_APP_KEY=your_shopee_app_key
SHOPEE_SHOP_ID=your_shopee_shop_id
SHOPEE_REFRESH_TOKEN=your_shopee_refresh_token

# Redmart Configuration
REDMART_APP_KEY=your_redmart_app_key
REDMART_APP_SECRET=your_redmart_app_secret
REDMART_REFRESH_TOKEN=your_redmart_refresh_token
REDMART_STORE_ID=your_redmart_store_id

# Lazada Configuration
LAZADA_APP_KEY=your_lazada_app_key
LAZADA_APP_SECRET=your_lazada_app_secret
LAZADA_REFRESH_TOKEN=your_lazada_refresh_token
```

**Note:** You only need to configure credentials for the platforms you intend to use. If you only use Shopee, you can leave the Redmart and Lazada fields empty.

### 3. Generate Access Tokens from Refresh Tokens

Before running the pipelines, you need to generate fresh access tokens from your refresh tokens:

#### For Shopee:

```bash
uv run python shopee_auth.py refresh
```

This will output:
```
=== Shopee Token Refresh: Success! ===

New Access Token: <token>
New Refresh Token: <token>

Update these in your .env file:
SHOPEE_ACCESS_TOKEN=<token>
SHOPEE_REFRESH_TOKEN=<token>
```

Copy the tokens and update your `.env` file.

#### For Redmart:

```bash
uv run python redmart_auth.py refresh
```

Update your `.env` file with the new tokens.

#### For Lazada:

```bash
uv run python lazada_auth.py refresh
```

Update your `.env` file with the new tokens.

**Important Notes:**
- Access tokens expire after a certain period (typically 4 hours for Shopee, varies for others)
- The pipelines will automatically refresh tokens during execution if a refresh token is provided
- However, it's good practice to generate fresh tokens before starting the server
- The refresh tokens themselves also expire (typically after 30 days), so you may need to re-authorize periodically

### 4. Start the Dagster Web Server

Run the following command to start the Dagster web interface:

```bash
uv run dagster dev
```

This will:
- Start the Dagster web server on http://localhost:3000
- Auto-reload when you make code changes
- Display logs and execution history

### 5. Access the Web Interface

Open your browser and navigate to:

```
http://localhost:3000
```

You'll see the Dagster UI with:
- **Assets**: View and materialize your data assets (Shopee, Redmart, Lazada products)
- **Jobs**: Run grouped operations
- **Runs**: Monitor current and past pipeline executions
- **Schedules**: View and manage automated runs

## Running the Pipelines

### Option 1: Using the Web Interface (Recommended)

1. Navigate to **Assets** in the left sidebar
2. You'll see three assets in the "ecommerce" group:
   - `shopee/products`
   - `redmart/products`
   - `lazada/products`
3. Click on any asset to see details
4. Click **Materialize** to run the pipeline for that platform
5. Monitor progress in real-time with logs

### Option 2: Using the Command Line

To materialize a specific asset:

```bash
# Run Shopee pipeline
uv run dagster asset materialize -m sellpipelines.definitions --select shopee

# Run Redmart pipeline
uv run dagster asset materialize -m sellpipelines.definitions --select redmart

# Run Lazada pipeline
uv run dagster asset materialize -m sellpipelines.definitions --select lazada

# Run all pipelines
uv run dagster asset materialize -m sellpipelines.definitions --select "*"
```

### Option 3: Using Jobs

Jobs allow you to run multiple related assets together:

```bash
# Run all ecommerce pipelines at once
uv run dagster job execute -m sellpipelines.definitions -j all_ecommerce_job
```

## What the Pipeline Does

When you run a pipeline, it will:

1. **Authenticate** with the platform API using your credentials
   - Automatically refreshes access tokens if expired (when refresh token is provided)
2. **Fetch product data**:
   - Shopee: Item IDs → Full product details
   - Redmart: Paginated product list
   - Lazada: Paginated product list
3. **Transform data** to a unified schema with fields:
   - `platform_id`: Unique product ID on the platform
   - `platform_name`: Name of the platform (shopee/redmart/lazada)
   - `product_name`: Product title
   - `price`: Current price
   - `stock`: Available stock quantity
   - `status`: Product status
   - `image_url`: Product image URL
   - `store_id`: Seller/store identifier
4. **Load data** into DuckDB at `data.duckdb`
5. **Run quality checks** to validate the data

## Viewing the Data

The data is stored in a DuckDB database at `data.duckdb` in the `sell_data` schema.

### Using DuckDB CLI

```bash
# Install DuckDB CLI if you don't have it
# On macOS: brew install duckdb
# On Linux: Download from https://duckdb.org/

# Open the database
duckdb data.duckdb

# Query the data
SELECT * FROM sell_data.products LIMIT 10;
SELECT platform_name, COUNT(*) FROM sell_data.products GROUP BY platform_name;
```

### Using Python

```python
import duckdb

conn = duckdb.connect('data.duckdb')
df = conn.execute('SELECT * FROM sell_data.products').df()
print(df.head())
```

## Troubleshooting

### Authentication Errors

If you get authentication errors:
1. **Refresh your access tokens** using the auth scripts (Step 3 above)
2. Verify your credentials in `.env` are correct
3. Check if your refresh token has expired (typically 30 days)
4. Ensure all required environment variables are set

### Token Expiration

If you see errors like "Invalid access token" or "Token expired":

```bash
# Refresh tokens for the affected platform
uv run python shopee_auth.py refresh    # For Shopee
uv run python redmart_auth.py refresh   # For Redmart
uv run python lazada_auth.py refresh    # For Lazada

# Update your .env file with the new tokens
# Restart the Dagster server
```

### No Data Returned

If pipelines run but return no data:
1. Check the Dagster logs in the web UI
2. Verify your shop/store has products with status "NORMAL" (Shopee) or active
3. Check API rate limits haven't been exceeded
4. Verify the store_id/shop_id is correct

### Database Locked Errors

If you get "database is locked" errors:
1. Close any other connections to `data.duckdb`
2. Stop any running Dagster jobs
3. Try again

### Port Already in Use

If port 3000 is already in use, run Dagster on a different port:

```bash
uv run dagster dev -p 3001
```

Then access the UI at http://localhost:3001

## Initial Setup (First Time Only)

If you don't have refresh tokens yet, you'll need to go through the OAuth flow:

### Shopee OAuth Flow

```bash
# Step 1: Get authorization URL
uv run python shopee_auth.py

# Follow the instructions to authorize your app
# Add the CODE to your .env file as SHOPEE_CODE

# Step 2: Exchange code for tokens
uv run python shopee_auth.py token

# Copy the access_token and refresh_token to your .env file
```

### Redmart OAuth Flow

```bash
# Step 1: Get authorization URL
uv run python redmart_auth.py

# Follow the instructions to authorize your app

# Step 2: Exchange code for tokens
uv run python redmart_auth.py <your_code>

# Copy the tokens to your .env file
```

### Lazada OAuth Flow

Similar process - check with the Lazada Open Platform documentation.

## Advanced Usage

### Setting up Schedules

To enable automatic pipeline runs:

1. Navigate to **Automation** → **Schedules** in the Dagster UI
2. Enable the schedules you want:
   - `daily_shopee_schedule`: Runs Shopee pipeline daily at midnight
   - `daily_redmart_schedule`: Runs Redmart pipeline daily at midnight
   - `daily_lazada_schedule`: Runs Lazada pipeline daily at midnight

**Note:** When using schedules, ensure your refresh tokens are valid as the pipelines will auto-refresh access tokens during execution.

### Running in Production

For production deployments, use Dagster Cloud or deploy with:

```bash
# Run Dagster daemon for schedules/sensors
uv run dagster-daemon run

# In another terminal, run the webserver
uv run dagster-webserver -h 0.0.0.0 -p 3000
```

### Monitoring Token Expiration

The pipelines include automatic token refresh, but you should monitor:
- Access tokens expire in ~4 hours (Shopee)
- Refresh tokens expire in ~30 days
- Set up alerts when refresh tokens are about to expire

## Quick Reference

```bash
# Install dependencies
uv sync

# Refresh tokens (do this regularly)
uv run python shopee_auth.py refresh
uv run python redmart_auth.py refresh
uv run python lazada_auth.py refresh

# Start the server
uv run dagster dev

# Run a pipeline
uv run dagster asset materialize -m sellpipelines.definitions --select shopee

# View data
duckdb data.duckdb -c "SELECT * FROM sell_data.products LIMIT 10"
```

## Need Help?

- Check Dagster logs in the web UI under **Runs**
- Review individual asset logs for detailed error messages
- Verify tokens are fresh and valid
- Consult the [Dagster documentation](https://docs.dagster.io/)
- Consult the [DLT documentation](https://dlthub.com/docs/)
