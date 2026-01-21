---
name: dag-builder
description: Design DAG-based pipeline architectures with DuckDB and dlt
---

# DAG Builder

Design and implement DAG-based pipeline architectures using production orchestration tools.

## What This Skill Provides

### Core Capabilities

1. **Pipeline Architecture**
   - End-to-end workflow design
   - DAG orchestration patterns (Duckdb. dlt)
   - Component dependencies and data flow
   - Error handling and retry strategies

## Prerequisites

Before running this skill, ensure:

- `uv` is installed (check with `uv --version`)
- You have inspected and understand how to create multi-asset integrations (see this guide: https://docs.dagster.io/integrations/guides/multi-asset-integration)

## Best Practices

### Pipeline Design

- **Modularity**: Each stage should be independently testable
- **Idempotency**: Re-running stages should be safe
- **Observability**: Log metrics at every stage
- **Versioning**: Track data, code
- **Failure Handling**: Implement retry logic and alerting


## Integration Points

### Orchestration Tools

- **Dagster**: Asset-based pipeline orchestration

## Troubleshooting

### Debugging Steps

1. Check pipeline logs for each stage
2. Validate input/output data at boundaries
3. Test components in isolation
4. Review experiment tracking metrics
5. Inspect model artifacts and metadata
