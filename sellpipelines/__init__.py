"""Sell Pipelines - Dagster orchestrated dlt pipelines for e-commerce platforms.

This package provides:
- DLT sources for Shopee, Redmart, and Lazada
- Dagster assets with proper orchestration
- Asset checks for data quality
- Jobs and schedules for automation
- Field transformers for data normalization

Import sources and transformers from submodules to avoid circular imports:
    from sellpipelines.sources import shopee_source, redmart_source, lazada_source
    from sellpipelines.transformers import extract_shopee_fields, ...
"""


def __getattr__(name: str):
    """Lazy import to avoid circular dependencies."""
    if name in (
        "shopee_source",
        "redmart_source",
        "lazada_source",
    ):
        from sellpipelines import sources

        return getattr(sources, name)
    elif name in (
        "extract_shopee_fields",
        "extract_redmart_fields",
        "extract_lazada_fields",
    ):
        from sellpipelines import transformers

        return getattr(transformers, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    # Sources
    "shopee_source",
    "redmart_source",
    "lazada_source",
    # Transformers
    "extract_shopee_fields",
    "extract_redmart_fields",
    "extract_lazada_fields",
]
