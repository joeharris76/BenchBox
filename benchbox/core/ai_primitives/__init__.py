"""AI/ML Primitives benchmark for SQL-based AI functions.

This benchmark tests cross-platform SQL-based AI/ML functions that execute
within the data warehouse. It covers:

- Generative AI (text completion, question answering)
- NLP Analysis (sentiment, entity extraction, classification)
- Text Transformation (summarization, translation)
- Embedding Generation (vector embeddings)

Platform Support:
- Snowflake: Cortex functions (COMPLETE, SUMMARIZE, SENTIMENT, etc.)
- BigQuery: ML functions (ML.GENERATE_TEXT, ML.UNDERSTAND_TEXT, etc.)
- Databricks: AI functions (ai_query, ai_summarize, etc.)
- DuckDB/ClickHouse: Not supported (skip_on)

Key Differences from Vector Search benchmark:
- This benchmark tests AI function invocation performance
- Vector Search benchmark tests embedding storage and similarity queries

Safety Features:
- Cost estimation before execution
- Budget controls (--max-ai-cost flag)
- Dry-run mode for cost preview
- Rate limiting and retry logic

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from benchbox.core.ai_primitives.benchmark import AIPrimitivesBenchmark
from benchbox.core.ai_primitives.catalog import (
    AICatalog,
    AICatalogError,
    AIQuery,
    load_ai_catalog,
)
from benchbox.core.ai_primitives.cost import (
    CostEstimate,
    CostTracker,
    estimate_query_cost,
    get_platform_pricing,
)
from benchbox.core.ai_primitives.queries import AIQueryManager

__all__ = [
    # Benchmark
    "AIPrimitivesBenchmark",
    # Catalog
    "AIQuery",
    "AICatalog",
    "AICatalogError",
    "load_ai_catalog",
    # Query Manager
    "AIQueryManager",
    # Cost
    "CostEstimate",
    "CostTracker",
    "estimate_query_cost",
    "get_platform_pricing",
]
