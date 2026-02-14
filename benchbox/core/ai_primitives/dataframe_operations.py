"""DataFrame operations for AI Primitives benchmark.

This module provides DataFrame implementations of AI Primitives benchmark operations,
enabling benchmarking of ML/AI operations on DataFrame platforms using local models.

AI Primitives tests AI/ML functions across categories:
- Embedding (3 queries): Vector embedding generation using sentence-transformers
- NLP (5 queries): Sentiment analysis, classification, entity extraction
- Transform (4 queries): Summarization, translation, grammar correction - REQUIRE LLM API
- Generative (4 queries): Text completion, Q&A, SQL generation - REQUIRE LLM API

DataFrame Implementation Scope:
    **In Scope (8 queries - local model support):**
    - Sentiment analysis (nlp_sentiment_single, nlp_sentiment_batch)
    - Classification (nlp_classify_priority, nlp_classify_segment)
    - Entity extraction (nlp_entity_extraction)
    - Embedding generation (embedding_single, embedding_batch, embedding_large_dimension)

    **Out of Scope (8 queries - require LLM API):**
    - Text completion (generative_complete_simple, generative_complete_customer_profile)
    - Question answering (generative_question_answer)
    - SQL generation (generative_sql_generation)
    - Summarization (transform_summarize_short, transform_summarize_long)
    - Translation (transform_translate_comment)
    - Grammar correction (transform_grammar_fix)

Platform Support:
    - PySpark MLlib: spark-nlp or UDFs with sentence-transformers
    - Polars: Apply with sentence-transformers, TextBlob
    - Pandas: Apply with sentence-transformers, TextBlob, spaCy

Dependencies:
    - sentence-transformers: For embedding operations
    - textblob: For sentiment analysis
    - spacy: For entity extraction (optional)

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from benchbox.utils.clock import elapsed_seconds, mono_time

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class AIOperationType(Enum):
    """Types of AI operations supported by the DataFrame benchmark.

    These map to AI Primitives benchmark categories with local model support.
    """

    # Embedding operations (sentence-transformers)
    EMBEDDING_SINGLE = "embedding_single"
    EMBEDDING_BATCH = "embedding_batch"
    EMBEDDING_LARGE_DIMENSION = "embedding_large_dimension"

    # NLP operations (TextBlob, spaCy)
    SENTIMENT_SINGLE = "sentiment_single"
    SENTIMENT_BATCH = "sentiment_batch"
    CLASSIFY_PRIORITY = "classify_priority"
    CLASSIFY_SEGMENT = "classify_segment"
    ENTITY_EXTRACTION = "entity_extraction"

    # Vector similarity operations (NumPy)
    COSINE_SIMILARITY = "cosine_similarity"
    EUCLIDEAN_DISTANCE = "euclidean_distance"
    TOP_K_SIMILARITY = "top_k_similarity"


# Query IDs that require LLM API and should be skipped for DataFrame execution
SKIP_FOR_DATAFRAME = [
    # Generative queries (require LLM API)
    "generative_complete_simple",
    "generative_complete_customer_profile",
    "generative_question_answer",
    "generative_sql_generation",
    # Transform queries (require LLM API)
    "transform_summarize_short",
    "transform_summarize_long",
    "transform_translate_comment",
    "transform_grammar_fix",
]


@dataclass
class AIModelCapabilities:
    """Available ML model capabilities for AI DataFrame operations.

    Detects which ML libraries are installed and available for use.
    This enables graceful degradation when optional dependencies are missing.

    Attributes:
        has_sentence_transformers: sentence-transformers is installed
        has_textblob: textblob is installed
        has_spacy: spaCy is installed
        has_numpy: NumPy is installed (for vector operations)
        has_torch: PyTorch is installed (for sentence-transformers)
        missing_packages: List of missing packages with install commands
    """

    has_sentence_transformers: bool = False
    has_textblob: bool = False
    has_spacy: bool = False
    has_numpy: bool = False
    has_torch: bool = False
    missing_packages: list[str] = field(default_factory=list)

    @classmethod
    def detect(cls) -> AIModelCapabilities:
        """Detect available ML model capabilities.

        Returns:
            AIModelCapabilities with detected capabilities
        """
        caps = cls()
        missing = []

        # Check NumPy (required for vector operations)
        try:
            import numpy  # noqa: F401

            caps.has_numpy = True
        except ImportError:
            missing.append("numpy: pip install numpy")

        # Check PyTorch (required for sentence-transformers)
        try:
            import torch  # noqa: F401

            caps.has_torch = True
        except ImportError:
            missing.append("torch: pip install torch")

        # Check sentence-transformers
        try:
            import sentence_transformers  # noqa: F401

            caps.has_sentence_transformers = True
        except ImportError:
            missing.append("sentence-transformers: pip install sentence-transformers")

        # Check TextBlob
        try:
            import textblob  # noqa: F401

            caps.has_textblob = True
        except ImportError:
            missing.append("textblob: pip install textblob")

        # Check spaCy
        try:
            import spacy  # noqa: F401

            caps.has_spacy = True
        except ImportError:
            missing.append("spacy: pip install spacy && python -m spacy download en_core_web_sm")

        caps.missing_packages = missing
        return caps

    def can_run_embeddings(self) -> bool:
        """Check if embedding operations can run."""
        return self.has_sentence_transformers and self.has_torch

    def can_run_sentiment(self) -> bool:
        """Check if sentiment analysis can run."""
        return self.has_textblob

    def can_run_classification(self) -> bool:
        """Check if classification can run."""
        return self.has_textblob  # Uses simple rule-based classification

    def can_run_entity_extraction(self) -> bool:
        """Check if entity extraction can run."""
        return self.has_spacy or self.has_textblob  # Fallback to regex-based

    def get_missing_for_operation(self, operation: AIOperationType) -> list[str]:
        """Get missing packages for a specific operation.

        Args:
            operation: The operation to check

        Returns:
            List of missing package install commands
        """
        missing = []

        if operation in (
            AIOperationType.EMBEDDING_SINGLE,
            AIOperationType.EMBEDDING_BATCH,
            AIOperationType.EMBEDDING_LARGE_DIMENSION,
        ):
            if not self.has_sentence_transformers:
                missing.append("sentence-transformers: pip install sentence-transformers")
            if not self.has_torch:
                missing.append("torch: pip install torch")

        elif operation in (AIOperationType.SENTIMENT_SINGLE, AIOperationType.SENTIMENT_BATCH) or operation in (
            AIOperationType.CLASSIFY_PRIORITY,
            AIOperationType.CLASSIFY_SEGMENT,
        ):
            if not self.has_textblob:
                missing.append("textblob: pip install textblob")

        elif operation == AIOperationType.ENTITY_EXTRACTION:
            if not self.has_spacy and not self.has_textblob:
                missing.append("spacy: pip install spacy && python -m spacy download en_core_web_sm")

        elif operation in (
            AIOperationType.COSINE_SIMILARITY,
            AIOperationType.EUCLIDEAN_DISTANCE,
            AIOperationType.TOP_K_SIMILARITY,
        ):
            if not self.has_numpy:
                missing.append("numpy: pip install numpy")

        return missing


@dataclass
class DataFrameAICapabilities:
    """Platform capabilities for DataFrame AI operations.

    Declares what AI operations a DataFrame platform supports based on
    available ML libraries and platform features.

    Attributes:
        platform_name: Name of the platform
        model_caps: Underlying model capabilities
        supports_udf: Platform supports user-defined functions
        supports_batch_inference: Platform can batch ML inference efficiently
        notes: Platform-specific notes
    """

    platform_name: str
    model_caps: AIModelCapabilities = field(default_factory=AIModelCapabilities.detect)
    supports_udf: bool = True
    supports_batch_inference: bool = True
    notes: str = ""

    def supports_operation(self, operation: AIOperationType) -> bool:
        """Check if an operation type is supported.

        Args:
            operation: The operation type to check

        Returns:
            True if the operation is supported
        """
        if operation in (
            AIOperationType.EMBEDDING_SINGLE,
            AIOperationType.EMBEDDING_BATCH,
            AIOperationType.EMBEDDING_LARGE_DIMENSION,
        ):
            return self.model_caps.can_run_embeddings()

        if operation in (AIOperationType.SENTIMENT_SINGLE, AIOperationType.SENTIMENT_BATCH):
            return self.model_caps.can_run_sentiment()

        if operation in (AIOperationType.CLASSIFY_PRIORITY, AIOperationType.CLASSIFY_SEGMENT):
            return self.model_caps.can_run_classification()

        if operation == AIOperationType.ENTITY_EXTRACTION:
            return self.model_caps.can_run_entity_extraction()

        if operation in (
            AIOperationType.COSINE_SIMILARITY,
            AIOperationType.EUCLIDEAN_DISTANCE,
            AIOperationType.TOP_K_SIMILARITY,
        ):
            return self.model_caps.has_numpy

        return False

    def get_unsupported_operations(self) -> list[AIOperationType]:
        """Get list of operations not supported by this platform.

        Returns:
            List of unsupported AIOperationType values
        """
        return [op for op in AIOperationType if not self.supports_operation(op)]


@dataclass
class DataFrameAIResult:
    """Result of a DataFrame AI operation.

    Attributes:
        operation_type: Type of AI operation
        success: Whether the operation completed successfully
        start_time: Operation start timestamp (Unix time)
        end_time: Operation end timestamp (Unix time)
        duration_ms: Operation duration in milliseconds
        rows_processed: Number of rows processed
        model_name: Name of the model used
        model_load_time_ms: Time to load the model (if applicable)
        inference_time_ms: Time for inference (excluding model load)
        error_message: Error description if operation failed
        metrics: Additional operation-specific metrics
    """

    operation_type: AIOperationType
    success: bool
    start_time: float
    end_time: float
    duration_ms: float
    rows_processed: int
    model_name: str | None = None
    model_load_time_ms: float | None = None
    inference_time_ms: float | None = None
    error_message: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def failure(
        cls,
        operation_type: AIOperationType,
        error_message: str,
        start_time: float | None = None,
    ) -> DataFrameAIResult:
        """Create a failure result.

        Args:
            operation_type: The operation that failed
            error_message: Description of the failure
            start_time: Optional start time (defaults to now)

        Returns:
            DataFrameAIResult indicating failure
        """
        now = time.time()
        return cls(
            operation_type=operation_type,
            success=False,
            start_time=start_time or now,
            end_time=now,
            duration_ms=0.0 if start_time is None else (now - start_time) * 1000,
            rows_processed=0,
            error_message=error_message,
        )


# =============================================================================
# Model Loading and Caching
# =============================================================================


@lru_cache(maxsize=4)
def _get_sentence_transformer_model(model_name: str) -> Any:
    """Get a cached sentence-transformers model.

    Args:
        model_name: Name of the model to load

    Returns:
        SentenceTransformer model instance
    """
    from sentence_transformers import SentenceTransformer

    logger.info(f"Loading sentence-transformers model: {model_name}")
    return SentenceTransformer(model_name)


@lru_cache(maxsize=1)
def _get_spacy_model(model_name: str = "en_core_web_sm") -> Any:
    """Get a cached spaCy model.

    Args:
        model_name: Name of the model to load

    Returns:
        spaCy model instance
    """
    import spacy

    logger.info(f"Loading spaCy model: {model_name}")
    return spacy.load(model_name)


# =============================================================================
# Embedding Operations
# =============================================================================


def generate_embedding(
    text: str,
    model_name: str = "all-MiniLM-L6-v2",
) -> list[float]:
    """Generate embedding for a single text using sentence-transformers.

    Args:
        text: Text to embed
        model_name: sentence-transformers model name

    Returns:
        Embedding as list of floats
    """
    model = _get_sentence_transformer_model(model_name)
    embedding = model.encode(text, convert_to_numpy=True)
    return embedding.tolist()


def generate_embeddings_batch(
    texts: list[str],
    model_name: str = "all-MiniLM-L6-v2",
    batch_size: int = 32,
) -> list[list[float]]:
    """Generate embeddings for a batch of texts.

    Args:
        texts: List of texts to embed
        model_name: sentence-transformers model name
        batch_size: Batch size for encoding

    Returns:
        List of embeddings as lists of floats
    """
    model = _get_sentence_transformer_model(model_name)
    embeddings = model.encode(texts, convert_to_numpy=True, batch_size=batch_size)
    return [emb.tolist() for emb in embeddings]


# =============================================================================
# Sentiment Operations
# =============================================================================


def analyze_sentiment(text: str) -> float:
    """Analyze sentiment of text using TextBlob.

    Args:
        text: Text to analyze

    Returns:
        Sentiment polarity score (-1 to 1)
    """
    from textblob import TextBlob

    blob = TextBlob(str(text) if text else "")
    return blob.sentiment.polarity


def analyze_sentiment_batch(texts: list[str]) -> list[float]:
    """Analyze sentiment for a batch of texts.

    Args:
        texts: List of texts to analyze

    Returns:
        List of sentiment polarity scores
    """
    return [analyze_sentiment(text) for text in texts]


# =============================================================================
# Classification Operations
# =============================================================================


def classify_priority(text: str, labels: list[str] | None = None) -> str:
    """Classify text into priority categories using rule-based approach.

    For production use, this would use a trained classifier or zero-shot
    classification with transformers. This implementation uses simple
    keyword matching for benchmark purposes.

    Args:
        text: Text to classify
        labels: Optional list of labels (default: urgent, normal, low priority)

    Returns:
        Predicted priority label
    """
    if labels is None:
        labels = ["urgent", "normal", "low priority"]

    text_lower = str(text).lower() if text else ""

    # Simple keyword-based classification
    urgent_keywords = ["urgent", "asap", "immediately", "critical", "emergency", "rush"]
    low_keywords = ["later", "whenever", "no rush", "low priority", "optional"]

    for keyword in urgent_keywords:
        if keyword in text_lower:
            return labels[0]  # urgent

    for keyword in low_keywords:
        if keyword in text_lower:
            return labels[2]  # low priority

    return labels[1]  # normal


def classify_segment(text: str, segments: list[str] | None = None) -> str:
    """Classify text into market segment using rule-based approach.

    For production use, this would use a trained classifier. This implementation
    uses simple keyword matching for benchmark purposes.

    Args:
        text: Text to classify
        segments: Optional list of segments (default: TPC-H segments)

    Returns:
        Predicted segment label
    """
    if segments is None:
        segments = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]

    text_lower = str(text).lower() if text else ""

    # Simple keyword-based classification
    segment_keywords = {
        "AUTOMOBILE": ["car", "vehicle", "auto", "motor", "drive", "engine"],
        "BUILDING": ["construct", "build", "house", "structure", "architect"],
        "FURNITURE": ["chair", "table", "desk", "bed", "sofa", "wood"],
        "HOUSEHOLD": ["home", "kitchen", "clean", "domestic", "family"],
        "MACHINERY": ["machine", "equipment", "industrial", "factory", "tool"],
    }

    for segment, keywords in segment_keywords.items():
        if segment in segments:
            for keyword in keywords:
                if keyword in text_lower:
                    return segment

    # Default to first segment if no match
    return segments[0]


# =============================================================================
# Entity Extraction Operations
# =============================================================================


def extract_entities(text: str, entity_types: list[str] | None = None) -> dict[str, list[str]]:
    """Extract entities from text using spaCy or regex fallback.

    Args:
        text: Text to extract entities from
        entity_types: Optional list of entity types to extract

    Returns:
        Dictionary mapping entity type to list of extracted entities
    """
    if entity_types is None:
        entity_types = ["feature", "material", "color", "size"]

    result: dict[str, list[str]] = {etype: [] for etype in entity_types}

    text_str = str(text) if text else ""
    if not text_str:
        return result

    # Try spaCy first
    try:
        nlp = _get_spacy_model()
        doc = nlp(text_str)

        for ent in doc.ents:
            # Map spaCy entity types to our types
            if ent.label_ in ("PRODUCT", "ORG") and "feature" in entity_types:
                result["feature"].append(ent.text)
            elif ent.label_ == "MATERIAL" and "material" in entity_types:
                result["material"].append(ent.text)

        # Use adjectives for colors and sizes
        for token in doc:
            if token.pos_ == "ADJ":
                text_lower = token.text.lower()
                if any(c in text_lower for c in ["red", "blue", "green", "black", "white", "yellow"]):
                    if "color" in entity_types:
                        result["color"].append(token.text)
                elif any(s in text_lower for s in ["small", "medium", "large", "tiny", "huge"]):
                    if "size" in entity_types:
                        result["size"].append(token.text)

    except Exception as e:
        # Fallback to simple regex-based extraction
        logger.debug(f"spaCy extraction failed, using regex fallback: {e}")
        import re

        # Color extraction
        if "color" in entity_types:
            colors = re.findall(r"\b(red|blue|green|black|white|yellow|brown|gray|grey)\b", text_str, re.IGNORECASE)
            result["color"] = list(set(colors))

        # Size extraction
        if "size" in entity_types:
            sizes = re.findall(r"\b(small|medium|large|tiny|huge|xl|xxl)\b", text_str, re.IGNORECASE)
            result["size"] = list(set(sizes))

        # Material extraction
        if "material" in entity_types:
            materials = re.findall(
                r"\b(steel|iron|copper|brass|aluminum|plastic|wood|leather|cotton)\b", text_str, re.IGNORECASE
            )
            result["material"] = list(set(materials))

    return result


# =============================================================================
# Vector Similarity Operations
# =============================================================================


def cosine_similarity(vec1: list[float], vec2: list[float]) -> float:
    """Compute cosine similarity between two vectors.

    Args:
        vec1: First vector
        vec2: Second vector

    Returns:
        Cosine similarity score (-1 to 1, where 1 is identical, 0 is orthogonal, -1 is opposite)
    """
    import numpy as np

    v1 = np.array(vec1)
    v2 = np.array(vec2)

    dot_product = np.dot(v1, v2)
    norm1 = np.linalg.norm(v1)
    norm2 = np.linalg.norm(v2)

    if norm1 == 0 or norm2 == 0:
        return 0.0

    return float(dot_product / (norm1 * norm2))


def euclidean_distance(vec1: list[float], vec2: list[float]) -> float:
    """Compute Euclidean distance between two vectors.

    Args:
        vec1: First vector
        vec2: Second vector

    Returns:
        Euclidean distance
    """
    import numpy as np

    v1 = np.array(vec1)
    v2 = np.array(vec2)

    return float(np.linalg.norm(v1 - v2))


def top_k_similarity(
    query_vec: list[float],
    vectors: list[list[float]],
    k: int = 10,
) -> list[tuple[int, float]]:
    """Find top-K most similar vectors using cosine similarity.

    Args:
        query_vec: Query vector
        vectors: List of vectors to search
        k: Number of results to return

    Returns:
        List of (index, similarity) tuples, sorted by similarity descending
    """
    import numpy as np

    query = np.array(query_vec)
    query_norm = np.linalg.norm(query)

    if query_norm == 0:
        return []

    similarities = []
    for i, vec in enumerate(vectors):
        v = np.array(vec)
        v_norm = np.linalg.norm(v)
        if v_norm > 0:
            sim = float(np.dot(query, v) / (query_norm * v_norm))
            similarities.append((i, sim))

    # Sort by similarity descending and take top-k
    similarities.sort(key=lambda x: x[1], reverse=True)
    return similarities[:k]


# =============================================================================
# DataFrame Operations Manager
# =============================================================================


class DataFrameAIOperationsManager:
    """Manager for DataFrame AI operations.

    Provides AI operation execution for DataFrame platforms, with capability
    detection and graceful error handling for missing dependencies.

    Example:
        manager = DataFrameAIOperationsManager("polars-df")

        # Check capabilities before running
        if manager.supports_operation(AIOperationType.EMBEDDING_SINGLE):
            result = manager.execute_embedding_single(texts)

        # Get helpful error for missing dependencies
        if not manager.supports_operation(AIOperationType.EMBEDDING_BATCH):
            print(manager.get_unsupported_message(AIOperationType.EMBEDDING_BATCH))
    """

    def __init__(self, platform_name: str) -> None:
        """Initialize the AI operations manager.

        Args:
            platform_name: Platform name (e.g., "polars-df", "pandas-df", "pyspark-df")
        """
        self.platform_name = platform_name.lower()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # Detect available capabilities
        self._model_caps = AIModelCapabilities.detect()
        self._capabilities = self._build_capabilities()

    def _build_capabilities(self) -> DataFrameAICapabilities:
        """Build platform capabilities.

        Returns:
            DataFrameAICapabilities for this platform
        """
        notes = []

        if "polars" in self.platform_name:
            notes.append("Uses map_elements for row-level operations.")
        elif "pandas" in self.platform_name:
            notes.append("Uses apply for row-level operations.")
        elif "pyspark" in self.platform_name:
            notes.append("Uses Pandas UDFs for distributed inference.")

        if not self._model_caps.can_run_embeddings():
            notes.append("Embedding operations unavailable - install sentence-transformers.")
        if not self._model_caps.can_run_sentiment():
            notes.append("Sentiment analysis unavailable - install textblob.")
        if not self._model_caps.can_run_entity_extraction():
            notes.append("Entity extraction unavailable - install spacy.")

        return DataFrameAICapabilities(
            platform_name=self.platform_name,
            model_caps=self._model_caps,
            supports_udf=True,
            supports_batch_inference="pyspark" not in self.platform_name,  # PySpark needs batching
            notes=" ".join(notes),
        )

    def get_capabilities(self) -> DataFrameAICapabilities:
        """Get platform AI capabilities.

        Returns:
            DataFrameAICapabilities for this platform
        """
        return self._capabilities

    def supports_operation(self, operation: AIOperationType) -> bool:
        """Check if an operation type is supported.

        Args:
            operation: The operation to check

        Returns:
            True if supported
        """
        return self._capabilities.supports_operation(operation)

    def get_unsupported_message(self, operation: AIOperationType) -> str:
        """Get error message for unsupported operation.

        Args:
            operation: The unsupported operation

        Returns:
            Helpful error message with install instructions
        """
        missing = self._model_caps.get_missing_for_operation(operation)

        if not missing:
            return f"Operation {operation.value} is not supported on {self.platform_name}."

        msg = [
            f"Operation {operation.value} requires missing dependencies.",
            "",
            "Install missing packages:",
        ]
        for pkg in missing:
            msg.append(f"  {pkg}")

        return "\n".join(msg)

    def execute_embedding_single(
        self,
        texts: list[str],
        model_name: str = "all-MiniLM-L6-v2",
    ) -> DataFrameAIResult:
        """Execute single embedding generation.

        Args:
            texts: List of texts to embed (one at a time)
            model_name: sentence-transformers model name

        Returns:
            DataFrameAIResult with operation outcome
        """
        start_time = time.time()
        operation = AIOperationType.EMBEDDING_SINGLE

        if not self.supports_operation(operation):
            return DataFrameAIResult.failure(
                operation,
                self.get_unsupported_message(operation),
                start_time,
            )

        try:
            # Load model (cached)
            model_load_start = mono_time()
            _ = _get_sentence_transformer_model(model_name)
            model_load_time = (elapsed_seconds(model_load_start)) * 1000

            # Generate embeddings
            inference_start = mono_time()
            embeddings = [generate_embedding(text, model_name) for text in texts]
            inference_time = (elapsed_seconds(inference_start)) * 1000

            end_time = time.time()
            return DataFrameAIResult(
                operation_type=operation,
                success=True,
                start_time=start_time,
                end_time=end_time,
                duration_ms=(end_time - start_time) * 1000,
                rows_processed=len(texts),
                model_name=model_name,
                model_load_time_ms=model_load_time,
                inference_time_ms=inference_time,
                metrics={
                    "embedding_dimension": len(embeddings[0]) if embeddings else 0,
                    "embeddings": embeddings,
                },
            )

        except Exception as e:
            self.logger.error(f"Embedding single failed: {e}")
            return DataFrameAIResult.failure(operation, str(e), start_time)

    def execute_embedding_batch(
        self,
        texts: list[str],
        model_name: str = "all-MiniLM-L6-v2",
        batch_size: int = 32,
    ) -> DataFrameAIResult:
        """Execute batch embedding generation.

        Args:
            texts: List of texts to embed
            model_name: sentence-transformers model name
            batch_size: Batch size for encoding

        Returns:
            DataFrameAIResult with operation outcome
        """
        start_time = time.time()
        operation = AIOperationType.EMBEDDING_BATCH

        if not self.supports_operation(operation):
            return DataFrameAIResult.failure(
                operation,
                self.get_unsupported_message(operation),
                start_time,
            )

        try:
            model_load_start = mono_time()
            _ = _get_sentence_transformer_model(model_name)
            model_load_time = (elapsed_seconds(model_load_start)) * 1000

            inference_start = mono_time()
            embeddings = generate_embeddings_batch(texts, model_name, batch_size)
            inference_time = (elapsed_seconds(inference_start)) * 1000

            end_time = time.time()
            return DataFrameAIResult(
                operation_type=operation,
                success=True,
                start_time=start_time,
                end_time=end_time,
                duration_ms=(end_time - start_time) * 1000,
                rows_processed=len(texts),
                model_name=model_name,
                model_load_time_ms=model_load_time,
                inference_time_ms=inference_time,
                metrics={
                    "embedding_dimension": len(embeddings[0]) if embeddings else 0,
                    "batch_size": batch_size,
                    "embeddings": embeddings,
                },
            )

        except Exception as e:
            self.logger.error(f"Embedding batch failed: {e}")
            return DataFrameAIResult.failure(operation, str(e), start_time)

    def execute_sentiment_single(self, texts: list[str]) -> DataFrameAIResult:
        """Execute single sentiment analysis.

        Args:
            texts: List of texts to analyze (one at a time)

        Returns:
            DataFrameAIResult with operation outcome
        """
        start_time = time.time()
        operation = AIOperationType.SENTIMENT_SINGLE

        if not self.supports_operation(operation):
            return DataFrameAIResult.failure(
                operation,
                self.get_unsupported_message(operation),
                start_time,
            )

        try:
            inference_start = mono_time()
            scores = [analyze_sentiment(text) for text in texts]
            inference_time = (elapsed_seconds(inference_start)) * 1000

            end_time = time.time()
            return DataFrameAIResult(
                operation_type=operation,
                success=True,
                start_time=start_time,
                end_time=end_time,
                duration_ms=(end_time - start_time) * 1000,
                rows_processed=len(texts),
                model_name="textblob",
                inference_time_ms=inference_time,
                metrics={"sentiment_scores": scores},
            )

        except Exception as e:
            self.logger.error(f"Sentiment single failed: {e}")
            return DataFrameAIResult.failure(operation, str(e), start_time)

    def execute_sentiment_batch(self, texts: list[str]) -> DataFrameAIResult:
        """Execute batch sentiment analysis.

        Args:
            texts: List of texts to analyze

        Returns:
            DataFrameAIResult with operation outcome
        """
        start_time = time.time()
        operation = AIOperationType.SENTIMENT_BATCH

        if not self.supports_operation(operation):
            return DataFrameAIResult.failure(
                operation,
                self.get_unsupported_message(operation),
                start_time,
            )

        try:
            inference_start = mono_time()
            scores = analyze_sentiment_batch(texts)
            inference_time = (elapsed_seconds(inference_start)) * 1000

            end_time = time.time()
            return DataFrameAIResult(
                operation_type=operation,
                success=True,
                start_time=start_time,
                end_time=end_time,
                duration_ms=(end_time - start_time) * 1000,
                rows_processed=len(texts),
                model_name="textblob",
                inference_time_ms=inference_time,
                metrics={"sentiment_scores": scores},
            )

        except Exception as e:
            self.logger.error(f"Sentiment batch failed: {e}")
            return DataFrameAIResult.failure(operation, str(e), start_time)

    def execute_classification(
        self,
        texts: list[str],
        labels: list[str],
        operation: AIOperationType,
    ) -> DataFrameAIResult:
        """Execute text classification.

        Args:
            texts: List of texts to classify
            labels: List of possible labels
            operation: Classification operation type

        Returns:
            DataFrameAIResult with operation outcome
        """
        start_time = time.time()

        if not self.supports_operation(operation):
            return DataFrameAIResult.failure(
                operation,
                self.get_unsupported_message(operation),
                start_time,
            )

        try:
            inference_start = mono_time()

            if operation == AIOperationType.CLASSIFY_PRIORITY:
                predictions = [classify_priority(text, labels) for text in texts]
            else:  # CLASSIFY_SEGMENT
                predictions = [classify_segment(text, labels) for text in texts]

            inference_time = (elapsed_seconds(inference_start)) * 1000

            end_time = time.time()
            return DataFrameAIResult(
                operation_type=operation,
                success=True,
                start_time=start_time,
                end_time=end_time,
                duration_ms=(end_time - start_time) * 1000,
                rows_processed=len(texts),
                model_name="rule-based",
                inference_time_ms=inference_time,
                metrics={
                    "predictions": predictions,
                    "labels": labels,
                },
            )

        except Exception as e:
            self.logger.error(f"Classification failed: {e}")
            return DataFrameAIResult.failure(operation, str(e), start_time)

    def execute_entity_extraction(
        self,
        texts: list[str],
        entity_types: list[str] | None = None,
    ) -> DataFrameAIResult:
        """Execute entity extraction.

        Args:
            texts: List of texts to extract entities from
            entity_types: Optional list of entity types to extract

        Returns:
            DataFrameAIResult with operation outcome
        """
        start_time = time.time()
        operation = AIOperationType.ENTITY_EXTRACTION

        if not self.supports_operation(operation):
            return DataFrameAIResult.failure(
                operation,
                self.get_unsupported_message(operation),
                start_time,
            )

        try:
            inference_start = mono_time()
            extractions = [extract_entities(text, entity_types) for text in texts]
            inference_time = (elapsed_seconds(inference_start)) * 1000

            end_time = time.time()
            return DataFrameAIResult(
                operation_type=operation,
                success=True,
                start_time=start_time,
                end_time=end_time,
                duration_ms=(end_time - start_time) * 1000,
                rows_processed=len(texts),
                model_name="spacy" if self._model_caps.has_spacy else "regex",
                inference_time_ms=inference_time,
                metrics={
                    "extractions": extractions,
                    "entity_types": entity_types or ["feature", "material", "color", "size"],
                },
            )

        except Exception as e:
            self.logger.error(f"Entity extraction failed: {e}")
            return DataFrameAIResult.failure(operation, str(e), start_time)


# =============================================================================
# Helper Functions
# =============================================================================


def get_dataframe_ai_manager(platform_name: str) -> DataFrameAIOperationsManager | None:
    """Get a DataFrame AI operations manager for a platform.

    Args:
        platform_name: Platform name (e.g., "polars-df", "pandas-df", "pyspark-df")

    Returns:
        DataFrameAIOperationsManager if platform is a DataFrame platform,
        None otherwise.
    """
    platform_lower = platform_name.lower()

    # Check if this is a DataFrame platform
    df_platforms = ("polars-df", "polars", "pandas-df", "pandas", "pyspark-df", "pyspark", "datafusion-df")
    if not any(p in platform_lower for p in df_platforms):
        logger.debug(f"Platform {platform_name} is not a DataFrame platform")
        return None

    try:
        return DataFrameAIOperationsManager(platform_name)
    except Exception as e:
        logger.warning(f"Failed to create AI operations manager for {platform_name}: {e}")
        return None


def get_skip_for_dataframe() -> list[str]:
    """Get query IDs that should be skipped for DataFrame execution.

    These are generative/transform queries that require LLM API integration
    and are not supported for local DataFrame execution.

    Returns:
        List of query IDs to skip
    """
    return SKIP_FOR_DATAFRAME.copy()


def validate_ai_primitives_dataframe_platform(platform_name: str) -> tuple[bool, str]:
    """Validate that a platform can run AI Primitives DataFrame benchmark.

    Checks if the platform is a DataFrame platform and has required dependencies
    for at least some AI operations.

    Args:
        platform_name: Platform name to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    platform_lower = platform_name.lower()

    # Check if this is a DataFrame platform
    df_platforms = ("polars-df", "polars", "pandas-df", "pandas", "pyspark-df", "pyspark", "datafusion-df")
    if not any(p in platform_lower for p in df_platforms):
        return False, (
            f"AI Primitives DataFrame benchmark requires a DataFrame platform.\n"
            f"Platform '{platform_name}' is not a DataFrame platform.\n"
            f"\n"
            f"Supported platforms:\n"
            f"  - polars-df\n"
            f"  - pandas-df\n"
            f"  - pyspark-df\n"
            f"\n"
            f"Example:\n"
            f"  benchbox run --platform polars-df --benchmark ai_primitives --mode dataframe\n"
        )

    # Check for available ML libraries
    caps = AIModelCapabilities.detect()

    if not caps.can_run_embeddings() and not caps.can_run_sentiment():
        return False, (
            "AI Primitives DataFrame benchmark requires ML dependencies.\n"
            "No supported ML libraries are installed.\n"
            "\n"
            "Install at least one of:\n"
            "  - For embeddings: pip install sentence-transformers torch\n"
            "  - For sentiment: pip install textblob\n"
            "  - For entities: pip install spacy && python -m spacy download en_core_web_sm\n"
        )

    return True, ""


__all__ = [
    "AIOperationType",
    "AIModelCapabilities",
    "DataFrameAICapabilities",
    "DataFrameAIResult",
    "DataFrameAIOperationsManager",
    "SKIP_FOR_DATAFRAME",
    "get_dataframe_ai_manager",
    "get_skip_for_dataframe",
    "validate_ai_primitives_dataframe_platform",
    # Individual operations
    "generate_embedding",
    "generate_embeddings_batch",
    "analyze_sentiment",
    "analyze_sentiment_batch",
    "classify_priority",
    "classify_segment",
    "extract_entities",
    "cosine_similarity",
    "euclidean_distance",
    "top_k_similarity",
]
