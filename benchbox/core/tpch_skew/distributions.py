"""Skew distribution implementations for TPC-H Skew benchmark.

Provides various probability distributions for introducing data skew:
- Zipfian: Power-law distribution common in real-world data
- Normal: Gaussian distribution for bell-curve patterns
- Exponential: Decay-based distribution for time-based patterns
- Uniform: Standard uniform distribution (baseline)

Based on the research: "Introducing Skew into the TPC-H Benchmark"
Reference: https://www.tpc.org/tpctc/tpctc2011/slides_and_papers/introducing_skew_into_the_tpc_h_benchmark.pdf

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from abc import ABC, abstractmethod
from typing import Any

import numpy as np


class SkewDistribution(ABC):
    """Abstract base class for skew distributions."""

    @abstractmethod
    def sample(self, size: int, rng: np.random.Generator) -> np.ndarray:
        """Generate samples from the distribution.

        Args:
            size: Number of samples to generate
            rng: NumPy random generator for reproducibility

        Returns:
            Array of samples in range [0, 1]
        """

    @abstractmethod
    def get_skew_factor(self) -> float:
        """Get the effective skew factor of this distribution.

        Returns:
            Float in range [0, 1] where 0 = uniform, 1 = maximum skew
        """

    @abstractmethod
    def get_description(self) -> str:
        """Get a human-readable description of the distribution.

        Returns:
            Description string
        """

    def map_to_range(self, samples: np.ndarray, min_val: int, max_val: int) -> np.ndarray:
        """Map [0, 1] samples to integer range [min_val, max_val].

        Args:
            samples: Array of samples in [0, 1]
            min_val: Minimum output value (inclusive)
            max_val: Maximum output value (inclusive)

        Returns:
            Array of integers in specified range
        """
        return np.floor(samples * (max_val - min_val + 1) + min_val).astype(np.int64)


class ZipfianDistribution(SkewDistribution):
    """Zipfian (power-law) distribution.

    Models "popularity" where a small number of values appear very frequently.
    Common in customer activity, product sales, web page visits.

    The parameter `s` (Zipf exponent) controls skewness:
    - s = 0: Uniform distribution
    - s = 1: Standard Zipf distribution (Zipf's law)
    - s > 1: Increasing concentration on top values
    """

    def __init__(self, s: float = 1.0, num_elements: int = 1000):
        """Initialize Zipfian distribution.

        Args:
            s: Zipf exponent (skewness parameter). Default 1.0 for Zipf's law.
            num_elements: Number of distinct elements in the domain
        """
        if s < 0:
            raise ValueError(f"Zipf exponent must be non-negative, got {s}")
        if num_elements < 1:
            raise ValueError(f"num_elements must be positive, got {num_elements}")

        self.s = s
        self.num_elements = num_elements

        # Pre-compute normalization constant (generalized harmonic number)
        self._harmonic = self._compute_harmonic()

    def _compute_harmonic(self) -> float:
        """Compute generalized harmonic number H_{N,s}."""
        ranks = np.arange(1, self.num_elements + 1, dtype=np.float64)
        return np.sum(1.0 / np.power(ranks, self.s))

    def sample(self, size: int, rng: np.random.Generator) -> np.ndarray:
        """Generate Zipfian samples using inverse transform sampling.

        Args:
            size: Number of samples to generate
            rng: NumPy random generator

        Returns:
            Array of values in [0, 1] representing rank fractions
        """
        if self.s == 0:
            # Uniform case
            return rng.random(size)

        # Use inverse transform sampling for Zipf distribution
        u = rng.random(size)
        ranks = np.zeros(size, dtype=np.float64)

        # Compute CDF values for each rank
        cumsum = 0.0
        cdf = np.zeros(self.num_elements)
        for k in range(1, self.num_elements + 1):
            cumsum += 1.0 / (k**self.s * self._harmonic)
            cdf[k - 1] = cumsum

        # Inverse transform: find rank for each uniform sample
        for i in range(size):
            # Binary search for the rank
            rank = np.searchsorted(cdf, u[i])
            ranks[i] = rank / self.num_elements

        return ranks

    def get_skew_factor(self) -> float:
        """Get effective skew factor."""
        # Normalize s to [0, 1] range (s=2 is considered maximum practical skew)
        return min(self.s / 2.0, 1.0)

    def get_description(self) -> str:
        """Get distribution description."""
        return f"Zipfian(s={self.s}, N={self.num_elements})"


class NormalDistribution(SkewDistribution):
    """Truncated normal distribution for bell-curve patterns.

    Useful for modeling values that cluster around a mean,
    like product prices, customer ages, or geographic regions.
    """

    def __init__(self, mean: float = 0.5, std: float = 0.15):
        """Initialize normal distribution.

        Args:
            mean: Mean of the distribution (in [0, 1])
            std: Standard deviation (controls spread)
        """
        if not 0 <= mean <= 1:
            raise ValueError(f"mean must be in [0, 1], got {mean}")
        if std <= 0:
            raise ValueError(f"std must be positive, got {std}")

        self.mean = mean
        self.std = std

    def sample(self, size: int, rng: np.random.Generator) -> np.ndarray:
        """Generate truncated normal samples.

        Args:
            size: Number of samples to generate
            rng: NumPy random generator

        Returns:
            Array of values in [0, 1]
        """
        samples = rng.normal(self.mean, self.std, size)
        # Truncate to [0, 1]
        return np.clip(samples, 0, 1)

    def get_skew_factor(self) -> float:
        """Get effective skew factor based on std."""
        # Lower std = more concentration = higher skew
        # std=0.5 is effectively uniform, std=0.05 is highly concentrated
        return max(0, 1 - self.std / 0.5)

    def get_description(self) -> str:
        """Get distribution description."""
        return f"Normal(μ={self.mean}, σ={self.std})"


class ExponentialDistribution(SkewDistribution):
    """Exponential distribution for decay-based patterns.

    Useful for modeling recency effects:
    - Recent orders more common than old orders
    - Popular products have declining interest
    - Customer engagement decay
    """

    def __init__(self, rate: float = 3.0):
        """Initialize exponential distribution.

        Args:
            rate: Rate parameter (lambda). Higher = faster decay = more skew
        """
        if rate <= 0:
            raise ValueError(f"rate must be positive, got {rate}")

        self.rate = rate

    def sample(self, size: int, rng: np.random.Generator) -> np.ndarray:
        """Generate exponential samples truncated to [0, 1].

        Args:
            size: Number of samples to generate
            rng: NumPy random generator

        Returns:
            Array of values in [0, 1]
        """
        # Generate exponential samples
        samples = rng.exponential(1.0 / self.rate, size)
        # Normalize to [0, 1] using CDF transformation
        # CDF of exponential: 1 - exp(-rate * x)
        # We want concentration at low values, so use 1-exp(-rate*x) directly
        # But we need to map raw samples to [0,1] range
        # Use: x / (x + 1) transformation for [0, inf) -> [0, 1)
        normalized = samples / (samples + 1)
        return normalized

    def get_skew_factor(self) -> float:
        """Get effective skew factor based on rate."""
        # rate=0.5 is low skew, rate=5 is high skew
        return min(self.rate / 5.0, 1.0)

    def get_description(self) -> str:
        """Get distribution description."""
        return f"Exponential(λ={self.rate})"


class UniformDistribution(SkewDistribution):
    """Uniform distribution (baseline with no skew).

    Provides uniform random distribution as a baseline
    for comparison with skewed distributions.
    """

    def sample(self, size: int, rng: np.random.Generator) -> np.ndarray:
        """Generate uniform samples.

        Args:
            size: Number of samples to generate
            rng: NumPy random generator

        Returns:
            Array of uniformly distributed values in [0, 1]
        """
        return rng.random(size)

    def get_skew_factor(self) -> float:
        """Get effective skew factor (always 0 for uniform)."""
        return 0.0

    def get_description(self) -> str:
        """Get distribution description."""
        return "Uniform"


def create_distribution(dist_type: str, **params: Any) -> SkewDistribution:
    """Factory function to create distribution by name.

    Args:
        dist_type: Distribution type name ("zipfian", "normal", "exponential", "uniform")
        **params: Distribution-specific parameters

    Returns:
        SkewDistribution instance

    Raises:
        ValueError: If distribution type is unknown
    """
    dist_type = dist_type.lower()

    if dist_type == "zipfian":
        return ZipfianDistribution(**params)
    elif dist_type == "normal":
        return NormalDistribution(**params)
    elif dist_type == "exponential":
        return ExponentialDistribution(**params)
    elif dist_type == "uniform":
        return UniformDistribution()
    else:
        raise ValueError(f"Unknown distribution type: {dist_type}. Valid types: zipfian, normal, exponential, uniform")
