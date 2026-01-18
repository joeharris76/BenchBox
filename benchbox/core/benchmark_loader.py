"""Benchmark loading functionality.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import importlib
from typing import Any

from benchbox.core.config import BenchmarkConfig, SystemProfile


def get_benchmark_instance(config: BenchmarkConfig, system_profile: SystemProfile | None) -> Any:
    """Get benchmark instance based on configuration."""
    benchmark_class = get_benchmark_class(config.name)

    cpu_cores = 1
    if system_profile:
        cpu_cores = getattr(system_profile, "cpu_cores_logical", 1)

    benchmark_kwargs = {
        "scale_factor": config.scale_factor,
        "compress_data": config.compress_data,
        "compression_type": config.compression_type,
        "compression_level": config.compression_level,
    }

    try:
        options = getattr(config, "options", {}) or {}
        force_regenerate = bool(options.get("force_regenerate"))
        benchmark_id = config.name.lower()
        if benchmark_id in ("tpcds", "joinorder"):
            return benchmark_class(
                parallel=cpu_cores,
                force_regenerate=force_regenerate,
                **benchmark_kwargs,
            )
        else:
            return benchmark_class(parallel=cpu_cores, **benchmark_kwargs)
    except TypeError:
        return benchmark_class(**benchmark_kwargs)


def get_benchmark_class(benchmark_name: str) -> Any:
    """Dynamically load benchmark class using importlib."""
    benchmark_name = benchmark_name.lower()
    module_name = f"benchbox.core.{benchmark_name}.benchmark"

    class_name_map = {
        "tpch": "TPCHBenchmark",
        "tpcds": "TPCDSBenchmark",
        "ssb": "SSBBenchmark",
        "read_primitives": "ReadPrimitivesBenchmark",
        "metadata_primitives": "MetadataPrimitivesBenchmark",
        "ai_primitives": "AIPrimitivesBenchmark",
        "clickbench": "ClickBenchBenchmark",
        "h2odb": "H2OBenchmark",
        "amplab": "AMPLabBenchmark",
        "tpcdi": "TPCDIBenchmark",
        "merge": "MergeBenchmark",
        "joinorder": "JoinOrderBenchmark",
        "tpchavoc": "TPCHavocBenchmark",
        "tpch_skew": "TPCHSkewBenchmark",
        "tsbs_devops": "TSBSDevOpsBenchmark",
        "nyctaxi": "NYCTaxiBenchmark",
        "coffeeshop": "CoffeeShopBenchmark",
        "write_primitives": "WritePrimitivesBenchmark",
        "datavault": "DataVaultBenchmark",
        "tpcds_obt": "TPCDSOBTBenchmark",
    }

    class_name = class_name_map.get(benchmark_name, f"{benchmark_name.capitalize()}Benchmark")

    try:
        module = importlib.import_module(module_name)
        benchmark_class = getattr(module, class_name)
        return benchmark_class
    except (ImportError, AttributeError) as e:
        available = [
            "read_primitives",
            "metadata_primitives",
            "ai_primitives",
            "tpch",
            "tpcds",
            "tpcds_obt",
            "ssb",
            "clickbench",
            "h2odb",
            "amplab",
            "tpcdi",
            "merge",
            "joinorder",
            "tpchavoc",
            "tpch_skew",
            "tsbs_devops",
            "nyctaxi",
            "coffeeshop",
            "write_primitives",
            "datavault",
        ]
        raise ValueError(f"Benchmark '{benchmark_name}' not supported yet. Available: {', '.join(available)}") from e
