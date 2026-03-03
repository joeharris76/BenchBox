"""Data organization module for pre-sorted and clustered dataset generation.

Provides post-processing of generated TBL/CSV data into sorted Parquet files
with configurable sort columns, row group sizes, and clustering strategies.
"""

from benchbox.core.data_organization.clustering import (
    HilbertClusterer,
    ZOrderClusterer,
    hilbert_index_2d,
    z_order_key,
)
from benchbox.core.data_organization.config import (
    DataOrganizationConfig,
    SortColumn,
    SortOrder,
)
from benchbox.core.data_organization.sorting import SortedParquetWriter

__all__ = [
    "DataOrganizationConfig",
    "SortColumn",
    "SortOrder",
    "SortedParquetWriter",
    "ZOrderClusterer",
    "HilbertClusterer",
    "z_order_key",
    "hilbert_index_2d",
]
