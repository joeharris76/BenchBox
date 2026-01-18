"""Core data models for TPC-DS schema definitions."""

from __future__ import annotations

from enum import Enum
from typing import NamedTuple


class DataType(Enum):
    """Enumeration of SQL data types used in TPC-DS."""

    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL(15,2)"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"


class Column(NamedTuple):
    """Represents a column in a database table."""

    name: str
    data_type: DataType
    size: int | None = None  # For VARCHAR and CHAR types
    nullable: bool = False
    primary_key: bool = False
    foreign_key: tuple[str, str] | None = None  # (table_name, column_name)

    def get_sql_type(self) -> str:
        """Get the SQL data type string for this column."""
        if self.data_type in (DataType.VARCHAR, DataType.CHAR) and self.size is not None:
            return f"{self.data_type.value}({self.size})"
        return self.data_type.value


class Table:
    """Represents a database table with its columns and constraints."""

    def __init__(self, name: str, columns: list[Column]) -> None:
        """Initialize a Table with a name and list of columns.

        Args:
            name: The name of the table
            columns: List of column definitions
        """
        self.name = name
        self.columns = columns

    def get_primary_key(self) -> list[str]:
        """Get the primary key column names for this table."""
        return [col.name for col in self.columns if col.primary_key]

    def get_foreign_keys(self) -> dict[str, tuple[str, str]]:
        """Get the foreign key mappings for this table.

        Returns:
            A dictionary mapping column names to (table, column) pairs
        """
        return {col.name: col.foreign_key for col in self.columns if col.foreign_key is not None}

    def get_create_table_sql(
        self,
        enable_primary_keys: bool = True,
        enable_foreign_keys: bool = True,
    ) -> str:
        """Generate CREATE TABLE SQL statement for this table."""
        column_defs = []
        pk_columns = []
        fk_defs = []

        for col in self.columns:
            col_def = f"{col.name} {col.get_sql_type()}"

            if not col.nullable:
                col_def += " NOT NULL"

            if col.primary_key and enable_primary_keys:
                pk_columns.append(col.name)

            if col.foreign_key and enable_foreign_keys:
                ref_table, ref_col = col.foreign_key
                fk_defs.append(f"FOREIGN KEY ({col.name}) REFERENCES {ref_table}({ref_col})")

            column_defs.append(col_def)

        # Add primary key constraint if columns are marked as PK and enabled
        if pk_columns and enable_primary_keys:
            column_defs.append(f"PRIMARY KEY ({', '.join(pk_columns)})")

        # Add foreign key constraints if enabled
        if enable_foreign_keys:
            column_defs.extend(fk_defs)

        sql = f"CREATE TABLE {self.name} (\n    "
        sql += ",\n    ".join(column_defs)
        sql += "\n);"

        return sql


__all__ = ["DataType", "Column", "Table"]
