"""TPC-DI file parsers for CSV, XML, and fixed-width format processing.

Simplified parsers that delegate bulk processing to database engines.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ DI (TPC-DI) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-DI specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import logging
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional

import pandas as pd

try:
    import chardet

    HAS_CHARDET = True
except ImportError:
    HAS_CHARDET = False


class ParseResult:
    """Result of a file parsing operation."""

    def __init__(
        self,
        data: pd.DataFrame,
        records_parsed: int,
        records_skipped: int = 0,
        parsing_errors: Optional[list[str]] = None,
        schema_info: Optional[dict[str, Any]] = None,
        quality_metrics: Optional[dict[str, Any]] = None,
    ):
        """Initialize parse result.

        Args:
            data: Parsed dataframe
            records_parsed: Number of records successfully parsed
            records_skipped: Number of records skipped due to errors
            parsing_errors: List of parsing error messages
            schema_info: Information about detected schema
            quality_metrics: Data quality metrics
        """
        self.data = data
        self.records_parsed = records_parsed
        self.records_skipped = records_skipped
        self.parsing_errors = parsing_errors or []
        self.schema_info = schema_info if schema_info is not None else {}
        self.quality_metrics = quality_metrics if quality_metrics is not None else {}

    @property
    def success_rate(self) -> float:
        """Calculate parsing success rate."""
        total = self.records_parsed + self.records_skipped
        return self.records_parsed / total if total > 0 else 0.0

    @property
    def has_errors(self) -> bool:
        """Check if parsing had any errors."""
        return len(self.parsing_errors) > 0 or self.records_skipped > 0


class FileParser(ABC):
    """Abstract base class for file parsers."""

    def __init__(self) -> None:
        """Initialize base parser."""
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def parse_file(self, file_path: Path, **kwargs: Any) -> ParseResult:
        """Parse a file and return structured data.

        Args:
            file_path: Path to the file to parse
            **kwargs: Additional parser-specific parameters

        Returns:
            ParseResult containing parsed data and metadata
        """
        ...

    def _detect_encoding(self, file_path: Path) -> str:
        """Detect file encoding using chardet if available."""
        if not HAS_CHARDET:
            return "utf-8"

        try:
            with open(file_path, "rb") as f:
                raw_data = f.read(10000)  # Read first 10KB for detection
                result = chardet.detect(raw_data)
                encoding = result.get("encoding", "utf-8")
                confidence = result.get("confidence", 0.0)

                if confidence < 0.7:
                    self.logger.warning(f"Low encoding confidence ({confidence:.2f}) for {file_path}, using utf-8")
                    return "utf-8"

                return encoding
        except Exception as e:
            self.logger.warning(f"Encoding detection failed for {file_path}: {e}, using utf-8")
            return "utf-8"


class CSVParser(FileParser):
    """Parser for CSV files with TPC-DI specific handling."""

    def __init__(
        self,
        delimiter: str = ",",
        quote_char: str = '"',
        escape_char: Optional[str] = None,
        encoding: Optional[str] = None,
        skip_blank_lines: bool = True,
        header_row: int = 0,
        expected_columns: Optional[list[str]] = None,
        dtype_mapping: Optional[dict[str, str]] = None,
    ):
        """Initialize CSV parser.

        Args:
            delimiter: Field delimiter character
            quote_char: Quote character for text fields
            escape_char: Escape character (None for auto-detection)
            encoding: Character encoding of the file (None for auto-detection)
            skip_blank_lines: Whether to skip blank lines
            header_row: Row number containing column headers (0-based)
            expected_columns: List of expected column names for validation
            dtype_mapping: Mapping of column names to data types
        """
        super().__init__()
        self.delimiter = delimiter
        self.quote_char = quote_char
        self.escape_char = escape_char
        self.encoding = encoding
        self.skip_blank_lines = skip_blank_lines
        self.header_row = header_row
        self.expected_columns = expected_columns
        self.dtype_mapping = dtype_mapping or {}

    def parse_file(
        self,
        file_path: Path,
        **kwargs,
    ) -> ParseResult:
        """Parse a CSV file.

        Args:
            file_path: Path to the CSV file
            **kwargs: Additional pandas read_csv parameters

        Returns:
            ParseResult with parsed data and metadata
        """
        self.logger.info(f"Parsing CSV file: {file_path}")

        # Detect encoding if not specified
        encoding = self.encoding or self._detect_encoding(file_path)

        try:
            # Build pandas read_csv parameters
            read_params = {
                "delimiter": self.delimiter,
                "quotechar": self.quote_char,
                "encoding": encoding,
                "skip_blank_lines": self.skip_blank_lines,
                "header": self.header_row,
                "dtype": self.dtype_mapping,
                **kwargs,
            }

            if self.escape_char:
                read_params["escapechar"] = self.escape_char

            # Read the entire file into a DataFrame
            df = pd.read_csv(file_path, **read_params)

            # Validate columns if expected columns are provided
            if self.expected_columns:
                missing_cols = set(self.expected_columns) - set(df.columns)
                if missing_cols:
                    self.logger.warning(f"Missing expected columns: {missing_cols}")

            records_parsed = len(df)

            # Create schema info
            schema_info = {
                "columns": list(df.columns),
                "dtypes": df.dtypes.to_dict(),
                "shape": df.shape,
                "encoding": encoding,
            }

            # Basic quality metrics
            quality_metrics = {
                "null_counts": df.isnull().sum().to_dict(),
                "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
            }

            self.logger.info(f"Successfully parsed {records_parsed} records from {file_path}")

            return ParseResult(
                data=df,
                records_parsed=records_parsed,
                records_skipped=0,
                parsing_errors=[],
                schema_info=schema_info,
                quality_metrics=quality_metrics,
            )

        except Exception as e:
            self.logger.error(f"Failed to parse CSV file {file_path}: {e}")
            # Return empty result with error
            return ParseResult(
                data=pd.DataFrame(),
                records_parsed=0,
                records_skipped=0,
                parsing_errors=[str(e)],
                schema_info={},
                quality_metrics={},
            )


class PipeDelimitedParser(CSVParser):
    """Parser for pipe-delimited files (common in TPC-DI)."""

    def __init__(self, **kwargs):
        """Initialize pipe-delimited parser."""
        kwargs.setdefault("delimiter", "|")
        super().__init__(**kwargs)


class FixedWidthParser(FileParser):
    """Parser for fixed-width format files."""

    def __init__(
        self,
        field_specifications: list[tuple[str, int, int]],
        encoding: Optional[str] = None,
        skip_blank_lines: bool = True,
        header_lines: int = 0,
        dtype_mapping: Optional[dict[str, str]] = None,
    ):
        """Initialize fixed-width parser.

        Args:
            field_specifications: List of (field_name, start_pos, length) tuples
            encoding: Character encoding of the file
            skip_blank_lines: Whether to skip blank lines
            header_lines: Number of header lines to skip
            dtype_mapping: Mapping of field names to data types
        """
        super().__init__()
        self.field_specifications = field_specifications
        self.encoding = encoding
        self.skip_blank_lines = skip_blank_lines
        self.header_lines = header_lines
        self.dtype_mapping = dtype_mapping or {}

    def parse_file(self, file_path: Path, **kwargs) -> ParseResult:
        """Parse a fixed-width format file.

        Args:
            file_path: Path to the fixed-width file
            **kwargs: Additional parameters

        Returns:
            ParseResult with parsed data and metadata
        """
        self.logger.info(f"Parsing fixed-width file: {file_path}")

        # Detect encoding if not specified
        encoding = self.encoding or self._detect_encoding(file_path)

        try:
            # Build column specifications for pandas
            colspecs = [(start, start + length) for _, start, length in self.field_specifications]
            names = [name for name, _, _ in self.field_specifications]

            # Read the fixed-width file
            df = pd.read_fwf(
                file_path,
                colspecs=colspecs,
                names=names,
                encoding=encoding,
                skiprows=self.header_lines,
                dtype=self.dtype_mapping,
                **kwargs,
            )

            # Remove blank lines if requested
            if self.skip_blank_lines:
                df = df.dropna(how="all")

            records_parsed = len(df)

            # Create schema info
            schema_info = {
                "columns": list(df.columns),
                "dtypes": df.dtypes.to_dict(),
                "shape": df.shape,
                "encoding": encoding,
                "field_specifications": self.field_specifications,
            }

            # Basic quality metrics
            quality_metrics = {
                "null_counts": df.isnull().sum().to_dict(),
                "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
            }

            self.logger.info(f"Successfully parsed {records_parsed} records from {file_path}")

            return ParseResult(
                data=df,
                records_parsed=records_parsed,
                records_skipped=0,
                parsing_errors=[],
                schema_info=schema_info,
                quality_metrics=quality_metrics,
            )

        except Exception as e:
            self.logger.error(f"Failed to parse fixed-width file {file_path}: {e}")
            return ParseResult(
                data=pd.DataFrame(),
                records_parsed=0,
                records_skipped=0,
                parsing_errors=[str(e)],
                schema_info={},
                quality_metrics={},
            )


class XMLParser(FileParser):
    """Parser for XML files with TPC-DI specific handling."""

    def __init__(
        self,
        record_tag: str,
        field_mappings: Optional[dict[str, str]] = None,
        attribute_mappings: Optional[dict[str, str]] = None,
        encoding: Optional[str] = None,
        namespace_map: Optional[dict[str, str]] = None,
        dtype_mapping: Optional[dict[str, str]] = None,
    ):
        """Initialize XML parser.

        Args:
            record_tag: XML tag name that represents a single record
            field_mappings: Mapping of XML tags/attributes to column names
            attribute_mappings: Mapping of XML attributes to column names
            encoding: Character encoding of the file
            namespace_map: XML namespace mappings
            dtype_mapping: Mapping of column names to data types
        """
        super().__init__()
        self.record_tag = record_tag
        self.field_mappings = field_mappings or {}
        self.attribute_mappings = attribute_mappings or {}
        self.encoding = encoding
        self.namespace_map = namespace_map or {}
        self.dtype_mapping = dtype_mapping or {}

    def parse_file(self, file_path: Path, **kwargs) -> ParseResult:
        """Parse an XML file.

        Args:
            file_path: Path to the XML file
            **kwargs: Additional parameters

        Returns:
            ParseResult with parsed data and metadata
        """
        self.logger.info(f"Parsing XML file: {file_path}")

        try:
            # Parse the XML file
            tree = ET.parse(file_path)
            root = tree.getroot()

            # Find all record elements
            record_elements = root.findall(f".//{self.record_tag}")

            records = []
            parsing_errors = []

            for i, element in enumerate(record_elements):
                try:
                    record = self._extract_record_data(element)
                    records.append(record)
                except Exception as e:
                    parsing_errors.append(f"Error parsing record {i}: {e}")
                    continue

            # Create DataFrame from records
            if records:
                df = pd.DataFrame(records)

                # Apply data type mappings
                for col, dtype in self.dtype_mapping.items():
                    if col in df.columns:
                        try:
                            df[col] = df[col].astype(dtype)
                        except Exception as e:
                            self.logger.warning(f"Failed to convert column {col} to {dtype}: {e}")
            else:
                df = pd.DataFrame()

            records_parsed = len(df)
            records_skipped = len(parsing_errors)

            # Create schema info
            schema_info = {
                "columns": list(df.columns) if not df.empty else [],
                "dtypes": df.dtypes.to_dict() if not df.empty else {},
                "shape": df.shape,
                "record_tag": self.record_tag,
                "total_elements_found": len(record_elements),
            }

            # Basic quality metrics
            quality_metrics = {
                "null_counts": df.isnull().sum().to_dict() if not df.empty else {},
                "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024 if not df.empty else 0,
            }

            self.logger.info(f"Successfully parsed {records_parsed} records from {file_path}")
            if parsing_errors:
                self.logger.warning(f"Skipped {records_skipped} records due to parsing errors")

            return ParseResult(
                data=df,
                records_parsed=records_parsed,
                records_skipped=records_skipped,
                parsing_errors=parsing_errors,
                schema_info=schema_info,
                quality_metrics=quality_metrics,
            )

        except Exception as e:
            self.logger.error(f"Failed to parse XML file {file_path}: {e}")
            return ParseResult(
                data=pd.DataFrame(),
                records_parsed=0,
                records_skipped=0,
                parsing_errors=[str(e)],
                schema_info={},
                quality_metrics={},
            )

    def _extract_record_data(self, element: ET.Element) -> dict[str, Any]:
        """Extract data from a single XML record element.

        Args:
            element: XML element representing a record

        Returns:
            Dictionary with extracted field data
        """
        record = {}

        # Extract attributes
        for attr_name, col_name in self.attribute_mappings.items():
            value = element.get(attr_name)
            if value is not None:
                record[col_name] = value

        # Extract child element text
        for child in element:
            tag_name = child.tag
            col_name = self.field_mappings.get(tag_name, tag_name)

            # Handle namespaces
            if "}" in tag_name:
                tag_name = tag_name.split("}")[1]
                col_name = self.field_mappings.get(tag_name, tag_name)

            record[col_name] = child.text if child.text is not None else ""

        return record


class MultiFormatParser:
    """Utility class for parsing multiple file formats."""

    def __init__(self):
        """Initialize multi-format parser."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self._parsers = {
            ".csv": CSVParser,
            ".txt": PipeDelimitedParser,  # Assume pipe-delimited for .txt
            ".xml": XMLParser,
        }

    def get_parser(self, file_path: Path, parser_config: Optional[dict[str, Any]] = None) -> FileParser:
        """Get appropriate parser for a file based on its extension.

        Args:
            file_path: Path to the file
            parser_config: Configuration for the parser

        Returns:
            Configured parser instance

        Raises:
            ValueError: If file format is not supported
        """
        suffix = file_path.suffix.lower()
        parser_class = self._parsers.get(suffix)

        if not parser_class:
            raise ValueError(f"Unsupported file format: {suffix}")

        config = parser_config or {}
        return parser_class(**config)

    def parse_file(self, file_path: Path, parser_config: Optional[dict[str, Any]] = None, **kwargs) -> ParseResult:
        """Parse a file using the appropriate parser.

        Args:
            file_path: Path to the file
            parser_config: Configuration for the parser
            **kwargs: Additional parsing parameters

        Returns:
            ParseResult with parsed data and metadata
        """
        parser = self.get_parser(file_path, parser_config)
        return parser.parse_file(file_path, **kwargs)


# Convenience functions for common TPC-DI file types
def parse_csv_file(file_path: Path, **kwargs) -> ParseResult:
    """Parse a CSV file with default CSV parser."""
    parser = CSVParser()
    return parser.parse_file(file_path, **kwargs)


def parse_pipe_delimited_file(file_path: Path, **kwargs) -> ParseResult:
    """Parse a pipe-delimited file."""
    parser = PipeDelimitedParser()
    return parser.parse_file(file_path, **kwargs)


def parse_fixed_width_file(file_path: Path, field_specifications: list[tuple[str, int, int]], **kwargs) -> ParseResult:
    """Parse a fixed-width file."""
    parser = FixedWidthParser(field_specifications)
    return parser.parse_file(file_path, **kwargs)


def parse_xml_file(file_path: Path, record_tag: str, **kwargs) -> ParseResult:
    """Parse an XML file."""
    parser = XMLParser(record_tag)
    return parser.parse_file(file_path, **kwargs)
