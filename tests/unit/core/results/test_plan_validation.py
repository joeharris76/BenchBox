"""Tests for query plan tree validation."""

from __future__ import annotations

import pytest

from benchbox.core.results.query_plan_models import (
    LogicalOperator,
    LogicalOperatorType,
    QueryPlanDAG,
    describe_tree,
    validate_plan_tree,
    validate_root_operator,
)

pytestmark = pytest.mark.fast


class TestValidatePlanTree:
    """Tests for validate_plan_tree function."""

    def test_valid_tree_passes_validation(self) -> None:
        """Test that a well-formed tree passes validation."""
        root = LogicalOperator(
            operator_type=LogicalOperatorType.PROJECT,
            operator_id="project_1",
            children=[
                LogicalOperator(
                    operator_type=LogicalOperatorType.SCAN,
                    operator_id="scan_1",
                    table_name="orders",
                )
            ],
        )

        errors = validate_plan_tree(root)

        assert errors == []

    def test_detects_duplicate_operator_ids(self) -> None:
        """Test that duplicate operator IDs are detected."""
        root = LogicalOperator(
            operator_type=LogicalOperatorType.PROJECT,
            operator_id="op_1",
            children=[
                LogicalOperator(
                    operator_type=LogicalOperatorType.SCAN,
                    operator_id="op_1",  # Duplicate!
                )
            ],
        )

        errors = validate_plan_tree(root)

        assert len(errors) == 1
        assert "Duplicate operator_id" in errors[0]

    def test_detects_empty_operator_type(self) -> None:
        """Test that missing operator type is detected."""
        root = LogicalOperator(
            operator_type="",  # type: ignore[arg-type]
            operator_id="op_1",
        )

        errors = validate_plan_tree(root)

        assert len(errors) == 1
        assert "Missing operator_type" in errors[0]

    def test_single_operator_tree_is_valid(self) -> None:
        """Test that a single-operator tree is valid."""
        root = LogicalOperator(
            operator_type=LogicalOperatorType.SCAN,
            operator_id="scan_1",
            table_name="test",
        )

        errors = validate_plan_tree(root)

        assert errors == []

    def test_deep_tree_is_valid(self) -> None:
        """Test that a deep tree is valid if well-formed."""
        # Build a 10-level deep tree
        current = LogicalOperator(
            operator_type=LogicalOperatorType.SCAN,
            operator_id="scan_1",
        )
        for i in range(10):
            current = LogicalOperator(
                operator_type=LogicalOperatorType.FILTER,
                operator_id=f"filter_{i}",
                children=[current],
            )

        errors = validate_plan_tree(current)

        assert errors == []

    def test_tree_with_multiple_children_is_valid(self) -> None:
        """Test that a tree with multiple children per node is valid."""
        root = LogicalOperator(
            operator_type=LogicalOperatorType.JOIN,
            operator_id="join_1",
            children=[
                LogicalOperator(
                    operator_type=LogicalOperatorType.SCAN,
                    operator_id="scan_1",
                ),
                LogicalOperator(
                    operator_type=LogicalOperatorType.SCAN,
                    operator_id="scan_2",
                ),
            ],
        )

        errors = validate_plan_tree(root)

        assert errors == []


class TestValidateRootOperator:
    """Tests for validate_root_operator function."""

    def test_project_root_is_valid(self) -> None:
        """Test that Project as root is valid."""
        root = LogicalOperator(
            operator_type=LogicalOperatorType.PROJECT,
            operator_id="project_1",
        )

        warnings = validate_root_operator(root)

        assert warnings == []

    def test_sort_root_is_valid(self) -> None:
        """Test that Sort as root is valid."""
        root = LogicalOperator(
            operator_type=LogicalOperatorType.SORT,
            operator_id="sort_1",
        )

        warnings = validate_root_operator(root)

        assert warnings == []

    def test_scan_root_gives_warning(self) -> None:
        """Test that Scan as root generates a warning."""
        root = LogicalOperator(
            operator_type=LogicalOperatorType.SCAN,
            operator_id="scan_1",
        )

        warnings = validate_root_operator(root)

        assert len(warnings) == 1
        assert "unusual" in warnings[0].lower()

    def test_filter_root_gives_warning(self) -> None:
        """Test that Filter as root generates a warning."""
        root = LogicalOperator(
            operator_type=LogicalOperatorType.FILTER,
            operator_id="filter_1",
        )

        warnings = validate_root_operator(root)

        assert len(warnings) == 1
        assert "unusual" in warnings[0].lower()


class TestFingerprintVerification:
    """Tests for fingerprint integrity verification."""

    def test_unmodified_plan_passes_verification(self) -> None:
        """Test that an unmodified plan passes fingerprint verification."""
        plan = QueryPlanDAG(
            query_id="q01",
            platform="test",
            logical_root=LogicalOperator(
                operator_type=LogicalOperatorType.SCAN,
                operator_id="scan_1",
            ),
        )

        assert plan.verify_fingerprint() is True

    def test_modified_plan_fails_verification(self) -> None:
        """Test that modifying the tree invalidates the fingerprint."""
        plan = QueryPlanDAG(
            query_id="q01",
            platform="test",
            logical_root=LogicalOperator(
                operator_type=LogicalOperatorType.SCAN,
                operator_id="scan_1",
            ),
        )

        # Modify the tree after creation
        plan.logical_root.table_name = "new_table"

        assert plan.verify_fingerprint() is False

    def test_refresh_fingerprint_updates_correctly(self) -> None:
        """Test that refresh_fingerprint updates after modification."""
        plan = QueryPlanDAG(
            query_id="q01",
            platform="test",
            logical_root=LogicalOperator(
                operator_type=LogicalOperatorType.SCAN,
                operator_id="scan_1",
            ),
        )

        # Modify the tree
        plan.logical_root.table_name = "new_table"
        assert plan.verify_fingerprint() is False

        # Refresh fingerprint
        plan.refresh_fingerprint()
        assert plan.verify_fingerprint() is True

    def test_deserialized_plan_passes_verification(self) -> None:
        """Test that a plan round-tripped through serialization passes verification."""
        original = QueryPlanDAG(
            query_id="q01",
            platform="test",
            logical_root=LogicalOperator(
                operator_type=LogicalOperatorType.PROJECT,
                operator_id="project_1",
                children=[
                    LogicalOperator(
                        operator_type=LogicalOperatorType.SCAN,
                        operator_id="scan_1",
                    )
                ],
            ),
        )

        # Serialize and deserialize
        json_str = original.to_json()
        restored = QueryPlanDAG.from_json(json_str)

        assert restored.verify_fingerprint() is True
        assert restored.plan_fingerprint == original.plan_fingerprint


class TestDescribeTree:
    """Tests for describe_tree function."""

    def test_describe_single_node(self) -> None:
        """Test describing a single node tree."""
        root = LogicalOperator(
            operator_type=LogicalOperatorType.SCAN,
            operator_id="scan_1",
        )

        description = describe_tree(root)

        assert "Scan" in description
        assert "scan_1" in description

    def test_describe_tree_with_children(self) -> None:
        """Test describing a tree with children."""
        root = LogicalOperator(
            operator_type=LogicalOperatorType.PROJECT,
            operator_id="project_1",
            children=[
                LogicalOperator(
                    operator_type=LogicalOperatorType.SCAN,
                    operator_id="scan_1",
                )
            ],
        )

        description = describe_tree(root)

        assert "Project" in description
        assert "Scan" in description

    def test_describe_truncates_deep_tree(self) -> None:
        """Test that deep trees are truncated."""
        # Build a tree deeper than max_depth
        current = LogicalOperator(
            operator_type=LogicalOperatorType.SCAN,
            operator_id="scan_1",
        )
        for i in range(10):
            current = LogicalOperator(
                operator_type=LogicalOperatorType.FILTER,
                operator_id=f"filter_{i}",
                children=[current],
            )

        description = describe_tree(current, max_depth=3)

        assert "truncated" in description.lower()
