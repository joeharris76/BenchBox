# TPC-Havoc Variant Layout

The modular variant system keeps SQL-heavy definitions manageable:

- `variant_base.py` defines the shared generator abstractions (`VariantGenerator` and `StaticSQLVariant`).
- `variant_sets/` contains one module per TPC-H query (`q01.py`…`q22.py`) with static SQL variants built from the legacy definitions.
- `variants.py` re-exports the public API (`Qx_VARIANTS`, `VARIANT_REGISTRY`) so existing imports continue to work.

When adding or editing variants:

1. Update the appropriate module in `variant_sets/`, keeping descriptions and SQL in sync.
2. Extend the targeted tests in `tests/unit/tpchavoc/` to cover new behaviours.
3. Avoid importing heavy modules from data-only variant files to keep import time minimal.

This layout was introduced by the “TPC-Havoc Variant Modularization” TODO to make future maintenance significantly easier.
