"""Manifest handling mixin for the TPC-DI data generator."""

from __future__ import annotations

import json
from pathlib import Path


class ManifestMixin:
    """Provide manifest persistence and validation helpers."""

    def _validate_file_format_consistency(self, target_dir: Path) -> None:
        """Ensure no raw .tbl files exist when compression is enabled; ensure no empty compressed files."""
        if not self.should_use_compression():
            return
        raw_tbl = list(target_dir.glob("*.tbl"))
        if raw_tbl:
            names = ", ".join(f.name for f in raw_tbl[:5])
            more = "..." if len(raw_tbl) > 5 else ""
            raise RuntimeError(
                f"File format consistency violation: Found raw .tbl files with compression enabled: {names}{more}"
            )
        ext = self.get_compressor().get_file_extension()
        compressed = list(target_dir.glob(f"*.tbl{ext}"))
        empties = [f for f in compressed if f.stat().st_size <= (9 if ext == ".zst" else 20)]
        if empties:
            names = ", ".join(f.name for f in empties[:5])
            more = "..." if len(empties) > 5 else ""
            raise RuntimeError(f"File format consistency violation: Found empty compressed files: {names}{more}")

    def _write_manifest(self, output_dir: Path, table_paths: dict[str, str]) -> None:
        from datetime import datetime, timezone

        manifest = {
            "benchmark": "tpcdi",
            "scale_factor": self.scale_factor,
            "compression": {
                "enabled": self.should_use_compression(),
                "type": getattr(self, "compression_type", None),
                "level": getattr(self, "compression_level", None),
            },
            "parallel": self.max_workers or 1,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "generator_version": "v1",
            "tables": {},
        }
        for table, path_str in table_paths.items():
            p = Path(path_str)
            size = p.stat().st_size if p.exists() else 0
            rows = 0
            try:
                if str(p).endswith(".gz"):
                    import gzip

                    with gzip.open(p, "rt") as f:
                        rows = sum(1 for _ in f)
                elif str(p).endswith(".zst"):
                    try:
                        import zstandard as zstd

                        dctx = zstd.ZstdDecompressor()
                        with open(p, "rb") as fh, dctx.stream_reader(fh) as reader:
                            import io

                            rows = sum(1 for _ in io.TextIOWrapper(reader))
                    except Exception:
                        rows = 0
                else:
                    with open(p, "rb") as f:
                        rows = sum(1 for _ in f)
            except Exception:
                rows = 0
            manifest["tables"].setdefault(table, []).append(
                {
                    "path": p.name,
                    "size_bytes": size,
                    "row_count": rows,
                }
            )
        out = output_dir / "_datagen_manifest.json"
        with open(out, "w") as f:
            json.dump(manifest, f, indent=2)


__all__ = ["ManifestMixin"]
