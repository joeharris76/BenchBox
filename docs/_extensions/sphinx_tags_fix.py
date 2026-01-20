"""
Sphinx extension to fix sphinx-tags navigation behavior.

sphinx-tags generates tag pages with toctree directives, which causes
linked pages to appear in the sidebar navigation. This extension:

1. Converts toctrees in individual tag pages to doc references (not navigation)
2. Creates category pages for hierarchical sidebar navigation
3. Reorganizes tagsindex.md to link to category pages

See: https://github.com/melissawm/sphinx-tags/issues/106
"""

import re
from pathlib import Path

from sphinx.application import Sphinx

# Tag categories for organizing the tags index
# Maps category slug -> (display name, list of tags)
TAG_CATEGORIES = {
    "audience": (
        "By Audience",
        [
            "beginner",
            "intermediate",
            "advanced",
            "contributor",
        ],
    ),
    "benchmark": (
        "By Benchmark",
        [
            # TPC family
            "tpc-h",
            "tpc-ds",
            "tpc-di",
            "tpc-havoc",
            "tpch-skew",
            # Industry standard
            "ssb",
            "clickbench",
            "h2odb",
            "join-order",
            "amplab",
            # Real-world datasets
            "nyctaxi",
            "coffeeshop",
            "datavault",
            "tsbs-devops",
            # Primitives
            "read-primitives",
            "write-primitives",
            "transaction-primitives",
            "metadata-primitives",
            "ai-primitives",
            # Meta
            "custom-benchmark",
        ],
    ),
    "platform": (
        "By Platform",
        [
            # Embedded/Local SQL
            "duckdb",
            "sqlite",
            "postgresql",
            "datafusion",
            # Cloud Data Warehouses
            "snowflake",
            "databricks",
            "bigquery",
            "redshift",
            "motherduck",
            "starburst",
            # OLAP/Analytics
            "clickhouse",
            "trino",
            "presto",
            "firebolt",
            # Time Series
            "timescaledb",
            "influxdb",
            # AWS
            "athena",
            "aws-glue",
            "emr-serverless",
            "athena-spark",
            # GCP
            "dataproc",
            "dataproc-serverless",
            # Azure
            "azure",
            "fabric",
            "fabric-spark",
            "synapse-spark",
            # Spark
            "spark",
            "pyspark",
            # DataFrame libraries
            "pandas",
            "polars",
            "dask",
            "modin",
            "cudf",
            "datafusion-df",
        ],
    ),
    "platform-type": (
        "By Platform Type",
        [
            "sql-platform",
            "dataframe-platform",
            "cloud-platform",
            "embedded-platform",
            "cloud-storage",
        ],
    ),
    "content-type": (
        "By Content Type",
        [
            "guide",
            "tutorial",
            "reference",
            "concept",
            "quickstart",
        ],
    ),
    "feature": (
        "By Feature",
        [
            "architecture",
            "cli",
            "cloud",
            "e2e",
            "python-api",
            "configuration",
            "data-generation",
            "performance",
            "tuning",
            "validation",
            "testing",
            "visualization",
        ],
    ),
}

# Store tag counts discovered during build (populated by tagsindex processing)
_tag_counts: dict[str, str] = {}


def create_category_pages(app: Sphinx) -> None:
    """Create stub category pages that will be populated during source-read."""
    global _tag_counts

    tags_output_dir = getattr(app.config, "tags_output_dir", "_tags")
    tags_dir = Path(app.srcdir) / tags_output_dir

    # Ensure tags directory exists (sphinx-tags may not have run yet)
    tags_dir.mkdir(parents=True, exist_ok=True)

    # Pre-read tagsindex to get tag counts (needed before category pages are read)
    tagsindex_file = tags_dir / "tagsindex.md"
    if tagsindex_file.exists():
        content = tagsindex_file.read_text()
        tag_pattern = re.compile(r"^([a-z0-9-]+)\s+\((\d+)\)\s+<([a-z0-9-]+)>$", re.MULTILINE)
        _tag_counts = {match.group(1): match.group(2) for match in tag_pattern.finditer(content)}

    for category_slug in TAG_CATEGORIES:
        category_file = tags_dir / f"cat-{category_slug}.md"
        # Create stub file - content will be generated in source-read
        if not category_file.exists():
            category_file.write_text(f"# Category: {category_slug}\n")


def fix_tag_sources(app: Sphinx, docname: str, source: list) -> None:
    """Modify tag page sources before Sphinx parses them."""
    tags_output_dir = getattr(app.config, "tags_output_dir", "_tags")

    # Only process files in _tags directory
    if not docname.startswith(tags_output_dir + "/"):
        return

    basename = docname.split("/")[-1]

    if basename == "tagsindex":
        # Extract tag counts and reorganize into categories
        source[0] = _reorganize_tagsindex(source[0])
    elif basename.startswith("cat-"):
        # Generate category page content
        category_slug = basename[4:]  # Remove "cat-" prefix
        source[0] = _generate_category_page(category_slug)
    else:
        # Convert toctree to doc references in individual tag pages
        source[0] = _hide_tag_page_toctree(source[0])


def _hide_tag_page_toctree(content: str) -> str:
    """Convert toctree in a tag page to simple doc references.

    Toctrees always contribute to sidebar navigation, even with hidden: true.
    To exclude content pages from sidebar, we must replace toctree with
    regular doc references that only appear in the page body.
    """
    # Match MyST toctree directive with YAML frontmatter and entries
    pattern = re.compile(
        r"```\{toctree\}\n"
        r"---\n"
        r"maxdepth:\s*\d+\n"
        r"caption:\s*([^\n]+)\n"  # Capture caption
        r"---\n"
        r"(.*?)"  # Capture entries
        r"```",
        re.DOTALL,
    )

    def replace_with_refs(match: re.Match) -> str:
        caption = match.group(1)
        entries = match.group(2).strip().split("\n")

        # Convert file paths to doc references
        lines = [f"**{caption}**", ""]
        for entry in entries:
            entry = entry.strip()
            if not entry:
                continue
            # Remove .md/.rst extension for doc reference
            doc_path = re.sub(r"\.(md|rst)$", "", entry)
            lines.append(f"- {{doc}}`{doc_path}`")

        return "\n".join(lines)

    return pattern.sub(replace_with_refs, content)


def _reorganize_tagsindex(content: str) -> str:
    """Reorganize tagsindex.md to link to category pages."""
    global _tag_counts

    # Extract all tag entries from the existing toctree
    # Format: "tag-name (count) <tag-name>"
    tag_pattern = re.compile(r"^([a-z0-9-]+)\s+\((\d+)\)\s+<([a-z0-9-]+)>$", re.MULTILINE)
    _tag_counts = {match.group(1): match.group(2) for match in tag_pattern.finditer(content)}

    if not _tag_counts:
        return content  # No tags found, return original

    # Build new content with links to category pages
    lines = [
        "(tagoverview)=",
        "",
        "# Browse by Tag",
        "",
        "Select a category to browse tags:",
        "",
        "```{toctree}",
        "---",
        "maxdepth: 2",
        "---",
    ]

    # Add category pages
    for category_slug, (display_name, _tags) in TAG_CATEGORIES.items():
        lines.append(f"{display_name} <cat-{category_slug}>")

    lines.append("```")
    lines.append("")

    return "\n".join(lines)


def _generate_category_page(category_slug: str) -> str:
    """Generate content for a category page."""
    if category_slug not in TAG_CATEGORIES:
        return f"# Unknown category: {category_slug}\n"

    display_name, category_tags = TAG_CATEGORIES[category_slug]

    # Find tags that exist in this category
    existing_tags = [(t, _tag_counts.get(t, "?")) for t in category_tags if t in _tag_counts]

    lines = [
        f"# {display_name}",
        "",
    ]

    if existing_tags:
        lines.append("```{toctree}")
        lines.append("---")
        lines.append("maxdepth: 1")
        lines.append("---")
        for tag_name, count in existing_tags:
            lines.append(f"{tag_name} ({count}) <{tag_name}>")
        lines.append("```")
    else:
        lines.append("*No tags in this category yet.*")

    lines.append("")
    return "\n".join(lines)


def setup(app: Sphinx) -> dict:
    """Set up the extension."""
    # Create category stub files before build starts
    app.connect("builder-inited", create_category_pages)
    # Modify sources during read
    app.connect("source-read", fix_tag_sources)

    return {
        "version": "0.3",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
