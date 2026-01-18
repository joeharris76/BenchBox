# BenchBox Claude Code Skills

This directory contains custom skills for Claude Code to streamline BenchBox development workflows.

## Available Skills

### 1. **benchmark-test**
Run benchmark-specific tests quickly. Automatically detects which benchmark tests to run based on user request.

**Usage:** "Test TPC-H", "Run the TPC-DS tests", "Test primitives benchmark"

### 2. **tpc-compliance-check**
Validate TPC benchmark implementations against official specifications. Checks queries, data generation, test structure, and binary integration.

**Usage:** "Check TPC-H compliance", "Validate TPC-DS against spec", "Review benchmark compliance"

### 3. **compare-implementations**
Compare benchmark implementations to identify inconsistencies and harmonization opportunities. Useful for maintaining consistency across benchmarks.

**Usage:** "Compare TPC-H and TPC-DS implementations", "Review architecture consistency"

### 4. **project-todo-sync**
Manage the PROJECT_TODO.yaml and PROJECT_DONE.yaml workflow. Track tasks, mark items complete, and maintain project organization.

**Usage:** "Mark X as done", "Add TODO for Y", "What's on the TODO list?"

### 5. **quick-quality-check**
Run comprehensive pre-commit quality checks (lint, format, typecheck, tests) to ensure CI will pass.

**Usage:** "Run quality checks", "Check if ready to commit", "Verify code quality"

### 6. **dialect-translation-test**
Test SQL query translation across database dialects (DuckDB, Snowflake, BigQuery, Databricks, ClickHouse, etc.).

**Usage:** "Test dialect translation", "Verify query compatibility", "Check SQL across platforms"

### 7. **binary-wrapper-check**
Validate TPC binary integration and execution. Verify C binaries are working correctly for TPC-H and TPC-DS.

**Usage:** "Check TPC binaries", "Verify binary integration", "Debug query generation"

### 8. **architecture-review**
Review code architecture against BenchBox patterns and best practices. Ensures consistency with established patterns.

**Usage:** "Review architecture", "Check if follows patterns", "Validate code structure"

### 9. **live-platform-test**
Run integration tests against real cloud platforms (Databricks, Snowflake, BigQuery). Includes credential checking and cost awareness.

**Usage:** "Test on Databricks", "Run live platform tests", "Verify cloud compatibility"

### 10. **benchmark-plan-and-execute**
Complete workflow from planning to implementation to testing. Handles major features, new benchmarks, and significant changes.

**Usage:** "Implement X feature", "Create new benchmark", "Add Y functionality"

## How to Use

Skills are automatically available to Claude Code. Simply describe what you want to do in natural language, and Claude will invoke the appropriate skill.

### Examples

```
You: "Test TPC-H"
→ Invokes benchmark-test skill

You: "Check if TPC-DS is compliant with the spec"
→ Invokes tpc-compliance-check skill

You: "Run quality checks before committing"
→ Invokes quick-quality-check skill

You: "Implement a new ClickHouse adapter"
→ Invokes benchmark-plan-and-execute skill
```

## Skill Development

Each skill is defined in a markdown file with:
- YAML frontmatter with `description`
- Detailed instructions for Claude
- Examples and usage patterns
- Common issues and solutions
- Notes and best practices

## Benefits

These skills provide:
- **Consistency**: Follow established patterns automatically
- **Efficiency**: Common workflows automated
- **Quality**: Built-in quality checks and validation
- **Documentation**: Self-documenting workflows
- **Guidance**: New contributors can leverage expertise

## Customization

To modify a skill:
1. Edit the corresponding `.md` file
2. Changes take effect immediately
3. No restart required

To add a new skill:
1. Create new `.md` file in this directory
2. Add YAML frontmatter with description
3. Write detailed instructions
4. Test with natural language requests

## Notes

- Skills are invoked automatically by Claude based on context
- Multiple skills can be used together for complex workflows
- Skills complement but don't replace manual commands
- All skills follow BenchBox coding standards and patterns

## Related Documentation

- `CLAUDE.md` - BenchBox-specific guidance for Claude Code
- `_project/PROJECT_TODO.yaml` - Current project tasks
- `Makefile` - Available commands and test targets
- `pyproject.toml` - Project configuration

---

Created: 2025-10-23
Last Updated: 2025-10-23
