---
description: Validate TPC binary integration and execution
---

# Binary Wrapper Check

Verify that TPC C binaries are properly integrated and functioning correctly.

## Instructions

When the user asks to check binary integration, verify TPC binaries, or debug generation issues:

1. **Check binary locations**:

   ```bash
   # TPC-H binaries
   ls -la _binaries/tpc-h/*/

   # TPC-DS binaries
   ls -la _binaries/tpc-ds/*/

   # Look for platform-specific builds:
   # - darwin-arm64 (macOS Apple Silicon)
   # - darwin-x86_64 (macOS Intel)
   # - linux-x86_64 (Linux 64-bit)
   # - linux-arm64 (Linux ARM)
   # - windows-x86_64 (Windows 64-bit)
   ```

2. **Verify binary executables**:

   **TPC-H:** `dbgen` (data generation), `qgen` (query generation), templates in `queries/` or `variants/`

   **TPC-DS:** `dsdgen` (data generation), `dsqgen` (query generation), templates in `query_templates/`

3. **Test binary execution**:

   **TPC-H Query Generation:**
   ```bash
   # Set DSS_QUERY environment variable to templates location
   env DSS_QUERY=_sources/tpc-h/dbgen/queries \
     _binaries/tpc-h/darwin-arm64/qgen -s 1 -d 1

   # For variant 15a
   env DSS_QUERY=_sources/tpc-h/dbgen/variants \
     _binaries/tpc-h/darwin-arm64/qgen -a 15a
   ```

   **TPC-DS Query Generation:**
   ```bash
   # Generate query 1
   _binaries/tpc-ds/darwin-arm64/dsqgen \
     -TEMPLATE query1.tpl \
     -DIRECTORY _sources/tpc-ds/query_templates \
     -DIALECT netezza \
     -RNGSEED 1 \
     -FILTER Y

   # Generate with multiple variants
   _binaries/tpc-ds/darwin-arm64/dsqgen \
     -TEMPLATE query14.tpl \
     -DIRECTORY _sources/tpc-ds/query_templates \
     -DIALECT netezza \
     -RNGSEED 1 \
     -COUNT 2 \
     -FILTER Y
   ```

4. **Check Python wrapper integration**:

   **TPC-H:** Read `benchbox/core/tpch/c_tools.py` - check `_execute_qgen()` method, environment variable handling, and error handling.

   **TPC-DS:** Read `benchbox/core/tpcds/c_tools.py` - check `_execute_dsqgen()` method, parameter substitution, and template handling.

5. **Test through Python API**:

   ```python
   # Test TPC-H
   from benchbox.core.tpch import TPCH

   tpch = TPCH(scale_factor=1)
   query = tpch.get_query(query_num=1)
   print(query)

   # Test TPC-DS
   from benchbox.core.tpcds import TPCDS

   tpcds = TPCDS(scale_factor=1)
   query = tpcds.get_query(query_num=1)
   print(query)
   ```

6. **Common issues to check**:

   **Binary not found:** Wrong platform directory, binary not executable (`chmod +x`), incorrect path construction in wrapper.

   **Execution errors:** Missing environment variables (DSS_QUERY), wrong template directory path, missing template files, parameter substitution issues.

   **Output issues:** Encoding problems (UTF-8), newline handling, unsubstituted parameter values, query fragments instead of complete queries.

   **Platform-specific:** ARM vs x86 binary mismatch, macOS Gatekeeper blocking unsigned binaries, Linux library dependencies missing, Windows path separator issues.

7. **Report findings**:

   ```markdown
   ## Binary Integration Check

   ### Binary Availability
   ✅ TPC-H darwin-arm64: Present and executable
   ✅ TPC-DS darwin-arm64: Present and executable
   ❌ TPC-H linux-x86_64: Binary not found

   ### Execution Test Results

   **TPC-H Query 1:**
   ✅ Binary execution successful
   ✅ Query generated correctly
   ✅ Python wrapper integration working

   **TPC-DS Query 1:**
   ✅ Binary execution successful
   ⚠️  Template substitution incomplete
   ❌ Python wrapper missing error handling

   ### Issues Found

   1. **TPC-DS parameter substitution**
      - Location: benchbox/core/tpcds/c_tools.py:123
      - Issue: [NULLCOLSS] not being replaced
      - Impact: Some queries have unsubstituted placeholders

   2. **Missing Linux binaries**
      - Location: _binaries/tpc-h/linux-x86_64/
      - Issue: Directory exists but qgen binary missing
      - Impact: Cannot run on Linux systems

   ### Recommendations
   1. Fix parameter substitution in TPC-DS wrapper
   2. Compile or download missing Linux binaries
   3. Add better error messages when binaries not found
   ```

## Binary Sources

**Where binaries come from:**
- Compiled from official TPC sources in `_sources/tpc-{h,ds}/`
- Cross-compiled for multiple platforms
- Stored in `_binaries/` directory
- Should be pre-compiled for distribution

**To compile from source:**
```bash
# TPC-H
cd _sources/tpc-h/dbgen
make

# TPC-DS
cd _sources/tpc-ds/tools
make
```

## Platform Detection

BenchBox auto-detects platform using `platform.system().lower()` (darwin, linux, windows) and `platform.machine().lower()` (x86_64, arm64, aarch64), then maps to binary directory name `{system}-{machine}` and constructs path `_binaries/tpc-h/{platform_dir}/qgen`.

## Testing

Verify: binaries exist for current platform and are executable, direct binary execution works, environment variables set correctly, template files found, query generation produces valid SQL, parameter substitution works, Python wrapper calls binary correctly, error handling works, multi-platform support verified.

## Notes

- Binaries are platform-specific - need separate builds
- macOS may require security approval for unsigned binaries
- TPC specifications require using official C tools for compliance
- Python wrappers should be thin layers over C binaries
- Always test generated queries for completeness
