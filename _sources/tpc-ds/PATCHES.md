# TPC-DS Source Patches

This document describes patches applied to the TPC-DS dsdgen/dsqgen source code in BenchBox.

## Patch: Stdout Data Generation Support (-FILTER Y flag)

**Date Applied:** 2026-01-15
**Original Fix By:** Greg Rahn (gregrahn)
**Reference:** tpcds-kit (https://github.com/gregrahn/tpcds-kit) commit 7992dbb

### Problem

The upstream TPC-DS dsdgen source code has broken stdout support due to two bugs
that were introduced when official TPC specification imports (v2.10.0, v4.0.0)
reverted community fixes. This prevented efficient workflows like:

- Direct piping to compression (zstd, gzip)
- Memory-efficient streaming pipelines
- Reduced disk I/O during data generation

**Observed behavior before fix:**
- `-FILTER Y` → "ERROR: option 'FILTER' unknown"
- `-_FILTER Y` → Creates file instead of stdout output (fpOutfile overwritten)

### Solution

Port the stdout output fix from tpcds-kit to the BenchBox TPC-DS source. This
enables the `-FILTER Y` flag to output generated data to stdout instead of files.

### Changes

#### 1. params.h - Fix parameter name (Bug #1)

The help text showed `_FILTER` but the code called `is_set("FILTER")` without
the underscore prefix. Since `fnd_param()` uses prefix matching, "FILTER" never
matched "_FILTER".

**File:** `_sources/tpc-ds/tools/params.h` line 64

```c
// Before (broken):
{"_FILTER",     OPT_FLG,            20, "output data to stdout", NULL, "N"},

// After (fixed):
{"FILTER",      OPT_FLG,            20, "output data to stdout", NULL, "N"},
```

#### 2. print.c - Fix fpOutfile overwrite (Bug #2)

When FILTER was set, line 449 correctly set `fpOutfile = stdout`, but line 485
unconditionally overwrote it with `fpOutfile = pTdef->outfile` (which is NULL),
causing "Failed to open output file!" errors.

**File:** `_sources/tpc-ds/tools/print.c` lines 480-486

```c
// Before (broken):
#endif
       }
   }

   fpOutfile = pTdef->outfile;  // Overwrites stdout!
   res = (fpOutfile != NULL);

// After (fixed):
#endif
       }
      fpOutfile = pTdef->outfile;  // Moved inside else block
   }

   res = (fpOutfile != NULL);
```

### Usage

```bash
# Generate ship_mode table to stdout (can be piped to compression)
./dsdgen -TABLE ship_mode -SCALE 1 -FILTER Y | zstd > ship_mode.dat.zst

# Generate date_dim to stdout with fixed seed for reproducibility
./dsdgen -TABLE date_dim -SCALE 1 -FILTER Y -RNGSEED 12345 > date_dim.dat

# Verify stdout output works
./dsdgen -TABLE ship_mode -SCALE 1 -FILTER Y -RNGSEED 1 | head -5
```

### Limitations

- The `-FILTER Y` flag outputs data to stdout; use shell redirection or piping
- When using `-FILTER Y`, no `.dat` file is created in the output directory
- For parallel generation, run multiple dsdgen processes with different table names

### Verification

After applying patches and recompiling:

```bash
# Verify FILTER flag appears in help (not _FILTER)
./dsdgen -help 2>&1 | grep FILTER
# Expected: "FILTER" without underscore prefix

# Test stdout output produces data
./dsdgen -TABLE ship_mode -SCALE 1 -FILTER Y -RNGSEED 1 | wc -l
# Expected: 20 (ship_mode has 20 rows at SF1)

# Verify output matches file-based generation
./dsdgen -TABLE ship_mode -SCALE 1 -FILTER Y -RNGSEED 1 > /tmp/stdout.dat
./dsdgen -TABLE ship_mode -SCALE 1 -RNGSEED 1 -DIR /tmp -FORCE
diff /tmp/stdout.dat /tmp/ship_mode.dat
# Expected: No differences
```

---

## Patch: Linux Build Compatibility

**Date Applied:** 2026-01-15

### Problem

The TPC-DS source failed to compile on Linux due to:
1. Missing `MAXINT` definition (not in glibc, only in some BSD headers)
2. GCC 10+ changed default from `-fcommon` to `-fno-common`, causing multiple
   definition errors for tentative definitions

### Solution

Add Linux-specific compatibility fixes to enable clean compilation.

### Changes

#### 1. porting.h - Add MAXINT definition for Linux

**File:** `_sources/tpc-ds/tools/porting.h` lines 120-122

```c
#ifdef LINUX
#define MAXINT INT_MAX
#endif
```

Location: After the existing `#ifdef MACOS` block for MAXINT

#### 2. makefile - Add -fcommon for GCC 10+ compatibility

**File:** `_sources/tpc-ds/tools/makefile` line 59

```make
# Before:
LINUX_CFLAGS    = -g -Wall

# After:
LINUX_CFLAGS    = -g -Wall -fcommon
```

### Verification

```bash
# Clean build on Linux
cd _sources/tpc-ds/tools
make clean
make

# Verify binaries were created
ls -la dsdgen dsqgen
```

---

## Attribution

The stdout fix was originally implemented by **Greg Rahn** ([@gregrahn](https://github.com/gregrahn))
in the [tpcds-kit](https://github.com/gregrahn/tpcds-kit) repository (commit 7992dbb, 2013).

The fix was lost when official TPC specification imports (v2.10.0 for tpcds-kit,
v4.0.0 for BenchBox) replaced the patched source files with upstream versions
that contained the original bugs.

BenchBox re-applied these fixes based on analysis of the tpcds-kit commit history
and verification of the root cause through testing.

---

## Applying the Patch

To apply these changes to a fresh TPC-DS source distribution:

```bash
# From the TPC-DS source root directory (containing tools/)
patch -p1 < stdout-support.patch

# Rebuild
cd tools
make
```

The patch file `stdout-support.patch` is provided in the same directory as this document.

**Note:** The patch uses unified diff format (`-p1` strips the leading `a/` or `b/` prefix).

---

## Related Files

- `_sources/tpc-h/PATCHES.md` - Similar patches for TPC-H dbgen
- `_project/DONE/tpcds-stdout-fix/` - Implementation TODO items
- `tests/unit/core/tpcds/test_stdout_datagen.py` - Regression tests
