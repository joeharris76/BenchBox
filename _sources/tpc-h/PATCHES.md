# TPC-H Source Patches

This document describes patches applied to the TPC-H dbgen source code in BenchBox.

## Patch: Stdout Data Generation Support (-z flag)

**Date Applied:** 2026-01-15
**Reference:** tpch-kit (https://github.com/gregrahn/tpch-kit)

### Problem

The upstream TPC-H dbgen source code does not support streaming output to stdout,
requiring all data generation to go through intermediate files on disk. This prevents
efficient workflows like:
- Direct piping to compression (zstd, gzip)
- Memory-efficient streaming pipelines
- Reduced disk I/O during data generation

### Solution

Port the stdout output capability from tpch-kit to the BenchBox TPC-H source.
This adds a `-z` flag that outputs generated data to stdout instead of files.

### Changes

#### 1. dss.h - Add variable declaration

Add `EXTERN int zstdout;` to declare the global variable that controls stdout mode.

```c
EXTERN char *d_path;
EXTERN int  zstdout;   /* <-- Added */
```

Location: After line 281 (after `d_path` declaration)

#### 2. print.c - Add stdout conditional in print_prep()

Modify `print_prep()` to return stdout when `zstdout` is set, instead of opening a file.

```c
if (zstdout)
{
    return(stdout);
}
else
{
    res = tbl_open(table, "w");
    OPEN_CHECK(res, tdefs[table].name);
    return(res);
}
```

Location: Replace lines 97-99 in `print_prep()` function

#### 3. print.c - Fix money format specifier bug

Fix incorrect format specifier for money values. The `dollars` and `cents` variables
are `int` type but were using `%ld` format which is undefined behavior.

```c
// Before (incorrect):
fprintf(target, "%ld.%02ld", dollars, cents);

// After (correct):
fprintf(target, "%d.%02d", dollars, cents);
```

Location: Line 138 in `dbg_print()` function, `DT_MONEY` case

#### 4. driver.c - Add -z flag handling

Three changes to add command-line support for the `-z` flag:

**a. Add help text (line 395):**
```c
fprintf (stderr, "-z     -- output to stdout\n");
```

**b. Add 'z' to getopt string (line 454):**
```c
// Before:
"b:C:d:fi:hO:P:qs:S:T:U:v"

// After:
"b:C:d:fi:hO:P:qs:S:T:U:vz"
```

**c. Add case handler (lines 525-527):**
```c
case 'z':               /* output to stdout */
    zstdout = 1;
    break;
```

### Usage

```bash
# Generate customer table to stdout (can be piped to compression)
./dbgen -z -s 1 -T c | zstd > customer.tbl.zst

# Generate all tables to stdout (one at a time)
./dbgen -z -s 1 -T c > customer.tbl
./dbgen -z -s 1 -T s > supplier.tbl
# etc.
```

### Limitations

- The `-z` flag is incompatible with `-C` (chunks) - each chunk must be generated separately
- When using `-z`, only one table can be generated per invocation (use `-T` flag)
- For parallel generation, run multiple dbgen processes with different `-T` flags

### Verification

After applying patches and recompiling:

```bash
# Verify -z flag appears in help
./dbgen -h 2>&1 | grep -- "-z"

# Test stdout output
./dbgen -z -s 0.01 -T c | head -5

# Verify output matches file-based generation
./dbgen -z -s 0.01 -T c > /tmp/stdout.tbl
./dbgen -s 0.01 -T c
diff /tmp/stdout.tbl customer.tbl
```

---

## Applying the Patch

To apply these changes to a fresh TPC-H source distribution:

```bash
# From the TPC-H source root directory (containing dbgen/)
patch -p1 < stdout-support.patch

# Rebuild
make -f makefile.suite
```

The patch file `stdout-support.patch` is provided in the same directory as this document.

**Note:** The patch uses unified diff format (`-p1` strips the leading `a/` or `b/` prefix).
