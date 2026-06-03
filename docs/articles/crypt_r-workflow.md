# Industrialized workflow with crypt_r()

[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
is the industrialized counterpart of
[`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md).
It is designed for production pipelines that must pseudonymize several
input files at once, write their encrypted versions to disk, and produce
a recap log — all in parallel, without blocking the main R session.

This vignette walks through a complete happy-path example and then
summarizes the main options.

``` r

library(cryptRopen)
```

## When to use `crypt_r()` vs. `crypt_data()`

| Need | Use |
|----|----|
| Pseudonymize one data frame already in memory | [`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md) |
| Pseudonymize N files on disk, described by an Excel mask | [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md) |
| Parallel execution across rows of the mask | [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md) |
| Streaming read/write for files too large for RAM (parquet, CSV) | [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md) |
| Auto-written xlsx recap log (success, duration, sha256 per row) | [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md) |

## The Excel mask

[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
is driven by a single Excel mask that describes what to encrypt and
where. Required columns:

| Column | Content |
|----|----|
| `folder_path` | Directory holding the input file. |
| `file` | Input file name (with extension). `rio`-readable formats supported. |
| `encrypted_file` | Output file name (with extension). Parquet or CSV recommended. |
| `vars_to_encrypt` | Comma-separated list of columns in the input file to hash. May be empty — see *Copy-only rows* below. |
| `vars_to_remove` | Comma-separated list of columns to drop from the output (may be empty). |
| `to_encrypt` | `"X"` to include the row in the run, anything else to skip it. |

Duplicated `encrypted_file` values are automatically disambiguated with
a `DUPL<n>_` prefix, so the output paths stay unique.

### Copy-only rows

A mask row whose `vars_to_encrypt` cell is **empty** (blank, `NA`, or
whitespace-only) is legitimate. It means “process this file, encrypting
nothing”:

- the input file is written to `output_path` re-encoded to the output
  format implied by `encrypted_file` (e.g. csv → csv, parquet →
  parquet);
- `vars_to_remove` is still applied, so an empty-vars row with a
  non-empty `vars_to_remove` is a clean “purge these columns and copy”
  instruction;
- no `_crypt` columns are emitted, no `tc_*.parquet` is written to
  `intermediate_path`, and the recap log records `success = TRUE` with
  `tc_name = NA` for the row.

[`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md)
does **not** support this: calling it without anything to encrypt is
treated as misuse and raises an explicit error pointing to `dplyr` /
`rio` for plain column dropping or format conversion.

## A complete example

The example below uses a 10-row CSV fixture shipped with the package
(`inst/extdata/persons.csv`). Every write happens inside
[`tempdir()`](https://rdrr.io/r/base/tempfile.html), so the example
leaves nothing behind on your machine.

``` r

input_file    <- system.file("extdata", "persons.csv", package = "cryptRopen")
input_folder  <- dirname(input_file)

work_dir           <- tempfile("cryptR_vignette_")
dir.create(work_dir)
mask_dir           <- file.path(work_dir, "mask")
output_dir         <- file.path(work_dir, "output")
intermediate_dir   <- file.path(work_dir, "intermediate")
dir.create(mask_dir)
dir.create(output_dir)
dir.create(intermediate_dir)
```

Build a minimal mask: one row, pseudonymising `email` and dropping
`joined_date`.

``` r

mask <- data.frame(
  folder_path     = input_folder,
  file            = basename(input_file),
  encrypted_file  = "persons_crypt.csv",
  vars_to_encrypt = "email",
  vars_to_remove  = "joined_date",
  to_encrypt      = "X",
  stringsAsFactors = FALSE
)
writexl::write_xlsx(mask, file.path(mask_dir, "mask.xlsx"))
```

Dispatch the job.
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
returns immediately, before the workers have finished; the value is a
`cryptR_job` handle.

``` r

job <- crypt_r(
  mask_folder_path  = mask_dir,
  mask_file         = "mask.xlsx",
  output_path       = output_dir,
  intermediate_path = intermediate_dir,
  encryption_key    = "vignette-key",
  algorithm         = "md5",
  n_workers         = 1L
)
```

``` r

class(job)
#> [1] "cryptR_job"
```

## Inspecting the job

Three orthogonal views are available at any time during or after the
run.

### `cryptR_status()` — per-task lifecycle

``` r

cryptR_wait(job)
cryptR_status(job)
#>                      encrypted_file state error_message          start_time
#> persons_crypt.csv persons_crypt.csv  done          <NA> 2026-06-03 23:10:37
#>                              end_time duration_sec n_rows_processed
#> persons_crypt.csv 2026-06-03 23:10:38    0.5005779               10
```

Columns: `encrypted_file`, `state` (`running` / `done` / `failed`),
`error_message`, plus `start_time`, `end_time`, `duration_sec`,
`n_rows_processed`. `NA` until the task resolves.

### `summary(job)` — dashboard

``` r

summary(job)
#> <cryptR_job summary>
#>   tasks        : 1
#>     running    : 0
#>     done       : 1
#>     failed     : 0
#>   workers      : 1
#>   elapsed      : 0.89 s
#>   rows total   : 10
#>   output_path  : C:\Users\rheri\AppData\Local\Temp\RtmpcL5VwW\cryptR_vignette_95c4354034fa/output
#>   log_written  : FALSE
```

Returns a compact object with task counts by state, elapsed seconds,
active workers, total rows processed, output path, and the full status
data frame under `$status`.

### `cryptR_results()` — disk-oriented view

``` r

cryptR_results(job)
#>      encrypted_file
#> 1 persons_crypt.csv
#>                                                                                            output_file_path
#> 1 C:\\Users\\rheri\\AppData\\Local\\Temp\\RtmpcL5VwW\\cryptR_vignette_95c4354034fa/output/persons_crypt.csv
#>   exists size_bytes
#> 1   TRUE        625
#>                                                             sha256 success
#> 1 60677c41e5332934f4ba5ab1d182ebbd6d84f7da71b31d4c22ac5ff6da92acec    TRUE
#>   error_message
#> 1          <NA>
```

One row per filtered mask row with the expected `output_file_path`, a
live `exists` flag, and the `size_bytes` / `sha256` the worker recorded
right after writing the output. Joins cleanly with
[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md)
on `encrypted_file`.

## Finalising the job

[`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md)
waits for the tasks to resolve (if you have not called
[`cryptR_wait()`](https://danielrak.github.io/cryptRopen/reference/cryptR_wait.md)
already), re-publishes the correspondence tables produced inside the
mirai daemons back into your session’s environment, writes the recap
log, and tears down any daemons
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
created for the run.

``` r

job <- cryptR_collect(job)
```

The correspondence table is now visible in the parent session:

``` r

tcs <- get_correspondence_tables()
names(tcs)
#> [1] "tc_persons_crypt"
head(tcs$tc_persons_crypt)
#> # A tibble: 6 × 2
#>   email         email_crypt                     
#>   <chr>         <chr>                           
#> 1 a@example.com AE0B96F635D0ECC0335AF23BF4AB5C65
#> 2 b@example.com 741FD2483C97A3D557657C01744CB69A
#> 3 c@example.com A8443E50BC83A0B19186165AD010811E
#> 4 d@example.com 2FE239E57F1C4D022160CD162AD0F88B
#> 5 e@example.com 1635B322A0FB470E3A2A5858748B67FD
#> 6 f@example.com EB7A394EA07ADFB7C3C76987D60DF855
```

The recap log is an xlsx file whose name embeds the run timestamp:

``` r

list.files(output_dir, pattern = "^log_crypt_r_.*\\.xlsx$")
#> [1] "log_crypt_r_20260603_231038.xlsx"
```

An auto-watcher (based on
[`later::later()`](https://later.r-lib.org/reference/later.html)) writes
the same log as soon as the last mirai task resolves, so a manual
[`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md)
call is not strictly required for the log to appear — it just becomes a
reliable synchronisation point.

## Options recap

| Parameter | Default | Purpose |
|----|----|----|
| `algorithm` | `"md5"` | Hashing algorithm. Anything [`digest::digest()`](https://eddelbuettel.github.io/digest/man/digest.html) accepts. |
| `correspondence_table` | `TRUE` | Produce the TC on disk (parquet) and in `.cryptRopen_env`. |
| `engine` | `"auto"` | `"auto"`, `"in_memory"`, or `"streaming"`. See routing table below. |
| `chunk_size` | `1e6` | Rows per chunk when the selected engine is streaming. Ignored otherwise. |
| `n_workers` | `NULL` | Mirai daemons to spawn. Default heuristic: `min(detectCores()-1, n_rows, 8L)`. |

### Engine routing

| `engine` | Input / output combination | Effective behavior |
|----|----|----|
| `"in_memory"` | any | Full read into RAM (historical) |
| `"auto"` / `"streaming"` | parquet-in + parquet-out | Streaming via `arrow` |
| `"auto"` / `"streaming"` | csv-in + csv-out | Streaming via `arrow` |
| `"auto"` / `"streaming"` | mixed, rds, xlsx, … | Falls back to in-memory |

In streaming mode the input is read by chunks with an `arrow` Scanner
and written incrementally, keeping memory usage bounded regardless of
input size. The `chunk_size` argument controls the row count per chunk;
the default (`1e6`) is a reasonable trade-off between memory footprint
and per-chunk overhead.

### Daemons ownership

If no mirai daemons are active on the default profile when
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
is called, it spawns `n_workers` of its own and flags the job for
automatic teardown by
[`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md).
If daemons are already running,
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
reuses them and
[`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md)
leaves them alone — useful when you wrap several
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
calls inside a broader parallel session you manage yourself.

### Error handling

A failure on one mask row never interrupts the others. The failure is
captured on the corresponding mirai task and surfaces as:

- `state == "failed"` in
  [`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md),
- a non-`NA` `error_message` column,
- `success = FALSE` in the xlsx recap log.

Invalid `output_path` or `intermediate_path` — paths that don’t exist,
or aren’t scalar non-empty character — are caught before any task is
dispatched, with a clear error.

## Cleanup

Nothing was written outside
[`tempdir()`](https://rdrr.io/r/base/tempfile.html), so the example
cleans itself up when the R session ends. You can also drop the folder
explicitly:

``` r

unlink(work_dir, recursive = TRUE)
```

## See also

- [`?crypt_r`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md),
  [`?cryptR_status`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md),
  [`?cryptR_wait`](https://danielrak.github.io/cryptRopen/reference/cryptR_wait.md),
  [`?cryptR_collect`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md),
  [`?cryptR_results`](https://danielrak.github.io/cryptRopen/reference/cryptR_results.md),
  [`?summary.cryptR_job`](https://danielrak.github.io/cryptRopen/reference/summary.cryptR_job.md).
- [`vignette("cryptRopen")`](https://danielrak.github.io/cryptRopen/articles/cryptRopen.md)
  for the in-session workflow with
  [`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md).
- `NEWS.md` for the full change history.
