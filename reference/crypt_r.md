# Pseudonymize Variables Across Multiple Files via an Excel Mask

The high-level entry point for the batch / industrialized workflow.
Reads a single Excel mask that describes, for each input file, which
columns to hash and which to drop; dispatches one `mirai` task per
filtered mask row (parallel, non-blocking); and returns immediately a
`cryptR_job` handle. The job is inspected with
[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md)
/ [`summary()`](https://rdrr.io/r/base/summary.html) /
[`cryptR_results()`](https://danielrak.github.io/cryptRopen/reference/cryptR_results.md),
blocked on with
[`cryptR_wait()`](https://danielrak.github.io/cryptRopen/reference/cryptR_wait.md),
and finalized with
[`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md)
(which writes a recap xlsx log and tears down daemons when applicable).

## Usage

``` r
crypt_r(
  mask_folder_path,
  mask_file,
  output_path,
  intermediate_path,
  encryption_key,
  algorithm = "md5",
  correspondence_table = TRUE,
  engine = c("auto", "in_memory", "streaming"),
  chunk_size = 1000000L,
  n_workers = NULL
)
```

## Arguments

- mask_folder_path:

  Character scalar. Directory containing the Excel mask file.

- mask_file:

  Character scalar. Name of the Excel mask file (with extension).

- output_path:

  Character scalar. Existing directory where the pseudonymized output
  files will be written. Validated up-front.

- intermediate_path:

  Character scalar. Existing directory where the correspondence-table
  parquets (`tc_*.parquet`) and the recap xlsx log will be written.
  Validated up-front.

- encryption_key:

  Character scalar. The salt prepended to each value before hashing. See
  [`crypt_vector()`](https://danielrak.github.io/cryptRopen/reference/crypt_vector.md)
  for the underlying transformation.

- algorithm:

  Character scalar. Any algorithm accepted by the `algo` argument of
  [`digest::digest()`](https://eddelbuettel.github.io/digest/man/digest.html).
  Defaults to `"md5"`.

- correspondence_table:

  Logical scalar. If `TRUE` (default), a per-input correspondence table
  `tc_<stem>.parquet` is written under `intermediate_path` and also
  stored in the package-private environment (retrievable via
  [`get_correspondence_tables()`](https://danielrak.github.io/cryptRopen/reference/get_correspondence_tables.md)
  after
  [`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md)
  re-injects them post-run).

- engine:

  One of `"auto"`, `"in_memory"`, `"streaming"`. Selects the per-row
  processing engine. `"auto"` (default) and `"streaming"` both route
  parquet-in/parquet-out and csv-in/csv-out to the streaming engines (an
  `'arrow'` Scanner by chunks, with progressive write); mixed or
  non-streamable endpoints (rds, xlsx, parquet→csv, csv→parquet)
  silently fall back to `"in_memory"`. `"in_memory"` always reads the
  full input into RAM.

- chunk_size:

  Integer scalar. Number of rows per chunk when the effective engine is
  streaming. Ignored by `"in_memory"`. Defaults to `1e6`.

- n_workers:

  Integer scalar or `NULL`. Number of `mirai` daemons to spawn for the
  dispatch. When `NULL` (default), `crypt_r()` picks
  `min(parallel::detectCores() - 1, n_rows, 8L)` (floored at 1). If
  daemons are already running on the default profile when `crypt_r()` is
  called, `n_workers` is ignored and the existing daemons are reused —
  the user retains control and
  [`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md)
  will not tear them down.

## Value

A `cryptR_job` object carrying one `mirai` task per filtered mask row.
Inspect with
[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md)
/ [`summary()`](https://rdrr.io/r/base/summary.html) /
[`cryptR_results()`](https://danielrak.github.io/cryptRopen/reference/cryptR_results.md);
block with
[`cryptR_wait()`](https://danielrak.github.io/cryptRopen/reference/cryptR_wait.md);
finalize with
[`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md).

## Details

A per-row failure does not interrupt the other rows — the failure is
captured on the corresponding task and surfaces via
[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md).

**Empty `vars_to_encrypt` cell.** A mask row whose `vars_to_encrypt`
cell is blank (empty, `NA`, or whitespace-only) is legitimate and means
"process this file, applying `vars_to_remove` if any, hashing nothing."
The output file is written under `output_path` re-encoded to the format
implied by `encrypted_file`; no `_crypt` columns are emitted, and **no
`tc_*.parquet`** is produced. The recap log records `success = TRUE` and
`tc_name = NA` for such rows.
[`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md)
does **not** support this case — see its documentation.

## See also

[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md),
[`cryptR_wait()`](https://danielrak.github.io/cryptRopen/reference/cryptR_wait.md),
[`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md),
[`cryptR_results()`](https://danielrak.github.io/cryptRopen/reference/cryptR_results.md),
[`get_correspondence_tables()`](https://danielrak.github.io/cryptRopen/reference/get_correspondence_tables.md).

Other async_job:
[`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md),
[`cryptR_results()`](https://danielrak.github.io/cryptRopen/reference/cryptR_results.md),
[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md),
[`cryptR_wait()`](https://danielrak.github.io/cryptRopen/reference/cryptR_wait.md),
[`summary.cryptR_job()`](https://danielrak.github.io/cryptRopen/reference/summary.cryptR_job.md)

## Examples

``` r
# \donttest{
# Minimal end-to-end run using the persons.csv fixture shipped
# with the package. Everything is written to tempdir() and cleaned
# up at the end.
work_dir <- file.path(tempdir(), "crypt_r_example")
mask_dir <- file.path(work_dir, "mask")
out_dir  <- file.path(work_dir, "output")
int_dir  <- file.path(work_dir, "intermediate")
dir.create(mask_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(out_dir, showWarnings = FALSE)
dir.create(int_dir, showWarnings = FALSE)

input_file <- system.file("extdata", "persons.csv", package = "cryptRopen")
mask <- data.frame(
  folder_path     = dirname(input_file),
  file            = basename(input_file),
  encrypted_file  = "persons_crypt.csv",
  vars_to_encrypt = "email",
  vars_to_remove  = NA,
  to_encrypt      = "X",
  stringsAsFactors = FALSE
)
writexl::write_xlsx(mask, file.path(mask_dir, "mask.xlsx"))

# n_workers = 1L keeps the example friendly to CRAN check policies
# on parallelism in examples.
job <- crypt_r(
  mask_folder_path  = mask_dir,
  mask_file         = "mask.xlsx",
  output_path       = out_dir,
  intermediate_path = int_dir,
  encryption_key    = "demo-key",
  n_workers         = 1L
)
job <- cryptR_collect(job)

list.files(out_dir)
#> [1] "inspect_persons_crypt.csv.xlsx"   "log_crypt_r_20260603_212212.xlsx"
#> [3] "persons_crypt.csv"               
unlink(work_dir, recursive = TRUE)
# }
```
