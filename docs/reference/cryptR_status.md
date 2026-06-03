# Inspect the state of an asynchronous `crypt_r()` job

Returns a one-row-per-task snapshot of the job's progress. The result is
taken *at the time of the call*: a task resolved between two successive
calls will flip from `running` to `done`/`failed`, but the function
itself has no side effects (no log writing, no waiting).

## Usage

``` r
cryptR_status(job)
```

## Arguments

- job:

  A `cryptR_job` object, as returned by
  [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md).

## Value

A data.frame with columns `encrypted_file` (character), `state` (factor:
`running` / `done` / `failed`), `error_message` (character, `NA` unless
`state == "failed"`), `start_time` / `end_time` (POSIXct, `NA` until the
task resolves), `duration_sec` (numeric seconds, `NA` until the task
resolves), `n_rows_processed` (integer, `NA` until the task resolves and
the engine reports a row count).

## Details

The snapshot also carries per-row metrics (`start_time`, `end_time`,
`duration_sec`, `n_rows_processed`) for resolved tasks — populated from
the task payload shipped back by the engines (`.make_row_result()`).
Metrics are `NA` for tasks still `running` or that errored before
producing a payload; this lets you monitor a run in-flight without
reading the `log_crypt_r_*.xlsx` file from disk.

## See also

[`cryptR_wait()`](https://danielrak.github.io/cryptRopen/reference/cryptR_wait.md),
[`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md),
[`summary.cryptR_job()`](https://danielrak.github.io/cryptRopen/reference/summary.cryptR_job.md).

Other async_job:
[`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md),
[`cryptR_results()`](https://danielrak.github.io/cryptRopen/reference/cryptR_results.md),
[`cryptR_wait()`](https://danielrak.github.io/cryptRopen/reference/cryptR_wait.md),
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md),
[`summary.cryptR_job()`](https://danielrak.github.io/cryptRopen/reference/summary.cryptR_job.md)

## Examples

``` r
# \donttest{
# Build a tiny job (see ?crypt_r for a more detailed walkthrough)
work_dir <- file.path(tempdir(), "cryptR_status_example")
mask_dir <- file.path(work_dir, "mask")
out_dir  <- file.path(work_dir, "output")
int_dir  <- file.path(work_dir, "intermediate")
for (d in c(mask_dir, out_dir, int_dir)) {
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
}

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

job <- crypt_r(
  mask_folder_path  = mask_dir,
  mask_file         = "mask.xlsx",
  output_path       = out_dir,
  intermediate_path = int_dir,
  encryption_key    = "demo-key",
  n_workers         = 1L
)
cryptR_status(job)
#>                      encrypted_file   state error_message start_time end_time
#> persons_crypt.csv persons_crypt.csv running          <NA>       <NA>     <NA>
#>                   duration_sec n_rows_processed
#> persons_crypt.csv           NA               NA

cryptR_collect(job)
unlink(work_dir, recursive = TRUE)
# }
```
