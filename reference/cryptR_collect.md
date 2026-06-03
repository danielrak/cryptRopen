# Finalize an asynchronous `crypt_r()` job

Waits for all tasks to resolve (see
[`cryptR_wait()`](https://danielrak.github.io/cryptRopen/reference/cryptR_wait.md)),
extracts the per-row results, re-publishes the correspondence tables
into the parent's `.cryptRopen_env` (so
[`get_correspondence_tables()`](https://danielrak.github.io/cryptRopen/reference/get_correspondence_tables.md)
sees them after an async run), writes the recap log
`log_crypt_r_<timestamp>.xlsx` under `job$output_path`, and — when
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
created the mirai daemons itself (`daemons_owned_by_job = TRUE`) — tears
them down.

## Usage

``` r
cryptR_collect(job, timeout = Inf, poll_interval = 0.1)
```

## Arguments

- job:

  A `cryptR_job` object.

- timeout:

  Numeric. Maximum wait in seconds. Defaults to `Inf`. On expiration, an
  error of class `cryptR_timeout` is raised.

- poll_interval:

  Numeric. Polling period in seconds. Defaults to 0.1.

## Value

`invisible(job)` — with `log_written = TRUE` and, when applicable,
`daemons_torn_down = TRUE`. The modifications are applied to the
returned object only; S3 objects are not mutable in place in R, so
callers who want the updated flags must capture the return value
(`job <- cryptR_collect(job)`).

## Details

Idempotent on two independent axes:

- `job$log_written` guards the log-writing + TC re-injection block, so a
  manual `cryptR_collect()` called *after* the auto watcher has already
  run is a no-op — no duplicate xlsx, no TCs stored twice.

- `job$daemons_torn_down` guards the `mirai::daemons(0)` call so a
  second collect does not attempt a double teardown.

When daemons were set up externally before
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
was called, `daemons_owned_by_job` is `FALSE` and teardown is never
attempted — the user retains control of their own daemons.

## See also

[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md),
[`cryptR_wait()`](https://danielrak.github.io/cryptRopen/reference/cryptR_wait.md).

Other async_job:
[`cryptR_results()`](https://danielrak.github.io/cryptRopen/reference/cryptR_results.md),
[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md),
[`cryptR_wait()`](https://danielrak.github.io/cryptRopen/reference/cryptR_wait.md),
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md),
[`summary.cryptR_job()`](https://danielrak.github.io/cryptRopen/reference/summary.cryptR_job.md)

## Examples

``` r
# \donttest{
# Build a tiny job (see ?crypt_r for a more detailed walkthrough)
work_dir <- file.path(tempdir(), "cryptR_collect_example")
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
job <- cryptR_collect(job)

cryptR_status(job)
#>                      encrypted_file state error_message          start_time
#> persons_crypt.csv persons_crypt.csv  done          <NA> 2026-06-03 21:22:05
#>                              end_time duration_sec n_rows_processed
#> persons_crypt.csv 2026-06-03 21:22:06    0.7252164               10
unlink(work_dir, recursive = TRUE)
# }
```
