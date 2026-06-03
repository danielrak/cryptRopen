# Dashboard summary of an asynchronous `crypt_r()` job

Companion to [`print()`](https://rdrr.io/r/base/print.html). Returns a
structured list (class `summary.cryptR_job`) carrying the aggregates a
monitoring script typically needs: total task count, per-state counts,
elapsed seconds, active workers, total rows processed (sum over resolved
tasks — excludes NA), output path, and the full per-task status
dataframe.

## Usage

``` r
# S3 method for class 'cryptR_job'
summary(object, ...)
```

## Arguments

- object:

  A `cryptR_job` object.

- ...:

  Ignored.

## Value

An object of class `summary.cryptR_job` — a named list with elements
`n_tasks`, `counts`, `elapsed_sec`, `n_workers`, `total_rows`,
`output_path`, `log_written`, `status` (the full
[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md)
dataframe).

## Details

The object has its own [`print()`](https://rdrr.io/r/base/print.html)
method that renders a compact dashboard.

## See also

[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md),
[`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md).

Other async_job:
[`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md),
[`cryptR_results()`](https://danielrak.github.io/cryptRopen/reference/cryptR_results.md),
[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md),
[`cryptR_wait()`](https://danielrak.github.io/cryptRopen/reference/cryptR_wait.md),
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)

## Examples

``` r
# \donttest{
# Build a tiny job (see ?crypt_r for a more detailed walkthrough)
work_dir <- file.path(tempdir(), "cryptR_summary_example")
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
summary(job)
#> <cryptR_job summary>
#>   tasks        : 1
#>     running    : 0
#>     done       : 1
#>     failed     : 0
#>   workers      : 1
#>   elapsed      : 1.11 s
#>   rows total   : 10
#>   output_path  : /tmp/RtmpoVdRJG/cryptR_summary_example/output
#>   log_written  : TRUE

unlink(work_dir, recursive = TRUE)
# }
```
