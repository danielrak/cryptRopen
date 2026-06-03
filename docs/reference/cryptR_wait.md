# Block until an asynchronous `crypt_r()` job finishes

Polls every task in `job` and returns once they are all resolved (either
`done` or `failed`). A failed task does *not* raise an exception here —
its state is simply visible via
[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md).

## Usage

``` r
cryptR_wait(job, timeout = Inf, poll_interval = 0.1)
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

`invisible(job)`.

## See also

[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md),
[`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md).

Other async_job:
[`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md),
[`cryptR_results()`](https://danielrak.github.io/cryptRopen/reference/cryptR_results.md),
[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md),
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md),
[`summary.cryptR_job()`](https://danielrak.github.io/cryptRopen/reference/summary.cryptR_job.md)

## Examples

``` r
# \donttest{
# Build a tiny job (see ?crypt_r for a more detailed walkthrough)
work_dir <- file.path(tempdir(), "cryptR_wait_example")
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
cryptR_wait(job, timeout = 60)

cryptR_collect(job)
unlink(work_dir, recursive = TRUE)
# }
```
