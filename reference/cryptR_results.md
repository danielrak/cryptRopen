# Per-output disk-oriented view of an async `crypt_r()` job

Returns one row per filtered mask row with (a) the expected output file
path, (b) whether it currently exists on disk, and (c) the size/hash the
worker captured right after writing it. Companion to
[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md)
— the two views are orthogonal and join cleanly on `encrypted_file`.

## Usage

``` r
cryptR_results(job)
```

## Arguments

- job:

  A `cryptR_job` object, as returned by
  [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md).

## Value

A data.frame with columns:

- `encrypted_file` (character): row identifier (dedup'd file name).

- `output_file_path` (character): full path where the output is / would
  be written.

- `exists` (logical): result of `file.exists(output_file_path)` at call
  time.

- `size_bytes` (numeric): size recorded by the worker post-write, `NA`
  when the task has not produced a payload yet.

- `sha256` (character): sha256 of the output at worker-completion time,
  `NA` otherwise.

- `success` (logical): whether the engine reported a clean end.

- `error_message` (character): concatenated engine errors, `NA` on
  success.

## Details

Running tasks contribute a row with `exists = FALSE` and NA for
`size_bytes` / `sha256`. Tasks that failed before reaching the final
export contribute a row with `success = FALSE`, `exists` reflecting the
live file state (which may still be `FALSE` because the engine
short-circuited), and `error_message` set.

The `size_bytes` / `sha256` values are the ones the engine recorded
inside the worker. If the output file has been modified or removed
between then and the call to `cryptR_results()`, those columns do
**not** reflect that — they describe what was produced, not what is
currently on disk. Use
[`file.info()`](https://rdrr.io/r/base/file.info.html) /
[`digest::digest()`](https://eddelbuettel.github.io/digest/man/digest.html)
for live measurements when needed.

## See also

[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md),
[`summary.cryptR_job()`](https://danielrak.github.io/cryptRopen/reference/summary.cryptR_job.md).

Other async_job:
[`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md),
[`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md),
[`cryptR_wait()`](https://danielrak.github.io/cryptRopen/reference/cryptR_wait.md),
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md),
[`summary.cryptR_job()`](https://danielrak.github.io/cryptRopen/reference/summary.cryptR_job.md)

## Examples

``` r
# \donttest{
# Build a tiny job (see ?crypt_r for a more detailed walkthrough)
work_dir <- file.path(tempdir(), "cryptR_results_example")
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
cryptR_results(job)
#>      encrypted_file
#> 1 persons_crypt.csv
#>                                                  output_file_path exists
#> 1 /tmp/RtmpoVdRJG/cryptR_results_example/output/persons_crypt.csv   TRUE
#>   size_bytes                                                           sha256
#> 1        738 9dceb0aa9241af79f603dad25444dfaab3e586837040544192296f349c01e51d
#>   success error_message
#> 1    TRUE          <NA>

unlink(work_dir, recursive = TRUE)
# }
```
