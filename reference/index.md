# Package index

## Encrypt at the vector level

- [`crypt_vector()`](https://danielrak.github.io/cryptRopen/reference/crypt_vector.md)
  : Hash a Vector with a Salted Pre-Image

## Encrypt in-session

- [`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md)
  : Pseudonymize Variables in a Data Frame
- [`get_correspondence_tables()`](https://danielrak.github.io/cryptRopen/reference/get_correspondence_tables.md)
  : Retrieve Correspondence Tables from the Current Session

## Encrypt across many files

The mask-driven workflow.
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
dispatches one task per mask row in parallel and returns immediately;
the companions inspect / wait / finalize the job.

- [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
  : Pseudonymize Variables Across Multiple Files via an Excel Mask

- [`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md)
  :

  Inspect the state of an asynchronous
  [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
  job

- [`cryptR_wait()`](https://danielrak.github.io/cryptRopen/reference/cryptR_wait.md)
  :

  Block until an asynchronous
  [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
  job finishes

- [`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md)
  :

  Finalize an asynchronous
  [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
  job

- [`cryptR_results()`](https://danielrak.github.io/cryptRopen/reference/cryptR_results.md)
  :

  Per-output disk-oriented view of an async
  [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
  job

- [`summary(`*`<cryptR_job>`*`)`](https://danielrak.github.io/cryptRopen/reference/summary.cryptR_job.md)
  :

  Dashboard summary of an asynchronous
  [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
  job

- [`print(`*`<cryptR_job>`*`)`](https://danielrak.github.io/cryptRopen/reference/print.cryptR_job.md)
  :

  Compact Print Method for a `cryptR_job`

- [`print(`*`<summary.cryptR_job>`*`)`](https://danielrak.github.io/cryptRopen/reference/print.summary.cryptR_job.md)
  :

  Compact Print Method for a `summary.cryptR_job`

## Helpers

- [`inspect()`](https://danielrak.github.io/cryptRopen/reference/inspect.md)
  : Profile a Data Frame
