# cryptRopen: Industrialized Pseudonymization of Variables Across Datasets

Deterministic, salted-hash pseudonymization of one or many datasets,
driven by a single Excel mask that lists, per input file, which columns
to hash and which to drop.

## Where to start

Three entry points cover the typical needs:

- [`crypt_vector()`](https://danielrak.github.io/cryptRopen/reference/crypt_vector.md)
  — hash one vector.

- [`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md)
  — pseudonymize one data frame already in memory; the in-session
  workflow.

- [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
  — pseudonymize N files described by an Excel mask, in parallel,
  non-blocking; the industrialized workflow. Inspect / wait / finalize
  the returned job with
  [`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md),
  [`cryptR_wait()`](https://danielrak.github.io/cryptRopen/reference/cryptR_wait.md),
  [`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md),
  [`cryptR_results()`](https://danielrak.github.io/cryptRopen/reference/cryptR_results.md),
  and [`summary()`](https://rdrr.io/r/base/summary.html).

Use
[`get_correspondence_tables()`](https://danielrak.github.io/cryptRopen/reference/get_correspondence_tables.md)
to retrieve the `(original, hashed)` mapping stored after
[`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md)
/
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
runs.

## Learn more

- [`vignette("cryptRopen")`](https://danielrak.github.io/cryptRopen/articles/cryptRopen.md)
  — getting started.

- [`vignette("crypt_r-workflow")`](https://danielrak.github.io/cryptRopen/articles/crypt_r-workflow.md)
  — industrialized walkthrough.

- `NEWS.md` — change history.

## See also

Useful links:

- <https://danielrak.github.io/cryptRopen/>

- <https://github.com/danielrak/cryptRopen>

- Report bugs at <https://github.com/danielrak/cryptRopen/issues>

## Author

**Maintainer**: Daniel Rakotomalala <rakdanielh@gmail.com>
