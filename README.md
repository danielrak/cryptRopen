
<!-- README.md is generated from README.Rmd. Please edit that file -->

# cryptRopen

<!-- badges: start -->

<!-- CRAN-MARKER: uncomment when the package is published on CRAN
[![CRAN status](https://www.r-pkg.org/badges/version/cryptRopen)](https://CRAN.R-project.org/package=cryptRopen)
-->

[![R-CMD-check](https://github.com/danielrak/cryptRopen/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/danielrak/cryptRopen/actions/workflows/R-CMD-check.yaml)
[![Codecov test
coverage](https://codecov.io/gh/danielrak/cryptRopen/graph/badge.svg)](https://app.codecov.io/gh/danielrak/cryptRopen)
<!-- badges: end -->

cryptRopen pseudonymizes variables in one or many datasets, driven by a
single Excel mask that lists, per input file, which columns to hash and
which to drop. The transformation is a **salted hash** (MD5 by default;
any algorithm supported by `digest::digest()` works), which means:

- The same input value paired with the same key always yields the same
  hash. Joins across tables on the pseudonymized columns stay possible.
- The hash is one-way: holding the output does not let anyone recover
  the input. Only the correspondence table (kept by the party that owns
  the key) does.

Suitable for GDPR-style pseudonymization. **Not** suitable as a
reversible encryption scheme for confidentiality-bearing data.

## Installation

<!-- CRAN-MARKER: uncomment when the package is published on CRAN
From CRAN:
&#10;``` r
install.packages("cryptRopen")
```
&#10;-->

Install the development version from GitHub:

``` r
# install.packages("pak")
pak::pak("danielrak/cryptRopen")
```

## In-session use: `crypt_data()`

Pseudonymize one data frame already in memory. The correspondence table
is kept in a package-private environment and retrieved with
`get_correspondence_tables()`:

``` r
library(cryptRopen)

pseudonymized <- crypt_data(
  loaded_dataset             = head(mtcars, 3),
  vars_to_encrypt            = "mpg",
  vars_to_remove             = "cyl",
  encryption_key             = "demo-key",
  correspondence_table       = TRUE,
  correspondence_table_label = "demo"
)
pseudonymized
#>                                      mpg_crypt disp  hp drat    wt  qsec vs am
#> Mazda RX4     C33FDD39F6EAADF6C5E135AD5083E3D6  160 110 3.90 2.620 16.46  0  1
#> Mazda RX4 Wag C33FDD39F6EAADF6C5E135AD5083E3D6  160 110 3.90 2.875 17.02  0  1
#> Datsun 710    7E7E52C52B1C3686F553D1E4AC6A8207  108  93 3.85 2.320 18.61  1  1
#>               gear carb
#> Mazda RX4        4    4
#> Mazda RX4 Wag    4    4
#> Datsun 710       4    1

get_correspondence_tables()$tc_crypt_demo
#>                mpg                        mpg_crypt
#> Mazda RX4     21.0 C33FDD39F6EAADF6C5E135AD5083E3D6
#> Mazda RX4 Wag 21.0 C33FDD39F6EAADF6C5E135AD5083E3D6
#> Datsun 710    22.8 7E7E52C52B1C3686F553D1E4AC6A8207
```

## Batch / file-driven use: `crypt_r()`

`crypt_r()` reads an Excel mask describing N input files, dispatches one
`mirai::mirai()` task per row (parallel, non-blocking), writes the
pseudonymized files and a recap xlsx log to disk, and returns a
`cryptR_job` handle. Inspect it with `cryptR_status()` / `summary()` /
`cryptR_results()` and finalize with `cryptR_collect()`.

``` r
job <- crypt_r(
  mask_folder_path  = "path/to/mask",
  mask_file         = "mask.xlsx",
  output_path       = "path/to/output",
  intermediate_path = "path/to/intermediate",
  encryption_key    = "your-salt",
  algorithm         = "md5"
)

cryptR_status(job)
job <- cryptR_collect(job)
```

Each row of the mask declares its own `vars_to_encrypt`, so different
input files can have entirely different column names — the mask is the
contract, not a single schema. Large parquet / CSV inputs are
automatically routed to a streaming engine (`arrow::open_dataset()` +
chunked write) so memory usage stays bounded regardless of input size.

## Learn more

- `vignette("cryptRopen")` — getting started: the in-session workflow
  with `crypt_vector()` / `crypt_data()`, the difference between
  pseudonymization and encryption, and how to pick an algorithm.
- `vignette("crypt_r-workflow")` — full walkthrough of the mask-driven
  `crypt_r()` workflow: Excel mask layout, async inspection, streaming,
  daemons ownership, recap log.
- `NEWS.md` — change history.

## License

MIT © Daniel Rakotomalala.
