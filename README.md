
<!-- README.md is generated from README.Rmd. Please edit that file -->

# cryptRopen

<!-- badges: start -->

[![Codecov test
coverage](https://codecov.io/gh/danielrak/cryptRopen/graph/badge.svg)](https://app.codecov.io/gh/danielrak/cryptRopen)
[![R-CMD-check](https://github.com/danielrak/cryptRopen/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/danielrak/cryptRopen/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

`cryptRopen` provides deterministic pseudonymisation of data frame
variables, driven by an Excel mask for industrialised runs. The
underlying transformation is a **salted hash** (MD5 by default; any
algorithm supported by `digest::digest()` works): same input paired with
same key always yields the same output, joins across tables on the
pseudonymised columns stay possible, and the hash itself is not
reversible. Suitable for GDPR-style pseudonymisation, not for
confidentiality-bearing encryption.

## Installation

Development version from GitHub:

``` r
# install.packages("pak")
pak::pak("danielrak/cryptRopen")
```

## In-session use: `crypt_data()`

Pseudonymise one data frame and keep the correspondence table in a
package-private environment, retrievable via
`get_correspondence_tables()`:

``` r
library(cryptRopen)

encrypted <- crypt_data(
  loaded_dataset            = head(mtcars, 3),
  vars_to_encrypt           = "mpg",
  vars_to_remove            = "cyl",
  encryption_key            = "demo-key",
  correspondence_table      = TRUE,
  correspondence_table_label = "demo"
)
encrypted
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

## Industrialised use: `crypt_r()`

`crypt_r()` reads an Excel mask describing N input files, dispatches one
`mirai::mirai()` task per row (parallel, non-blocking), writes the
encrypted files and a recap xlsx log to disk, and returns a `cryptR_job`
handle you can inspect with `cryptR_status()` / `summary()` /
`cryptR_results()` and finalise with `cryptR_collect()`.

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

Large parquet / CSV inputs are automatically routed to a streaming
engine (`arrow::open_dataset()` + chunked write) so memory usage stays
bounded regardless of input size.

## Learn more

- `vignette("cryptRopen")` — getting started with `crypt_vector()` and
  `crypt_data()`.
- `vignette("crypt_r-workflow")` — complete walkthrough of the
  industrialised `crypt_r()` workflow (Excel mask, async inspection,
  streaming engines, daemons ownership, recap log).
- `NEWS.md` — full change history.

## License

MIT © Daniel Rakotomalala.
