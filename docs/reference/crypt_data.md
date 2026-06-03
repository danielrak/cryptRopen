# Pseudonymize Variables in a Data Frame

In-session version of the pseudonymization workflow: takes a data frame,
hashes the requested columns with a user-provided salt, and returns the
transformed data frame. The hash is deterministic and one-way (see
[`crypt_vector()`](https://danielrak.github.io/cryptRopen/reference/crypt_vector.md)
for the details of the transformation).

## Usage

``` r
crypt_data(
  loaded_dataset,
  vars_to_encrypt,
  vars_to_remove = NULL,
  encryption_key,
  algorithm = "md5",
  correspondence_table = TRUE,
  correspondence_table_label = NULL
)
```

## Arguments

- loaded_dataset:

  A data frame.

- vars_to_encrypt:

  Character vector of column names to hash. Must resolve to at least one
  non-empty entry after trimming and dropping `NA` / blank values; an
  empty vector raises an error.

- vars_to_remove:

  Character vector of column names to drop from the output. `NULL`
  (default) keeps all non-hashed columns.

- encryption_key:

  Character scalar. The salt prepended to each value before hashing. See
  [`crypt_vector()`](https://danielrak.github.io/cryptRopen/reference/crypt_vector.md).

- algorithm:

  Character scalar. Any algorithm accepted by the `algo` argument of
  [`digest::digest()`](https://eddelbuettel.github.io/digest/man/digest.html).
  Defaults to `"md5"`.

- correspondence_table:

  Logical scalar. If `TRUE` (default), stores the correspondence table
  in the package-private environment under the name
  `tc_crypt_<correspondence_table_label>`. Set to `FALSE` to skip the
  side effect entirely.

- correspondence_table_label:

  Character scalar. Required when `correspondence_table = TRUE`. Used to
  build the storage name so successive calls in the same session do not
  overwrite each other.

## Value

A data frame with the same row order as `loaded_dataset`, the
`vars_to_encrypt` columns replaced by their hashed counterparts (suffix
`_crypt`), and the `vars_to_remove` columns dropped.

## Details

When `correspondence_table = TRUE`, the (original, hashed) pairs are
stored in a package-private environment under the name
`tc_crypt_<correspondence_table_label>` and retrieved via
[`get_correspondence_tables()`](https://danielrak.github.io/cryptRopen/reference/get_correspondence_tables.md).
No file is written; this side effect replaces an older behavior that
polluted [`globalenv()`](https://rdrr.io/r/base/environment.html).

`crypt_data()` is **fail-fast** on an empty `vars_to_encrypt`: it raises
an explicit error rather than silently returning the input unchanged.
For a "drop columns / convert format" workflow on a batch of files
described by an Excel mask, use
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
with a blank `vars_to_encrypt` cell — that path is legitimate. On a
single in-memory object, use
[`dplyr::select()`](https://dplyr.tidyverse.org/reference/select.html) /
[`rio::export()`](http://gesistsa.github.io/rio/reference/export.md)
directly.

## See also

[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
for the batch / file-driven counterpart,
[`get_correspondence_tables()`](https://danielrak.github.io/cryptRopen/reference/get_correspondence_tables.md)
to retrieve the stored mapping,
[`crypt_vector()`](https://danielrak.github.io/cryptRopen/reference/crypt_vector.md)
for the underlying vector transformation.

## Examples

``` r
# Replace `mpg` with `mpg_crypt`, drop `cyl`, no correspondence
# table side effect on this run.
crypt_data(
  loaded_dataset       = mtcars[1:5, ],
  vars_to_encrypt      = "mpg",
  vars_to_remove       = "cyl",
  encryption_key       = "1234567",
  algorithm            = "md5",
  correspondence_table = FALSE
)
#>                                          mpg_crypt disp  hp drat    wt  qsec vs
#> Mazda RX4         CEA9D4F43791ACE22D90EDBF6CE405F5  160 110 3.90 2.620 16.46  0
#> Mazda RX4 Wag     CEA9D4F43791ACE22D90EDBF6CE405F5  160 110 3.90 2.875 17.02  0
#> Datsun 710        65080BE2078D5C5934F018F806E72E81  108  93 3.85 2.320 18.61  1
#> Hornet 4 Drive    18C5941010E3CDCB669F472DF3A1DD5E  258 110 3.08 3.215 19.44  1
#> Hornet Sportabout 1866EA523631A281BD9431915F7C407F  360 175 3.15 3.440 17.02  0
#>                   am gear carb
#> Mazda RX4          1    4    4
#> Mazda RX4 Wag      1    4    4
#> Datsun 710         1    4    1
#> Hornet 4 Drive     0    3    1
#> Hornet Sportabout  0    3    2
```
