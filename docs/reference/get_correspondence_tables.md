# Retrieve Correspondence Tables from the Current Session

Correspondence tables are the `(original, hashed)` mappings built during
a call to
[`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md)
or
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md).
They are stored in a package-private environment that accumulates across
successive calls within the same R session and is cleared when the
session ends.

## Usage

``` r
get_correspondence_tables(names = NULL)
```

## Arguments

- names:

  Optional character vector of table names to return. When `NULL`
  (default), every table currently stored is returned. When a vector is
  passed, the result preserves the requested order; names absent from
  the store appear as `NULL` entries so the caller can detect misses via
  `is.null(result[[name]])`.

## Value

A named list of data frames / tibbles. Names follow the convention
`tc_crypt_<label>` for
[`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md)
and `tc_<encrypted_file_without_extension>` for
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md).
An empty list when no correspondence table has been produced yet in the
session.

## See also

[`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md),
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md).

## Examples

``` r
# Run crypt_data() once to populate the store, then retrieve.
crypt_data(
  loaded_dataset             = mtcars[1:5, ],
  vars_to_encrypt            = "mpg",
  encryption_key             = "1234567",
  correspondence_table       = TRUE,
  correspondence_table_label = "demo"
)
#>                                          mpg_crypt cyl disp  hp drat    wt
#> Mazda RX4         CEA9D4F43791ACE22D90EDBF6CE405F5   6  160 110 3.90 2.620
#> Mazda RX4 Wag     CEA9D4F43791ACE22D90EDBF6CE405F5   6  160 110 3.90 2.875
#> Datsun 710        65080BE2078D5C5934F018F806E72E81   4  108  93 3.85 2.320
#> Hornet 4 Drive    18C5941010E3CDCB669F472DF3A1DD5E   6  258 110 3.08 3.215
#> Hornet Sportabout 1866EA523631A281BD9431915F7C407F   8  360 175 3.15 3.440
#>                    qsec vs am gear carb
#> Mazda RX4         16.46  0  1    4    4
#> Mazda RX4 Wag     17.02  0  1    4    4
#> Datsun 710        18.61  1  1    4    1
#> Hornet 4 Drive    19.44  1  0    3    1
#> Hornet Sportabout 17.02  0  0    3    2

# Full store as a named list.
get_correspondence_tables()
#> $tc_crypt_demo
#>                    mpg                        mpg_crypt
#> Mazda RX4         21.0 CEA9D4F43791ACE22D90EDBF6CE405F5
#> Mazda RX4 Wag     21.0 CEA9D4F43791ACE22D90EDBF6CE405F5
#> Datsun 710        22.8 65080BE2078D5C5934F018F806E72E81
#> Hornet 4 Drive    21.4 18C5941010E3CDCB669F472DF3A1DD5E
#> Hornet Sportabout 18.7 1866EA523631A281BD9431915F7C407F
#> 
#> $tc_persons_crypt
#> # A tibble: 10 × 2
#>    email         email_crypt                     
#>    <chr>         <chr>                           
#>  1 a@example.com 5039481DEF58F610590FE260B994AF1A
#>  2 b@example.com AA49E63CC89561CE3F7C9328486FD756
#>  3 c@example.com 5002AEF6282D0F3B1D1F24C0E1540B72
#>  4 d@example.com F7823A90F6717A115477F235B40C2C8F
#>  5 e@example.com 23C53292EB01730174CDDE37EB53DA80
#>  6 f@example.com FDFAA88718AFC1AE2029177337FC982C
#>  7 g@example.com BD80BBB60E8A6FF9ECCDF62249D66197
#>  8 h@example.com C19C713C407603F2656313721977BCB5
#>  9 i@example.com 05DB539F131D2FD8F659281C543D031C
#> 10 j@example.com 71413F22FD5F2699715044ABEF448D78
#> 

# Subset by name, preserving the requested order.
get_correspondence_tables(names = "tc_crypt_demo")
#> $tc_crypt_demo
#>                    mpg                        mpg_crypt
#> Mazda RX4         21.0 CEA9D4F43791ACE22D90EDBF6CE405F5
#> Mazda RX4 Wag     21.0 CEA9D4F43791ACE22D90EDBF6CE405F5
#> Datsun 710        22.8 65080BE2078D5C5934F018F806E72E81
#> Hornet 4 Drive    21.4 18C5941010E3CDCB669F472DF3A1DD5E
#> Hornet Sportabout 18.7 1866EA523631A281BD9431915F7C407F
#> 
```
