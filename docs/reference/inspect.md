# Profile a Data Frame

Returns a one-row-per-column profile of a data frame: class, number and
proportion of distinct values, number and proportion of missing values,
number and proportion of empty strings, length range of the character
representation, and the first ten unique modalities.

## Usage

``` r
inspect(data_frame, nrow = FALSE)
```

## Arguments

- data_frame:

  A data frame.

- nrow:

  Logical scalar. If `TRUE`, the number of rows of `data_frame` is
  emitted on stderr via
  [`message()`](https://rdrr.io/r/base/message.html) in addition to the
  returned tibble. Defaults to `FALSE`. Note that this parameter shadows
  the base function [`base::nrow()`](https://rdrr.io/r/base/nrow.html)
  inside the function body; callers passing a positional argument should
  keep `data_frame` first.

## Value

A tibble with one row per column of `data_frame` (or several when
`class(column)` has length \> 1), holding the per-column inspection
metrics described above.

## Details

Columns whose [`class()`](https://rdrr.io/r/base/class.html) has length
\> 1 (e.g. ordered factors with `class = c("ordered", "factor")`)
produce one row per class entry — this mirrors the historical behavior
built on `tibble(class = class(x), ...)`.

## Examples

``` r
# One row per column of CO2, with the distinct / NA / empty
# counts and the first ten unique modalities.
inspect(CO2)
#> # A tibble: 6 × 10
#>   variables class   nb_distinct prop_distinct nb_na prop_na nb_void prop_void
#>   <chr>     <chr>         <int>         <dbl> <int>   <dbl>   <int>     <dbl>
#> 1 Plant     ordered          12        0.143      0       0       0         0
#> 2 Plant     factor           12        0.143      0       0       0         0
#> 3 Type      factor            2        0.0238     0       0       0         0
#> 4 Treatment factor            2        0.0238     0       0       0         0
#> 5 conc      numeric           7        0.0833     0       0       0         0
#> 6 uptake    numeric          76        0.905      0       0       0         0
#> # ℹ 2 more variables: nchars <chr>, modalities <chr>
```
