# Getting started with cryptRopen

## What cryptRopen is for

`cryptRopen` pseudonymizes variables inside data frames so that
sensitive values (identifiers, emails, names) can be shared or stored in
a derivative form without exposing the originals. The pseudonymization
is **deterministic**: the same input value paired with the same key
always produces the same output, so joins across tables remain possible
on the pseudonymized columns. A side table — the *correspondence table*
— keeps the mapping between original and pseudonymized values for the
party that holds the key.

Typical use cases: preparing an extract for an analytics team that
should not see customer identifiers; sharing a dataset with an external
partner; feeding a downstream pipeline while keeping the original
identifiers under control.

## Pseudonymization, not encryption

The `crypt` in the package name is historical. The underlying
transformation is a **salted hash**, not reversible encryption:

- A normalized version of each input value is concatenated with a
  user-provided salt (`key`) and hashed with
  [`digest::digest()`](https://eddelbuettel.github.io/digest/man/digest.html).
- The default algorithm is `"md5"`. Any algorithm supported by
  [`digest::digest()`](https://eddelbuettel.github.io/digest/man/digest.html)
  (`"sha1"`, `"sha256"`, `"sha512"`, …) is accepted via the `algorithm`
  parameter — see *Algorithm choice* below.
- Holding the output does **not** let anyone recover the input. Only the
  correspondence table (or a rainbow-table-style attack on a weak key)
  does.

Consequence: cryptRopen is suitable for pseudonymization under
regulations such as GDPR, not for confidentiality-bearing encryption.

## When *not* to use cryptRopen

Pseudonymization is one tool among several. Reach for a different
approach when:

- **You need to recover the original value later.** A salted hash is
  one-way. If your workflow must un-pseudonymize at some point, use
  symmetric encryption (e.g. the `safer` or `sodium` packages) and
  manage the key separately.
- **You need formal anonymization, not pseudonymization.** Under
  regulations such as GDPR, pseudonymized data is still personal data —
  the correspondence table is the de facto re-identification key. For
  irreversible anonymization, consider differential privacy,
  k-anonymity, or aggregation.
- **You want to hide value distributions.** A deterministic hash
  preserves the frequency profile of the original column. An attacker
  who knows the rough distribution of the input (e.g. that certain
  customer IDs occur very often) can still spot the most frequent
  hashes. Add random padding or switch to format-preserving encryption
  if that matters for your threat model.
- **You need integrity guarantees on the data, not just on its
  identifiers.** Hashes here are computed *per cell*, not over a whole
  row or file. They do not detect tampering at the dataset level — pair
  them with a separate signature scheme if you need that.

## Algorithm choice

The package delegates the actual hashing to
[`digest::digest()`](https://eddelbuettel.github.io/digest/man/digest.html),
so every algorithm of that function is available via the `algorithm`
parameter. The trade-offs you should know:

- **`"md5"` (default)** — fast and short (32 hex chars). Cryptographic
  collisions exist as theoretical constructions but are negligible at
  practical dataset scales for *pseudonymization*. Recommended unless
  you have a specific reason to step up.
- **`"sha1"`** — 40 hex chars, deprecated in formal cryptography but
  still acceptable for non-adversarial pseudonymization. Rarely a better
  choice than MD5 or SHA-256.
- **`"sha256"`** — 64 hex chars, the modern conservative default if you
  want to be on the safer side. Slower than MD5 but the difference is
  typically below 1 % of total runtime on a real pseudonymization job
  dominated by I/O.
- **`"sha512"`** — 128 hex chars, only useful when downstream systems
  happen to expect a 512-bit hash. The output is twice as long for no
  practical security gain in this context.

Whatever you pick, lock it down in your run and keep the *same*
`encryption_key` across all runs that need to join: changing either
breaks the deterministic property and your tables stop joining on the
pseudonymized columns.

## Installation

``` r

# install.packages("pak")
pak::pak("danielrak/cryptRopen")
```

``` r

library(cryptRopen)
```

## Hashing a single vector

[`crypt_vector()`](https://danielrak.github.io/cryptRopen/reference/crypt_vector.md)
is the lowest-level entry point. It takes a vector, a key, and a hash
algorithm, and returns a character vector of the same length. `NA` and
empty strings stay `NA`. Input is uppercased, trimmed, and stripped of
internal spaces before hashing, so `"Alice"`, `"alice "` and `" ALICE "`
collapse to the same hash.

``` r

crypt_vector(
  vector = c("Alice", "Bob", "Charlie"),
  key = "my-secret-salt",
  algo = "md5"
)
#> [1] "450AC498E2781A99FCEE98F8D460A87E" "88C40ACBB5A8351455503E2EB8A44089"
#> [3] "1EEAF5D958D4AF3BA58900DDB131BB18"
```

Determinism and the NA / empty-string rules:

``` r

# Same value + same key => same hash, every time.
identical(
  crypt_vector("Alice", key = "k", algo = "md5"),
  crypt_vector("alice ", key = "k", algo = "md5")
)
#> [1] TRUE

# NAs and empty strings are preserved.
crypt_vector(c("Alice", NA, "", "  "), key = "k", algo = "md5")
#> [1] "7A9C2C3ACBB6E4B93066B2B66501EB10" NA                                
#> [3] NA                                 NA
```

## Hashing one or several columns of a data frame

[`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md)
operates on a data frame and returns a new data frame with the requested
columns replaced by their hashed counterparts (suffix `_crypt`).
Original columns can optionally be dropped (`vars_to_remove`).

``` r

head(mtcars, 3)
#>                mpg cyl disp  hp drat    wt  qsec vs am gear carb
#> Mazda RX4     21.0   6  160 110 3.90 2.620 16.46  0  1    4    4
#> Mazda RX4 Wag 21.0   6  160 110 3.90 2.875 17.02  0  1    4    4
#> Datsun 710    22.8   4  108  93 3.85 2.320 18.61  1  1    4    1

encrypted <- crypt_data(
  loaded_dataset            = head(mtcars, 3),
  vars_to_encrypt           = "mpg",
  vars_to_remove            = "cyl",
  encryption_key            = "demo-key",
  algorithm                 = "md5",
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
```

The correspondence table is **not** returned alongside the data frame;
it is stored inside a package-private environment and retrieved via
[`get_correspondence_tables()`](https://danielrak.github.io/cryptRopen/reference/get_correspondence_tables.md):

``` r

tcs <- get_correspondence_tables()
names(tcs)
#> [1] "tc_crypt_demo"
tcs$tc_crypt_demo
#>                mpg                        mpg_crypt
#> Mazda RX4     21.0 C33FDD39F6EAADF6C5E135AD5083E3D6
#> Mazda RX4 Wag 21.0 C33FDD39F6EAADF6C5E135AD5083E3D6
#> Datsun 710    22.8 7E7E52C52B1C3686F553D1E4AC6A8207
```

Tables accumulate across calls in the same session. Pass a character
vector to `names` to pull only the ones you need:

``` r

get_correspondence_tables(names = "tc_crypt_demo")
#> $tc_crypt_demo
#>                mpg                        mpg_crypt
#> Mazda RX4     21.0 C33FDD39F6EAADF6C5E135AD5083E3D6
#> Mazda RX4 Wag 21.0 C33FDD39F6EAADF6C5E135AD5083E3D6
#> Datsun 710    22.8 7E7E52C52B1C3686F553D1E4AC6A8207
```

Pseudonymising multiple columns at once is a matter of extending
`vars_to_encrypt`:

``` r

crypt_data(
  loaded_dataset            = head(mtcars, 3),
  vars_to_encrypt           = c("mpg", "hp"),
  encryption_key            = "demo-key",
  correspondence_table      = TRUE,
  correspondence_table_label = "demo_multi"
)
#>                                      mpg_crypt                         hp_crypt
#> Mazda RX4     C33FDD39F6EAADF6C5E135AD5083E3D6 7D10FEDBB84390262D97BCD0956D8762
#> Mazda RX4 Wag C33FDD39F6EAADF6C5E135AD5083E3D6 7D10FEDBB84390262D97BCD0956D8762
#> Datsun 710    7E7E52C52B1C3686F553D1E4AC6A8207 BB8BE843A95B4E5921897169087511EC
#>               cyl disp drat    wt  qsec vs am gear carb
#> Mazda RX4       6  160 3.90 2.620 16.46  0  1    4    4
#> Mazda RX4 Wag   6  160 3.90 2.875 17.02  0  1    4    4
#> Datsun 710      4  108 3.85 2.320 18.61  1  1    4    1
```

## When you need to batch multiple files

[`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md)
is the right tool when you hold one data frame in memory and want an
encrypted version of it. For an industrialized workflow — several input
files specified in an Excel mask, outputs written to disk, parallel
execution, a recap log — see
[`vignette("crypt_r-workflow")`](https://danielrak.github.io/cryptRopen/articles/crypt_r-workflow.md).

## Related resources

- [`?crypt_vector`](https://danielrak.github.io/cryptRopen/reference/crypt_vector.md),
  [`?crypt_data`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md),
  [`?crypt_r`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
  — the three public entry points.
- [`?get_correspondence_tables`](https://danielrak.github.io/cryptRopen/reference/get_correspondence_tables.md)
  — retrieval of the correspondence tables stored in the package-private
  environment.
- `NEWS.md` — full change history.
