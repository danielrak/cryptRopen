# Hash a Vector with a Salted Pre-Image

Returns a hash of the same length as the input vector. Each element is
first coerced to character, upper-cased, trimmed, and stripped of
internal whitespace, then concatenated with `key` and hashed with
[`digest::digest()`](https://eddelbuettel.github.io/digest/man/digest.html).
The transformation is one-way and deterministic: the same `(value, key)`
pair always produces the same hash, and the original value cannot be
recovered from the hash alone.

## Usage

``` r
crypt_vector(vector, key, algo)
```

## Arguments

- vector:

  Atomic vector of values to hash. Non-character inputs are coerced to
  character before normalization.

- key:

  Character scalar. The salt prepended to each value before hashing.
  Choose a value with enough entropy that rainbow-table attacks are
  impractical.

- algo:

  Character scalar. Any algorithm accepted by the `algo` argument of
  [`digest::digest()`](https://eddelbuettel.github.io/digest/man/digest.html)
  (`"md5"`, `"sha1"`, `"sha256"`, …).

## Value

Character vector of upper-case hexadecimal hashes, same length as
`vector`. `NA` at every position where the input was `NA` or empty.

## Details

`NA` values and empty strings are preserved as `NA` in the output — they
are not hashed.

## Examples

``` r
# Three IDs, a missing value, an empty string. The non-empty values
# come back as upper-case MD5 hashes; `NA` and "" stay `NA`.
crypt_vector(
  vector = c("1234", "5678", "9101112", NA, ""),
  key = "123456",
  algo = "md5"
)
#> [1] "B38E2BF274239FF5DD2B45EE9AE099C9" "D71B95E05E2208136FEA6F9052FD6F9B"
#> [3] "3F9A1DBCC3C016DE3DC05F247F23D01D" NA                                
#> [5] NA                                

# Determinism: same value + same key + same algo => same hash.
identical(
  crypt_vector("Alice",  key = "k", algo = "md5"),
  crypt_vector("alice ", key = "k", algo = "md5")
)
#> [1] TRUE
```
