#' Hash a Vector with a Salted Pre-Image
#'
#' Returns a hash of the same length as the input vector. Each element
#' is first coerced to character, upper-cased, trimmed, and stripped of
#' internal whitespace, then concatenated with `key` and hashed with
#' [digest::digest()]. The transformation is one-way and deterministic:
#' the same `(value, key)` pair always produces the same hash, and the
#' original value cannot be recovered from the hash alone.
#'
#' `NA` values and empty strings are preserved as `NA` in the output —
#' they are not hashed.
#'
#' @param vector Atomic vector of values to hash. Non-character inputs
#'   are coerced to character before normalization.
#' @param key Character scalar. The salt prepended to each value before
#'   hashing. Choose a value with enough entropy that rainbow-table
#'   attacks are impractical.
#' @param algo Character scalar. Any algorithm accepted by the `algo`
#'   argument of [digest::digest()] (`"md5"`, `"sha1"`, `"sha256"`, …).
#' @return Character vector of upper-case hexadecimal hashes, same
#'   length as `vector`. `NA` at every position where the input was
#'   `NA` or empty.
#' @export
#'
#' @examples
#' crypt_vector(
#'   vector = c("1234", "5678", "9101112", NA, ""),
#'   key = "123456",
#'   algo = "md5"
#' )
crypt_vector <- function(vector, key, algo) {
  normalized <- .normalize_crypt_input(vector)
  mask_na <- is.na(normalized) | nchar(normalized) == 0L

  out <- rep(NA_character_, length(normalized))
  hashes <- vapply(
    paste0(normalized[!mask_na], key),
    function(x) digest::digest(x, algo = algo, serialize = FALSE),
    character(1L),
    USE.NAMES = FALSE
  )
  out[!mask_na] <- stringr::str_to_upper(hashes)

  out
}

#' Normalize a vector before encryption
#'
#' Applies the deterministic pre-hash normalization:
#' upper-case, trim, remove internal spaces. Coerces to character
#' if needed (via stringr). NAs stay NA.
#'
#' @param x Atomic vector.
#' @return Character vector of same length as `x`.
#' @noRd
.normalize_crypt_input <- function(x) {
  x <- stringr::str_to_upper(x)
  x <- stringr::str_trim(x)
  stringr::str_replace_all(x, " ", "")
}
