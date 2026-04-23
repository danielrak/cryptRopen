# Unit tests for crypt_vector() and its private normalization helper.
# Non-regression coverage against the pre-refactor package lives in
# tests/testthat/test-baseline.R.


# ---------------------------------------------------------------------------
# Known hashes — verify correctness against digest::digest() directly
# ---------------------------------------------------------------------------

test_that("crypt_vector matches digest::digest() on simple inputs", {
  # Normalization is trivial here ("ABC" stays "ABC").
  expected <- toupper(digest::digest("ABCkey", algo = "md5", serialize = FALSE))
  result <- crypt_vector("ABC", key = "key", algo = "md5")

  expect_equal(result, expected)
})

test_that("crypt_vector applies upper-case then concatenates with key", {
  expected <- toupper(digest::digest("HELLOsecret",
    algo = "md5", serialize = FALSE
  ))
  expect_equal(
    crypt_vector("hello", key = "secret", algo = "md5"),
    expected
  )
})

test_that("crypt_vector strips leading, trailing and internal spaces", {
  # All four inputs normalize to "ABC" and must hash to the same value.
  inputs <- c("ABC", "  abc  ", "a b c", "  a b  c ")
  out <- crypt_vector(inputs, key = "K", algo = "md5")
  expect_length(unique(out), 1L)
  expect_equal(
    out[[1]],
    toupper(digest::digest("ABCK", algo = "md5", serialize = FALSE))
  )
})

test_that("crypt_vector reproduces historical known hashes", {
  # Values captured from the pre-refactor package — must not drift.
  expect_identical(
    crypt_vector(c(123, 456, 789), key = "123456", algo = "md5"),
    c(
      "1E191D851B3B49A248F4EA62F6B06410",
      "F87003205DD342BEC2B81EE940172A4D",
      "11A22927C85333CE98510B0FA1A1C27D"
    )
  )
  expect_identical(
    crypt_vector(c("abc", "def", NA), key = "123456", algo = "md5"),
    c(
      "09C74ADA9E6C9FBDEF249B258A66E948",
      "4FF225BA6F02CC8E5EB64E7647FBB7FC",
      NA
    )
  )
})


# ---------------------------------------------------------------------------
# NA / empty-string handling
# ---------------------------------------------------------------------------

test_that("NAs in input stay NA in output", {
  out <- crypt_vector(c("abc", NA, "def"), key = "K", algo = "md5")
  expect_true(is.na(out[2]))
  expect_false(is.na(out[1]))
  expect_false(is.na(out[3]))
})

test_that("empty strings become NA in output", {
  out <- crypt_vector(c("abc", "", "def"), key = "K", algo = "md5")
  expect_true(is.na(out[2]))
})

test_that("strings reduced to empty by normalization become NA", {
  out <- crypt_vector(c("   ", " a ", "  "), key = "K", algo = "md5")
  expect_true(is.na(out[1]))
  expect_false(is.na(out[2]))
  expect_true(is.na(out[3]))
})


# ---------------------------------------------------------------------------
# Structural properties
# ---------------------------------------------------------------------------

test_that("output is an unnamed character vector", {
  out <- crypt_vector(c("a", "b"), key = "K", algo = "md5")
  expect_type(out, "character")
  expect_null(names(out))
})

test_that("output has the same length as input", {
  for (n in c(0L, 1L, 2L, 5L, 20L)) {
    input <- if (n == 0L) character(0L) else rep("x", n)
    out <- crypt_vector(input, key = "K", algo = "md5")
    expect_length(out, n)
  }
})

test_that("length-zero input returns character(0)", {
  out <- crypt_vector(character(0), key = "K", algo = "md5")
  expect_equal(out, character(0))
})

test_that("output is uppercase hexadecimal for hex-producing algos", {
  out <- crypt_vector(c("foo", "bar"), key = "K", algo = "md5")
  expect_true(all(grepl("^[0-9A-F]+$", out)))
  expect_true(all(nchar(out) == 32L))
})


# ---------------------------------------------------------------------------
# Determinism, key / algo sensitivity
# ---------------------------------------------------------------------------

test_that("two calls with identical arguments produce identical results", {
  input <- c("foo", "bar", NA, "baz")
  a <- crypt_vector(input, key = "K", algo = "md5")
  b <- crypt_vector(input, key = "K", algo = "md5")
  expect_identical(a, b)
})

test_that("different keys produce different hashes on non-NA values", {
  a <- crypt_vector(c("foo", "bar"), key = "K1", algo = "md5")
  b <- crypt_vector(c("foo", "bar"), key = "K2", algo = "md5")
  expect_false(any(a == b))
})

test_that("different algos produce hashes of different lengths", {
  v <- c("foo", "bar")
  l_md5 <- nchar(crypt_vector(v, key = "K", algo = "md5"))
  l_sha1 <- nchar(crypt_vector(v, key = "K", algo = "sha1"))
  l_sha256 <- nchar(crypt_vector(v, key = "K", algo = "sha256"))
  expect_true(all(l_md5 == 32L))
  expect_true(all(l_sha1 == 40L))
  expect_true(all(l_sha256 == 64L))
})


# ---------------------------------------------------------------------------
# Non-character input coercion
# ---------------------------------------------------------------------------

test_that("numeric input is coerced and hashed consistently", {
  out_num <- crypt_vector(c(1234, 5678), key = "K", algo = "md5")
  out_chr <- crypt_vector(c("1234", "5678"), key = "K", algo = "md5")
  expect_identical(out_num, out_chr)
})

test_that("Date input is coerced via stringr (character representation)", {
  out <- crypt_vector(as.Date("2024-01-01"), key = "K", algo = "md5")
  expect_length(out, 1L)
  expect_false(is.na(out))
  expect_type(out, "character")
})


# ---------------------------------------------------------------------------
# .normalize_crypt_input() in isolation
# ---------------------------------------------------------------------------

test_that(".normalize_crypt_input uppercases", {
  expect_equal(cryptRopen:::.normalize_crypt_input("abc"), "ABC")
  expect_equal(cryptRopen:::.normalize_crypt_input("Hello"), "HELLO")
})

test_that(".normalize_crypt_input removes leading and trailing spaces", {
  expect_equal(cryptRopen:::.normalize_crypt_input("  abc  "), "ABC")
})

test_that(".normalize_crypt_input removes internal spaces", {
  expect_equal(cryptRopen:::.normalize_crypt_input("a b c"), "ABC")
  expect_equal(cryptRopen:::.normalize_crypt_input(" a  b  c "), "ABC")
})

test_that(".normalize_crypt_input propagates NA", {
  expect_true(is.na(cryptRopen:::.normalize_crypt_input(NA_character_)))
})

test_that(".normalize_crypt_input preserves length", {
  for (n in c(0L, 1L, 3L, 10L)) {
    x <- if (n == 0L) character(0L) else rep("abc", n)
    expect_length(cryptRopen:::.normalize_crypt_input(x), n)
  }
})

test_that(".normalize_crypt_input returns character", {
  expect_type(cryptRopen:::.normalize_crypt_input("abc"), "character")
  expect_type(cryptRopen:::.normalize_crypt_input(123), "character")
})
