# Tests for inspect(). The legacy snapshot on CO2 is kept as a historical
# anchor ; targeted tests below characterise each computed column, edge
# cases, and the internal helper .inspect_column() in isolation.

# ---- Legacy snapshot anchor ---------------------------------------------

test_that("Valid outputs are consistent (CO2 snapshot)", {

  output <- structure(list(
    variables = c("Plant", "Plant", "Type", "Treatment", "conc", "uptake"),
    class = c("ordered", "factor", "factor", "factor", "numeric", "numeric"),
    nb_distinct = c(12L, 12L, 2L, 2L, 7L, 76L),
    prop_distinct = c(0.142857142857143, 0.142857142857143, 0.0238095238095238,
                      0.0238095238095238, 0.0833333333333333, 0.904761904761905),
    nb_na = c(0L, 0L, 0L, 0L, 0L, 0L),
    prop_na = c(0, 0, 0, 0, 0, 0),
    nb_void = c(0L, 0L, 0L, 0L, 0L, 0L),
    prop_void = c(0, 0, 0, 0, 0, 0),
    nchars = c("3", "3", "6 / 11", "7 / 10", "2 / 3 / 4", "2 / 3 / 4"),
    modalities = c("Qn1 / Qn2 / Qn3 / Qc1 / Qc3 / Qc2 / Mn3 / Mn2 / Mn1 / Mc2",
                   "Qn1 / Qn2 / Qn3 / Qc1 / Qc3 / Qc2 / Mn3 / Mn2 / Mn1 / Mc2",
                   "Quebec / Mississippi", "nonchilled / chilled",
                   "95 / 175 / 250 / 350 / 500 / 675 / 1000",
                   "7.7 / 9.3 / 10.5 / 10.6 / 11.3 / 11.4 / 12 / 12.3 / 12.5 / 13")),
    class = c("tbl_df", "tbl", "data.frame"),
    row.names = c(NA, -6L))
  expect_equal(object = output,
               expected = inspect(CO2))
})

# ---- Output structure ---------------------------------------------------

test_that("inspect() returns a tibble with the expected columns in order", {
  df <- data.frame(a = 1:3, b = c("x", "y", "z"), stringsAsFactors = FALSE)
  out <- inspect(df)

  expect_s3_class(out, "tbl_df")
  expect_identical(
    names(out),
    c("variables", "class", "nb_distinct", "prop_distinct",
      "nb_na", "prop_na", "nb_void", "prop_void", "nchars", "modalities"))
})

test_that("inspect() produces one row per (column, class) pair", {
  df <- data.frame(
    x = 1:3,
    y = factor(c("a", "b", "c"), ordered = TRUE),
    stringsAsFactors = FALSE)
  out <- inspect(df)

  # y is ordered factor: class = c("ordered", "factor"), produces 2 rows
  expect_equal(nrow(out), 3L)
  expect_equal(out$variables, c("x", "y", "y"))
  expect_equal(out$class, c("integer", "ordered", "factor"))
})

# ---- Per-column metrics -------------------------------------------------

test_that("nb_distinct counts distinct values (NA counts as one)", {
  df <- data.frame(
    a = c(1, 2, 2, 3),
    b = c(NA, "x", "y", "y"),
    stringsAsFactors = FALSE)
  out <- inspect(df)
  expect_equal(out$nb_distinct, c(3L, 3L)) # b: NA, "x", "y"
})

test_that("prop_distinct is nb_distinct / nrow(data_frame)", {
  df <- data.frame(a = c(1, 1, 2, 3), b = letters[1:4])
  out <- inspect(df)
  expect_equal(out$prop_distinct, c(3 / 4, 4 / 4))
})

test_that("nb_na and prop_na track NAs per column", {
  df <- data.frame(
    a = c(NA, NA, 1, 2),
    b = c("x", NA, "y", "z"),
    stringsAsFactors = FALSE)
  out <- inspect(df)
  expect_equal(out$nb_na, c(2L, 1L))
  expect_equal(out$prop_na, c(0.5, 0.25))
})

test_that("nb_void counts empty strings (NAs do not contribute)", {
  df <- data.frame(
    a = c("", "x", "", NA, "y"),
    stringsAsFactors = FALSE)
  out <- inspect(df)
  expect_equal(out$nb_void, 2L)
  expect_equal(out$prop_void, 2 / 5)
})

test_that("nb_void on numeric columns returns 0 (coercion safe)", {
  df <- data.frame(a = c(1, 2, 3))
  out <- inspect(df)
  expect_equal(out$nb_void, 0L)
})

test_that("nchars lists up to 10 distinct nchar values, sorted ascending", {
  df <- data.frame(
    a = c("a", "bb", "ccc", "dddd"),
    stringsAsFactors = FALSE)
  out <- inspect(df)
  expect_equal(out$nchars, "1 / 2 / 3 / 4")
})

test_that("modalities lists up to 10 distinct values, sorted", {
  df <- data.frame(
    a = c("z", "a", "m", "a"),
    stringsAsFactors = FALSE)
  out <- inspect(df)
  expect_equal(out$modalities, "a / m / z")
})

test_that("modalities caps at 10 distinct values", {
  df <- data.frame(a = as.character(1:15), stringsAsFactors = FALSE)
  out <- inspect(df)
  expect_equal(
    out$modalities,
    paste(sort(as.character(1:15))[1:10], collapse = " / "))
})

# ---- POSIXct handling ---------------------------------------------------

test_that("POSIXct columns get class 'Date-time' (single-valued)", {
  df <- data.frame(
    ts = as.POSIXct(c("2024-01-01", "2024-01-02"), tz = "UTC"))
  out <- inspect(df)
  expect_equal(nrow(out), 1L)
  expect_equal(out$class, "Date-time")
})

# ---- Edge cases ---------------------------------------------------------

test_that("all-NA column: nb_distinct = 1, nb_na = nrow", {
  df <- data.frame(a = c(NA, NA, NA))
  out <- inspect(df)
  expect_equal(out$nb_distinct, 1L)
  expect_equal(out$nb_na, 3L)
  expect_equal(out$prop_na, 1)
})

test_that("empty data frame: metrics compute without error", {
  df <- data.frame(a = integer(0), b = character(0), stringsAsFactors = FALSE)
  out <- inspect(df)
  expect_equal(nrow(out), 2L)
  expect_equal(out$nb_distinct, c(0L, 0L))
  expect_true(all(is.nan(out$prop_distinct)))
  expect_equal(out$nb_na, c(0L, 0L))
  # Historical behavior: empty column gives "NA" string in nchars and
  # modalities (because 1:min(0, 10) = c(1, 0) indexes [1] which is NA).
  expect_equal(out$nchars, c("NA", "NA"))
  expect_equal(out$modalities, c("NA", "NA"))
})

test_that("Date columns: single class, class = 'Date'", {
  df <- data.frame(d = as.Date(c("2024-01-01", "2024-06-15", "2024-12-31")))
  out <- inspect(df)
  expect_equal(nrow(out), 1L)
  expect_equal(out$class, "Date")
  expect_equal(out$nb_distinct, 3L)
})

test_that("logical columns are handled", {
  df <- data.frame(b = c(TRUE, FALSE, TRUE, NA))
  out <- inspect(df)
  expect_equal(out$class, "logical")
  expect_equal(out$nb_distinct, 3L) # TRUE, FALSE, NA
  expect_equal(out$nb_na, 1L)
})

# ---- nrow = TRUE side-effect --------------------------------------------

test_that("nrow = TRUE prints the row count but returns the same tibble", {
  df <- data.frame(a = 1:7)
  out_default <- inspect(df)
  out_printed <- NULL
  printed <- capture.output({ out_printed <- inspect(df, nrow = TRUE) })
  expect_equal(out_printed, out_default)
  expect_true(any(grepl("7", printed)))
})

# ---- .inspect_column() helper in isolation ------------------------------

test_that(".inspect_column() returns one row per class of x", {
  out <- cryptRopen:::.inspect_column(1:3, rows = 3)
  expect_equal(nrow(out), 1L)
  expect_equal(out$class, "integer")

  out2 <- cryptRopen:::.inspect_column(
    factor(c("a", "b"), ordered = TRUE), rows = 2)
  expect_equal(nrow(out2), 2L)
  expect_equal(out2$class, c("ordered", "factor"))
})

test_that(".inspect_column() computes metrics against the 'rows' argument", {
  # Pass rows larger than length(x): prop_* reflects that denominator
  out <- cryptRopen:::.inspect_column(c(1, 2, 3), rows = 10)
  expect_equal(out$nb_distinct, 3L)
  expect_equal(out$prop_distinct, 0.3)
})
