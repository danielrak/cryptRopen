# Tests for crypt_data(). Legacy snapshot on mtcars is kept as a historical
# anchor. Targeted tests below characterise the refactored behavior:
# no globalenv() pollution, correspondence tables routed through the
# package-private env and surfaced by get_correspondence_tables().

# ---- Legacy snapshot anchor --------------------------------------------

test_that("Valid outputs are consistent (mtcars snapshot)", {

  output <- structure(list(
    mpg_crypt = c("CEA9D4F43791ACE22D90EDBF6CE405F5",
                  "CEA9D4F43791ACE22D90EDBF6CE405F5",
                  "65080BE2078D5C5934F018F806E72E81",
                  "18C5941010E3CDCB669F472DF3A1DD5E",
                  "1866EA523631A281BD9431915F7C407F"),
    disp = c(160, 160, 108, 258, 360),
    hp = c(110, 110, 93, 110, 175),
    drat = c(3.9, 3.9, 3.85, 3.08, 3.15),
    wt = c(2.62, 2.875, 2.32, 3.215, 3.44),
    qsec = c(16.46, 17.02, 18.61, 19.44, 17.02),
    vs = c(0, 0, 1, 1, 0),
    am = c(1, 1, 1, 0, 0),
    gear = c(4, 4, 4, 3, 3),
    carb = c(4, 4, 1, 1, 2)),
    class = "data.frame",
    row.names = c("Mazda RX4", "Mazda RX4 Wag", "Datsun 710",
                  "Hornet 4 Drive", "Hornet Sportabout"))

  expect_identical(object = output,
                   expected = crypt_data(
                     loaded_dataset = mtcars[1:5, ],
                     vars_to_encrypt = "mpg",
                     vars_to_remove = "cyl",
                     encryption_key = "1234567",
                     algorithm = "md5",
                     correspondence_table = FALSE
                   ))
})

# ---- Argument validation -----------------------------------------------

test_that("Errors are consistent", {

  expect_error(
    object = crypt_data(loaded_dataset = mtcars[1:5],
                        vars_to_encrypt = "noexist",
                        vars_to_remove = "cyl",
                        encryption_key = "123456",
                        algorithm = "md5",
                        correspondence_table = FALSE),
    regexp = "All indicated vars_to_encrypt must be effectively a variable name."
  )

  expect_error(
    object = crypt_data(loaded_dataset = mtcars[1:5],
                        vars_to_encrypt = "mpg",
                        vars_to_remove = "noexist",
                        encryption_key = "123456",
                        algorithm = "md5",
                        correspondence_table = FALSE),
    regexp = "All indicated vars_to_remove must be effectively a variable name."
  )

  expect_error(
    object = crypt_data(loaded_dataset = mtcars[1:5, ],
                        vars_to_encrypt = "mpg",
                        encryption_key = "1234567",
                        correspondence_table = TRUE),
    regexp = "correspondence_table_label must be indicated"
  )
})

# ---- No globalenv pollution --------------------------------------------

test_that("crypt_data() does not pollute globalenv()", {
  cryptRopen:::.clear_correspondence_tables()
  pre <- ls(envir = globalenv())

  crypt_data(
    loaded_dataset = mtcars[1:5, ],
    vars_to_encrypt = "mpg",
    encryption_key = "1234567",
    algorithm = "md5",
    correspondence_table = TRUE,
    correspondence_table_label = "pollution_check"
  )

  expect_equal(setdiff(ls(envir = globalenv()), pre), character(0))

  cryptRopen:::.clear_correspondence_tables()
})

# ---- get_correspondence_tables() ---------------------------------------

test_that("get_correspondence_tables() returns the table stored by crypt_data()", {
  cryptRopen:::.clear_correspondence_tables()

  res <- crypt_data(
    loaded_dataset = mtcars[1:5, ],
    vars_to_encrypt = "mpg",
    encryption_key = "1234567",
    algorithm = "md5",
    correspondence_table = TRUE,
    correspondence_table_label = "mylabel"
  )

  tcs <- get_correspondence_tables()
  expect_named(tcs, "tc_crypt_mylabel")

  tc <- tcs[["tc_crypt_mylabel"]]
  expect_s3_class(tc, "data.frame")
  expect_equal(sort(names(tc)), sort(c("mpg", "mpg_crypt")))
  expect_equal(nrow(tc), nrow(mtcars[1:5, ]))
  expect_equal(tc$mpg_crypt, res$mpg_crypt)

  cryptRopen:::.clear_correspondence_tables()
})

test_that("correspondence_table = FALSE stores nothing", {
  cryptRopen:::.clear_correspondence_tables()

  crypt_data(
    loaded_dataset = mtcars[1:5, ],
    vars_to_encrypt = "mpg",
    encryption_key = "1234567",
    algorithm = "md5",
    correspondence_table = FALSE
  )

  expect_equal(length(get_correspondence_tables()), 0L)
})

test_that("successive calls accumulate (stacking semantics)", {
  cryptRopen:::.clear_correspondence_tables()

  crypt_data(loaded_dataset = mtcars[1:5, ],
             vars_to_encrypt = "mpg",
             encryption_key = "k",
             correspondence_table = TRUE,
             correspondence_table_label = "A")
  crypt_data(loaded_dataset = mtcars[1:5, ],
             vars_to_encrypt = "hp",
             encryption_key = "k",
             correspondence_table = TRUE,
             correspondence_table_label = "B")

  tcs <- get_correspondence_tables()
  expect_setequal(names(tcs), c("tc_crypt_A", "tc_crypt_B"))

  cryptRopen:::.clear_correspondence_tables()
})

test_that(".clear_correspondence_tables() empties the env", {
  crypt_data(loaded_dataset = mtcars[1:5, ],
             vars_to_encrypt = "mpg",
             encryption_key = "k",
             correspondence_table = TRUE,
             correspondence_table_label = "toclear")
  expect_true(length(get_correspondence_tables()) > 0L)

  cryptRopen:::.clear_correspondence_tables()
  expect_equal(length(get_correspondence_tables()), 0L)
})

# ---- Duplicated column names handling ----------------------------------

test_that("duplicate encrypted/original column names are deduplicated", {
  cryptRopen:::.clear_correspondence_tables()

  df <- data.frame(
    mpg = c(1, 2, 3),
    mpg_crypt = c("a", "b", "c"),
    stringsAsFactors = FALSE
  )

  res <- crypt_data(
    loaded_dataset = df,
    vars_to_encrypt = "mpg",
    encryption_key = "k",
    correspondence_table = FALSE
  )

  expect_true("mpg_crypt" %in% names(res))
  expect_true(any(grepl("_dupl", names(res))))
})

# ---- Character column cleanup (trim + blank-to-NA) ---------------------

test_that("character columns are trimmed and blank strings become NA", {
  cryptRopen:::.clear_correspondence_tables()

  df <- data.frame(
    id = c("  alice  ", "   ", "bob"),
    stringsAsFactors = FALSE
  )

  res <- crypt_data(
    loaded_dataset = df,
    vars_to_encrypt = "id",
    encryption_key = "k",
    correspondence_table = TRUE,
    correspondence_table_label = "trim"
  )

  expect_true(is.na(res$id_crypt[2]))
  expect_false(is.na(res$id_crypt[1]))
  expect_false(is.na(res$id_crypt[3]))

  cryptRopen:::.clear_correspondence_tables()
})
