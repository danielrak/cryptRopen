# Unit tests for the internal .process_mask_row() helper extracted from
# crypt_r() in Phase 1.D.1. These tests exercise the helper SYNCHRONOUSLY
# (bypassing job::job) so they also run under R CMD check — which gives
# coverage independent of the AST sync-patch used by test-baseline.R.
#
# Phase 1.D.2 adds direct tests on .process_mask_row_in_memory() to lock
# the contract of the in-memory engine before the streaming engine is
# written in 1.D.4. The dispatcher .process_mask_row() currently just
# delegates to it, so the blocks above (which go through the dispatcher)
# and the blocks below (which call the engine directly) together prove
# both the dispatcher and the engine.

# ---- Fixtures ----------------------------------------------------------

# Build a one-row mask matching the shape consumed by .process_mask_row():
# columns folder_path, file, encrypted_file, vars_to_encrypt, vars_to_remove.
# (row_number / dupl_* are not read by the helper.)
make_sm <- function(folder_path, file, encrypted_file,
                    vars_to_encrypt, vars_to_remove = NA) {
  data.frame(
    folder_path     = folder_path,
    file            = file,
    encrypted_file  = encrypted_file,
    vars_to_encrypt = vars_to_encrypt,
    vars_to_remove  = vars_to_remove,
    stringsAsFactors = FALSE
  )
}

setup_dirs <- function() {
  root <- tempfile("pmr_")
  dir.create(root)
  inp <- file.path(root, "inputs");       dir.create(inp)
  out <- file.path(root, "output");       dir.create(out)
  int <- file.path(root, "intermediate"); dir.create(int)
  list(root = root, inp = inp, out = out, int = int)
}

# Remove every name that appeared in globalenv() after `pre` was snapped.
clean_globals <- function(pre) {
  post <- ls(envir = globalenv())
  new  <- setdiff(post, pre)
  if (length(new) > 0L) rm(list = new, envir = globalenv())
}

# ---- Happy path --------------------------------------------------------

test_that(".process_mask_row() writes crypt csv + tc parquet + inspect xlsx", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
  }, add = TRUE)

  csv <- file.path(dirs$inp, "persons.csv")
  utils::write.csv(
    data.frame(id = c("alice", "bob", "carol"),
               age = c(30, 40, 50),
               stringsAsFactors = FALSE),
    csv, row.names = FALSE)

  sm <- make_sm(
    folder_path     = dirs$inp,
    file            = "persons.csv",
    encrypted_file  = "persons_crypt.csv",
    vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row(
    sm                   = sm,
    input_path           = csv,
    output_path          = dirs$out,
    intermediate_path    = dirs$int,
    encryption_key       = "testkey",
    algorithm            = "md5",
    correspondence_table = TRUE)

  expect_true(file.exists(file.path(dirs$out, "persons_crypt.csv")))
  expect_true(file.exists(file.path(dirs$int, "tc_persons_crypt.parquet")))
  expect_true(file.exists(file.path(dirs$out,
                                    "inspect_persons_crypt.csv.xlsx")))

  # Output dataset has id_crypt but not id.
  out_df <- utils::read.csv(file.path(dirs$out, "persons_crypt.csv"),
                            stringsAsFactors = FALSE)
  expect_true("id_crypt" %in% names(out_df))
  expect_false("id" %in% names(out_df))
  expect_true("age" %in% names(out_df))
  expect_equal(nrow(out_df), 3L)

  # Historical side effects on globalenv (preserved until Phase 1.D.6).
  expect_true(exists("id_crypt",      envir = globalenv()))
  expect_true(exists("persons_crypt", envir = globalenv()))
  expect_true(exists("tc_persons_crypt", envir = globalenv()))
})

# ---- correspondence_table = FALSE --------------------------------------

test_that("correspondence_table = FALSE: no parquet, no tc_* in globalenv", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
  }, add = TRUE)

  csv <- file.path(dirs$inp, "p.csv")
  utils::write.csv(data.frame(id = c("a", "b"),
                              stringsAsFactors = FALSE),
                   csv, row.names = FALSE)

  sm <- make_sm(folder_path = dirs$inp, file = "p.csv",
                encrypted_file = "p_crypt.csv",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row(
    sm = sm, input_path = csv,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = FALSE)

  expect_true(file.exists(file.path(dirs$out, "p_crypt.csv")))
  expect_false(file.exists(file.path(dirs$int, "tc_p_crypt.parquet")))
  expect_false(exists("tc_p_crypt", envir = globalenv()))
})

# ---- Multiple vars_to_encrypt ------------------------------------------

test_that("multiple vars_to_encrypt: all encrypted, single combined TC", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
  }, add = TRUE)

  csv <- file.path(dirs$inp, "two.csv")
  utils::write.csv(
    data.frame(a = c("x", "y"),
               b = c("p", "q"),
               keep = c(1, 2),
               stringsAsFactors = FALSE),
    csv, row.names = FALSE)

  sm <- make_sm(folder_path = dirs$inp, file = "two.csv",
                encrypted_file = "two_crypt.csv",
                vars_to_encrypt = "a, b")

  cryptRopen:::.process_mask_row(
    sm = sm, input_path = csv,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE)

  out_df <- utils::read.csv(file.path(dirs$out, "two_crypt.csv"),
                            stringsAsFactors = FALSE)
  expect_true(all(c("a_crypt", "b_crypt", "keep") %in% names(out_df)))
  expect_false(any(c("a", "b") %in% names(out_df)))

  expect_true(exists("a_crypt", envir = globalenv()))
  expect_true(exists("b_crypt", envir = globalenv()))

  expect_true(file.exists(file.path(dirs$int, "tc_two_crypt.parquet")))
  tc <- arrow::read_parquet(file.path(dirs$int, "tc_two_crypt.parquet"))
  expect_true(all(c("a", "b", "a_crypt", "b_crypt") %in% names(tc)))
})

# ---- vars_to_remove ----------------------------------------------------

test_that("vars_to_remove drops the listed columns from the output", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
  }, add = TRUE)

  csv <- file.path(dirs$inp, "r.csv")
  utils::write.csv(
    data.frame(id = c("a", "b"),
               to_drop = c(1, 2),
               keep = c(9, 8),
               stringsAsFactors = FALSE),
    csv, row.names = FALSE)

  sm <- make_sm(folder_path = dirs$inp, file = "r.csv",
                encrypted_file = "r_crypt.csv",
                vars_to_encrypt = "id",
                vars_to_remove  = "to_drop")

  cryptRopen:::.process_mask_row(
    sm = sm, input_path = csv,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = FALSE)

  out_df <- utils::read.csv(file.path(dirs$out, "r_crypt.csv"),
                            stringsAsFactors = FALSE)
  expect_true("id_crypt" %in% names(out_df))
  expect_true("keep"     %in% names(out_df))
  expect_false("id"      %in% names(out_df))
  expect_false("to_drop" %in% names(out_df))
})

# ---- Missing input file -------------------------------------------------

test_that("missing input file is swallowed by try() (no crash, no output)", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
  }, add = TRUE)

  sm <- make_sm(folder_path = dirs$inp, file = "ghost.csv",
                encrypted_file = "ghost_crypt.csv",
                vars_to_encrypt = "id")

  expect_no_error(
    cryptRopen:::.process_mask_row(
      sm = sm,
      input_path = file.path(dirs$inp, "ghost.csv"),
      output_path = dirs$out, intermediate_path = dirs$int,
      encryption_key = "k", algorithm = "md5",
      correspondence_table = TRUE)
  )

  expect_false(file.exists(file.path(dirs$out, "ghost_crypt.csv")))
  expect_false(file.exists(file.path(dirs$int, "tc_ghost_crypt.parquet")))
})

# ---- Direct call to the in_memory engine (Phase 1.D.2) -----------------

test_that(".process_mask_row_in_memory() produces the same outputs directly", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
  }, add = TRUE)

  csv <- file.path(dirs$inp, "persons.csv")
  utils::write.csv(
    data.frame(id = c("alice", "bob"),
               age = c(30, 40),
               stringsAsFactors = FALSE),
    csv, row.names = FALSE)

  sm <- make_sm(folder_path = dirs$inp, file = "persons.csv",
                encrypted_file = "persons_crypt.csv",
                vars_to_encrypt = "id")

  # Call the engine directly (no dispatcher).
  cryptRopen:::.process_mask_row_in_memory(
    sm = sm, input_path = csv,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE)

  expect_true(file.exists(file.path(dirs$out, "persons_crypt.csv")))
  expect_true(file.exists(file.path(dirs$int, "tc_persons_crypt.parquet")))
  expect_true(exists("persons_crypt",    envir = globalenv()))
  expect_true(exists("id_crypt",         envir = globalenv()))
  expect_true(exists("tc_persons_crypt", envir = globalenv()))
})

test_that("dispatcher .process_mask_row() produces the same files as the in_memory engine", {
  # Two isolated runs on identical inputs; compare byte-for-byte the
  # encrypted CSV and the TC parquet. Proves the dispatcher forwards
  # arguments without transformation.
  d1 <- setup_dirs(); d2 <- setup_dirs()
  pre <- ls(envir = globalenv())
  on.exit({
    unlink(d1$root, recursive = TRUE, force = TRUE)
    unlink(d2$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
  }, add = TRUE)

  src <- data.frame(id = c("alpha", "beta", "gamma"),
                    v  = c(1, 2, 3),
                    stringsAsFactors = FALSE)
  utils::write.csv(src, file.path(d1$inp, "s.csv"), row.names = FALSE)
  utils::write.csv(src, file.path(d2$inp, "s.csv"), row.names = FALSE)

  sm1 <- make_sm(folder_path = d1$inp, file = "s.csv",
                 encrypted_file = "s_crypt.csv", vars_to_encrypt = "id")
  sm2 <- make_sm(folder_path = d2$inp, file = "s.csv",
                 encrypted_file = "s_crypt.csv", vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row(
    sm = sm1, input_path = file.path(d1$inp, "s.csv"),
    output_path = d1$out, intermediate_path = d1$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE)

  cryptRopen:::.process_mask_row_in_memory(
    sm = sm2, input_path = file.path(d2$inp, "s.csv"),
    output_path = d2$out, intermediate_path = d2$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE)

  # Same encrypted dataset (read back, since CSV serialization is stable).
  out1 <- utils::read.csv(file.path(d1$out, "s_crypt.csv"),
                          stringsAsFactors = FALSE)
  out2 <- utils::read.csv(file.path(d2$out, "s_crypt.csv"),
                          stringsAsFactors = FALSE)
  expect_equal(out1, out2)

  # Same correspondence table (read back via arrow, metadata-insensitive).
  tc1 <- as.data.frame(
    arrow::read_parquet(file.path(d1$int, "tc_s_crypt.parquet")))
  tc2 <- as.data.frame(
    arrow::read_parquet(file.path(d2$int, "tc_s_crypt.parquet")))
  expect_equal(tc1, tc2)
})
