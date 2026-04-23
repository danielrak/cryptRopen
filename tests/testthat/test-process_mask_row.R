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
make_mask_row <- function(folder_path, file, encrypted_file,
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

  mask_row <- make_mask_row(
    folder_path     = dirs$inp,
    file            = "persons.csv",
    encrypted_file  = "persons_crypt.csv",
    vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row(
    mask_row             = mask_row,
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

  # Phase 1.D.6.b: globalenv pollution retired. The in_memory engine no
  # longer assigns `<var>_crypt`, `<encrypted_file_stem>` or `tc_*` into
  # globalenv; correspondence tables live in .cryptRopen_env and are
  # accessible via get_correspondence_tables().
  expect_false(exists("id_crypt",      envir = globalenv(), inherits = FALSE))
  expect_false(exists("persons_crypt", envir = globalenv(), inherits = FALSE))
  expect_false(exists("tc_persons_crypt", envir = globalenv(), inherits = FALSE))
  expect_true("tc_persons_crypt" %in% names(get_correspondence_tables()))
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

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "p.csv",
                encrypted_file = "p_crypt.csv",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row(
    mask_row = mask_row, input_path = csv,
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

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "two.csv",
                encrypted_file = "two_crypt.csv",
                vars_to_encrypt = "a, b")

  cryptRopen:::.process_mask_row(
    mask_row = mask_row, input_path = csv,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE)

  out_df <- utils::read.csv(file.path(dirs$out, "two_crypt.csv"),
                            stringsAsFactors = FALSE)
  expect_true(all(c("a_crypt", "b_crypt", "keep") %in% names(out_df)))
  expect_false(any(c("a", "b") %in% names(out_df)))

  # Phase 1.D.6.b: no more globalenv pollution.
  expect_false(exists("a_crypt", envir = globalenv(), inherits = FALSE))
  expect_false(exists("b_crypt", envir = globalenv(), inherits = FALSE))

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

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "r.csv",
                encrypted_file = "r_crypt.csv",
                vars_to_encrypt = "id",
                vars_to_remove  = "to_drop")

  cryptRopen:::.process_mask_row(
    mask_row = mask_row, input_path = csv,
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

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "ghost.csv",
                encrypted_file = "ghost_crypt.csv",
                vars_to_encrypt = "id")

  expect_no_error(
    cryptRopen:::.process_mask_row(
      mask_row = mask_row,
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

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "persons.csv",
                encrypted_file = "persons_crypt.csv",
                vars_to_encrypt = "id")

  # Call the engine directly (no dispatcher).
  cryptRopen:::.process_mask_row_in_memory(
    mask_row = mask_row, input_path = csv,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE)

  expect_true(file.exists(file.path(dirs$out, "persons_crypt.csv")))
  expect_true(file.exists(file.path(dirs$int, "tc_persons_crypt.parquet")))
  # Phase 1.D.6.b: in_memory engine no longer pollutes globalenv. TC is
  # available via the package-private env.
  expect_false(exists("persons_crypt",    envir = globalenv(), inherits = FALSE))
  expect_false(exists("id_crypt",         envir = globalenv(), inherits = FALSE))
  expect_false(exists("tc_persons_crypt", envir = globalenv(), inherits = FALSE))
  expect_true("tc_persons_crypt" %in% names(get_correspondence_tables()))
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

  mask_row1 <- make_mask_row(folder_path = d1$inp, file = "s.csv",
                 encrypted_file = "s_crypt.csv", vars_to_encrypt = "id")
  mask_row2 <- make_mask_row(folder_path = d2$inp, file = "s.csv",
                 encrypted_file = "s_crypt.csv", vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row(
    mask_row = mask_row1, input_path = file.path(d1$inp, "s.csv"),
    output_path = d1$out, intermediate_path = d1$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE)

  cryptRopen:::.process_mask_row_in_memory(
    mask_row = mask_row2, input_path = file.path(d2$inp, "s.csv"),
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

# ---- Locked invariants of the in_memory engine (Phase 1.D.3) -----------
# These tests make explicit contracts that the baseline fixtures cover
# only implicitly (by byte-equality after deserialization). They fail with
# a specific diagnostic message if the invariant is broken, instead of a
# diffuse "file X differs from baseline".

test_that("inspect_*.xlsx layout: 'Obs =' / 'Nvars =' rows then indexed inspect()", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
  }, add = TRUE)

  csv <- file.path(dirs$inp, "persons.csv")
  utils::write.csv(
    data.frame(id = c("alice", "bob", "carol", "dan"),
               age = c(30, 40, 50, 60),
               stringsAsFactors = FALSE),
    csv, row.names = FALSE)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "persons.csv",
                encrypted_file = "persons_crypt.csv",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row_in_memory(
    mask_row = mask_row, input_path = csv,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE)

  inspect_path <- file.path(dirs$out, "inspect_persons_crypt.csv.xlsx")
  expect_true(file.exists(inspect_path))

  # Read raw. writexl writes a header row with the data.frame's column
  # names ("1:nrow(i)", "variables", "types", ...); the semantic rows
  # "Obs =" / "Nvars =" / indexed-inspect start at row 2. Trailing spaces
  # may be stripped on the round-trip, so we compare trimmed strings.
  raw <- readxl::read_xlsx(inspect_path, col_names = FALSE)
  raw <- as.data.frame(raw, stringsAsFactors = FALSE)

  # Row 2: "Obs =" + nrow (4).
  expect_equal(trimws(raw[[1L]][2L]), "Obs =")
  expect_equal(as.character(raw[[2L]][2L]), "4")

  # Row 3: "Nvars =" + nvars in the output (id_crypt + age = 2).
  expect_equal(trimws(raw[[1L]][3L]), "Nvars =")
  expect_equal(as.character(raw[[2L]][3L]), "2")

  # Row 4: first inspect() row, prefixed by a numeric index "1".
  expect_equal(as.character(raw[[1L]][4L]), "1")
})

test_that("TC parquet contains only distinct (original, crypt) pairs", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
  }, add = TRUE)

  # 10 rows but only 3 distinct id values → TC must have exactly 3 rows.
  csv <- file.path(dirs$inp, "dup.csv")
  utils::write.csv(
    data.frame(id = rep(c("a", "b", "c"), length.out = 10),
               v  = 1:10,
               stringsAsFactors = FALSE),
    csv, row.names = FALSE)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "dup.csv",
                encrypted_file = "dup_crypt.csv",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row_in_memory(
    mask_row = mask_row, input_path = csv,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE)

  tc <- as.data.frame(
    arrow::read_parquet(file.path(dirs$int, "tc_dup_crypt.parquet")))

  expect_equal(nrow(tc), 3L)
  # Original values in the TC are post-trim but PRE-normalize (the
  # upper-case is only applied inside crypt_vector() for hashing).
  expect_equal(sort(unique(tc$id)), c("a", "b", "c"))
  expect_equal(anyDuplicated(tc), 0L)
})

test_that("column order: <var>_crypt columns come before the non-encrypted ones", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
  }, add = TRUE)

  csv <- file.path(dirs$inp, "ord.csv")
  utils::write.csv(
    data.frame(a    = c("x", "y"),
               b    = c("p", "q"),
               keep = c(1, 2),
               tail = c(10, 20),
               stringsAsFactors = FALSE),
    csv, row.names = FALSE)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "ord.csv",
                encrypted_file = "ord_crypt.csv",
                vars_to_encrypt = "a, b")

  cryptRopen:::.process_mask_row_in_memory(
    mask_row = mask_row, input_path = csv,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = FALSE)

  out_df <- utils::read.csv(file.path(dirs$out, "ord_crypt.csv"),
                            stringsAsFactors = FALSE)

  # Historical order: encrypted columns first (in the order listed in
  # vars_to_encrypt), then the remaining non-encrypted columns (in their
  # original input order).
  expect_equal(names(out_df), c("a_crypt", "b_crypt", "keep", "tail"))
})

# ---- Phase 1.D.4.a plumbing: engine + chunk_size -----------------------
# At this phase the dispatcher is a no-op for engine/chunk_size: any
# value must yield the same behaviour as the default (in_memory).
# Behaviour is exercised end-to-end in 1.D.4.b/c/d.

test_that(".process_mask_row() ignores engine/chunk_size at 1.D.4.a (same outputs)", {
  # Historical purpose (1.D.4.a): verify that engine / chunk_size are
  # plumbed but unused. Since 1.D.4.b and 1.D.4.c, engine="streaming"
  # is active for parquet/parquet and csv/csv, so this no-op test now
  # exercises the still-passive branches: "in_memory" (explicit) and
  # "auto" (which remains a synonym of in_memory until 1.D.4.d).
  d0 <- setup_dirs(); d1 <- setup_dirs(); d2 <- setup_dirs()
  pre <- ls(envir = globalenv())
  on.exit({
    unlink(d0$root, recursive = TRUE, force = TRUE)
    unlink(d1$root, recursive = TRUE, force = TRUE)
    unlink(d2$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
  }, add = TRUE)

  src <- data.frame(id = c("a", "b", "c", "a"),
                    v  = 1:4,
                    stringsAsFactors = FALSE)
  for (d in list(d0, d1, d2)) {
    utils::write.csv(src, file.path(d$inp, "s.csv"), row.names = FALSE)
  }

  mask_row <- make_mask_row(folder_path = d0$inp, file = "s.csv",
                encrypted_file = "s_crypt.csv", vars_to_encrypt = "id")

  call_dispatch <- function(d, ...) {
    cryptRopen:::.process_mask_row(
      mask_row = mask_row, input_path = file.path(d$inp, "s.csv"),
      output_path = d$out, intermediate_path = d$int,
      encryption_key = "k", algorithm = "md5",
      correspondence_table = TRUE, ...)
  }

  call_dispatch(d0)                                         # default args
  call_dispatch(d1, engine = "in_memory", chunk_size = 2L)  # explicit no-op
  call_dispatch(d2, engine = "auto",      chunk_size = 100L)

  read_out <- function(d) utils::read.csv(
    file.path(d$out, "s_crypt.csv"), stringsAsFactors = FALSE)
  read_tc  <- function(d) as.data.frame(
    arrow::read_parquet(file.path(d$int, "tc_s_crypt.parquet")))

  expect_equal(read_out(d0), read_out(d1))
  expect_equal(read_out(d0), read_out(d2))
  expect_equal(read_tc(d0),  read_tc(d1))
  expect_equal(read_tc(d0),  read_tc(d2))
})

test_that("crypt_r() rejects an unknown engine via match.arg()", {
  # match.arg() fires before any I/O, so invalid args suffice.
  # Do not match the message string (it is locale-dependent — English
  # "should be one of" vs French "doit être un de"). Asserting that
  # an error is raised is enough; the backtrace points to match.arg.
  expect_error(
    crypt_r(mask_folder_path = tempdir(), mask_file = "nope.xlsx",
            output_path = tempdir(), intermediate_path = tempdir(),
            encryption_key = "k", engine = "unknown_engine"))
})

test_that("crypt_r() rejects an invalid chunk_size via stopifnot()", {
  expect_error(
    crypt_r(mask_folder_path = tempdir(), mask_file = "nope.xlsx",
            output_path = tempdir(), intermediate_path = tempdir(),
            encryption_key = "k", chunk_size = -5L))
  expect_error(
    crypt_r(mask_folder_path = tempdir(), mask_file = "nope.xlsx",
            output_path = tempdir(), intermediate_path = tempdir(),
            encryption_key = "k", chunk_size = NA_integer_))
  expect_error(
    crypt_r(mask_folder_path = tempdir(), mask_file = "nope.xlsx",
            output_path = tempdir(), intermediate_path = tempdir(),
            encryption_key = "k", chunk_size = c(1L, 2L)))
})

# ---- Phase 1.D.4.b — parquet streaming engine --------------------------
# These tests exercise .process_mask_row_streaming() both directly and
# through the dispatcher. Design contract validated with the user on
# 2026-04-22 (Option B): the streaming engine does NOT pollute
# globalenv(); the correspondence table is only in .cryptRopen_env +
# on disk. Fallback (engine="streaming" with non-parquet endpoints) is
# also asserted so the baseline stays green.

# Small helper — parquet fixture.
write_parquet_fixture <- function(path, df) {
  arrow::write_parquet(df, path)
}

# Ordered schema: the streaming test references this exact schema to
# assert that streaming output ≈ in_memory output at the row-set level.
# (Row order may be preserved chunk-by-chunk in streaming; we compare
# via set-of-rows semantics to stay robust.)
rowset <- function(df) {
  df <- dplyr::arrange(dplyr::as_tibble(df),
                       dplyr::across(dplyr::everything()))
  as.data.frame(df)
}

test_that("streaming (dispatcher): parquet-in/parquet-out happy path", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  src <- data.frame(id = c("alice", "bob", "carol", " alice ", ""),
                    age = c(30L, 40L, 50L, 30L, 99L),
                    stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "persons.parquet")
  write_parquet_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "persons.parquet",
                encrypted_file  = "persons_crypt.parquet",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "testkey", algorithm = "md5",
    correspondence_table = TRUE,
    engine = "streaming", chunk_size = 2L)

  # Files are where the in_memory engine would have put them.
  expect_true(file.exists(file.path(dirs$out, "persons_crypt.parquet")))
  expect_true(file.exists(file.path(dirs$int, "tc_persons_crypt.parquet")))
  expect_true(file.exists(file.path(dirs$out,
                                    "inspect_persons_crypt.parquet.xlsx")))

  out_df <- as.data.frame(arrow::read_parquet(
    file.path(dirs$out, "persons_crypt.parquet")))

  # Column order: encrypted first, then non-encrypted in input order.
  expect_equal(names(out_df), c("id_crypt", "age"))
  expect_equal(nrow(out_df), 5L)
  # Trimmed duplicate "alice" hashes identically to "alice".
  expect_equal(out_df$id_crypt[1], out_df$id_crypt[4])
  # Empty string becomes NA after cleanup.
  expect_true(is.na(out_df$id_crypt[5]))
})

test_that("streaming: no globalenv pollution (Option B)", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  src <- data.frame(id = c("a", "b", "c"),
                    stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "p.parquet")
  write_parquet_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "p.parquet",
                encrypted_file  = "p_crypt.parquet",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row_streaming(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE, chunk_size = 1L)

  # None of these historical names must appear in globalenv.
  expect_false(exists("id_crypt",    envir = globalenv(), inherits = FALSE))
  expect_false(exists("p_crypt",     envir = globalenv(), inherits = FALSE))
  expect_false(exists("tc_p_crypt",  envir = globalenv(), inherits = FALSE))
})

test_that("streaming: correspondence table is available via get_correspondence_tables()", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  cryptRopen:::.clear_correspondence_tables()

  src <- data.frame(id = c("a", "b", "a", "c"),
                    stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "p.parquet")
  write_parquet_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "p.parquet",
                encrypted_file  = "p_crypt.parquet",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row_streaming(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE, chunk_size = 2L)

  tcs <- get_correspondence_tables()
  expect_true("tc_p_crypt" %in% names(tcs))
  tc <- tcs[["tc_p_crypt"]]
  expect_equal(sort(names(tc)), sort(c("id", "id_crypt")))
  # 3 distinct originals: a, b, c (chunks merged then deduped).
  expect_equal(sort(unique(tc$id)), c("a", "b", "c"))
  expect_equal(anyDuplicated(tc), 0L)

  # Same content on disk as in memory.
  tc_disk <- as.data.frame(arrow::read_parquet(
    file.path(dirs$int, "tc_p_crypt.parquet")))
  expect_equal(rowset(tc), rowset(tc_disk))
})

test_that("streaming: outputs are invariant under chunk_size", {
  make_run <- function(chunk_size) {
    dirs <- setup_dirs()
    src  <- data.frame(id  = sprintf("v%03d", 1:30),
                       grp = rep(c("A", "B", "C"), length.out = 30),
                       stringsAsFactors = FALSE)
    inp <- file.path(dirs$inp, "big.parquet")
    write_parquet_fixture(inp, src)
    mask_row <- make_mask_row(folder_path = dirs$inp, file = "big.parquet",
                  encrypted_file  = "big_crypt.parquet",
                  vars_to_encrypt = "id")
    cryptRopen:::.process_mask_row_streaming(
      mask_row = mask_row, input_path = inp,
      output_path = dirs$out, intermediate_path = dirs$int,
      encryption_key = "k", algorithm = "md5",
      correspondence_table = TRUE, chunk_size = chunk_size)
    list(
      dirs = dirs,
      out  = as.data.frame(arrow::read_parquet(
        file.path(dirs$out, "big_crypt.parquet"))),
      tc   = as.data.frame(arrow::read_parquet(
        file.path(dirs$int, "tc_big_crypt.parquet")))
    )
  }

  pre <- ls(envir = globalenv())
  r1    <- make_run(1L)
  r10   <- make_run(10L)
  rbig  <- make_run(1e6L)

  on.exit({
    unlink(r1$dirs$root,   recursive = TRUE, force = TRUE)
    unlink(r10$dirs$root,  recursive = TRUE, force = TRUE)
    unlink(rbig$dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  # Output rowset identical across chunk sizes (order may differ since
  # arrow RecordBatch boundaries shift; content must not).
  expect_equal(rowset(r1$out),   rowset(r10$out))
  expect_equal(rowset(r10$out),  rowset(rbig$out))
  expect_equal(rowset(r1$tc),    rowset(r10$tc))
  expect_equal(rowset(r10$tc),   rowset(rbig$tc))
})

test_that("streaming: inspect_*.xlsx has the same layout as in_memory", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  src <- data.frame(id = c("a", "b", "c", "d"),
                    v  = 1:4,
                    stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "t.parquet")
  write_parquet_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "t.parquet",
                encrypted_file  = "t_crypt.parquet",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row_streaming(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE, chunk_size = 2L)

  xlsx_path <- file.path(dirs$out, "inspect_t_crypt.parquet.xlsx")
  expect_true(file.exists(xlsx_path))

  skip_if_not_installed("readxl")
  sheet <- readxl::read_excel(xlsx_path, col_names = FALSE)
  # writexl inserts a column-name header as row 1, so we expect:
  #   row 2 -> "Obs = " / n_rows
  #   row 3 -> "Nvars = " / n_inspect_rows
  #   row 4+ -> indexed inspect rows
  expect_equal(trimws(as.character(sheet[[1]][2])), "Obs =")
  expect_equal(trimws(as.character(sheet[[1]][3])), "Nvars =")
  expect_equal(as.integer(sheet[[2]][2]), 4L)        # n rows of output
})

test_that("streaming: vars_to_remove drops columns from the output", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  src <- data.frame(id   = c("a", "b"),
                    keep = c(1L, 2L),
                    drop = c("x", "y"),
                    stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "r.parquet")
  write_parquet_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "r.parquet",
                encrypted_file  = "r_crypt.parquet",
                vars_to_encrypt = "id",
                vars_to_remove  = "drop")

  cryptRopen:::.process_mask_row_streaming(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE, chunk_size = 10L)

  out_df <- as.data.frame(arrow::read_parquet(
    file.path(dirs$out, "r_crypt.parquet")))
  expect_equal(names(out_df), c("id_crypt", "keep"))
})

test_that("streaming: correspondence_table = FALSE writes no TC anywhere", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  cryptRopen:::.clear_correspondence_tables()

  src <- data.frame(id = c("a", "b"), stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "n.parquet")
  write_parquet_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "n.parquet",
                encrypted_file  = "n_crypt.parquet",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row_streaming(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = FALSE, chunk_size = 10L)

  expect_false(file.exists(file.path(dirs$int, "tc_n_crypt.parquet")))
  expect_false("tc_n_crypt" %in% names(get_correspondence_tables()))
  expect_true(file.exists(file.path(dirs$out, "n_crypt.parquet")))
})

test_that("dispatcher: engine='streaming' with non-streamable input falls back to in_memory", {
  # Streaming is wired for parquet-in/out (1.D.4.b) and csv-in/out
  # (1.D.4.c). Any other input format (here, .rds) must fall back to
  # the in_memory engine. Since 1.D.6.b, no engine pollutes globalenv —
  # the fallback now shares that clean contract.
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  rds <- file.path(dirs$inp, "s.rds")
  saveRDS(data.frame(id = c("a", "b"), stringsAsFactors = FALSE), rds)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "s.rds",
                encrypted_file  = "s_crypt.rds",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row(
    mask_row = mask_row, input_path = rds,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE,
    engine = "streaming", chunk_size = 4L)

  # Output exists in the historical format (rds — written via rio).
  expect_true(file.exists(file.path(dirs$out, "s_crypt.rds")))
  # Phase 1.D.6.b: the fallback no longer pollutes globalenv.
  expect_false(exists("s_crypt", envir = globalenv(), inherits = FALSE))
})

test_that("dispatcher: engine='streaming' with non-parquet output falls back to in_memory", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  src <- data.frame(id = c("a", "b"), stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "mix.parquet")
  write_parquet_fixture(inp, src)

  # Output declared as CSV — streaming only handles parquet-out.
  mask_row <- make_mask_row(folder_path = dirs$inp, file = "mix.parquet",
                encrypted_file  = "mix_crypt.csv",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE,
    engine = "streaming", chunk_size = 1L)

  expect_true(file.exists(file.path(dirs$out, "mix_crypt.csv")))
  # Phase 1.D.6.b: the fallback no longer pollutes globalenv.
  expect_false(exists("mix_crypt", envir = globalenv(), inherits = FALSE))
})

# Test previously asserting that engine="auto" remained a synonym of
# in_memory in 1.D.4.b was removed in 1.D.4.d: auto now actively routes
# to streaming when possible. The symmetric assertion (auto routes
# parquet/parquet to streaming) lives in the 1.D.4.d section below.

# ---- Phase 1.D.4.c — CSV streaming engine ------------------------------
# Same design contract as the parquet streaming engine: no globalenv
# pollution (Option B), TC in .cryptRopen_env + on disk (as parquet),
# fallback to in_memory on mixed endpoints. Byte-level equality of the
# CSV output vs in_memory is NOT guaranteed (different writer), but
# re-read semantic equality is.

# Small helper — CSV fixture written via utils::write.csv, which is
# what the in_memory engine produces via rio::export. This is the
# "reference" CSV format users create.
write_csv_fixture <- function(path, df) {
  utils::write.csv(df, path, row.names = FALSE)
}

test_that("csv streaming (dispatcher): csv-in/csv-out happy path", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  src <- data.frame(id = c("alice", "bob", "carol", " alice ", ""),
                    age = c(30L, 40L, 50L, 30L, 99L),
                    stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "persons.csv")
  write_csv_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "persons.csv",
                encrypted_file  = "persons_crypt.csv",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "testkey", algorithm = "md5",
    correspondence_table = TRUE,
    engine = "streaming", chunk_size = 2L)

  expect_true(file.exists(file.path(dirs$out, "persons_crypt.csv")))
  expect_true(file.exists(file.path(dirs$int, "tc_persons_crypt.parquet")))
  expect_true(file.exists(file.path(dirs$out,
                                    "inspect_persons_crypt.csv.xlsx")))

  out_df <- as.data.frame(arrow::read_csv_arrow(
    file.path(dirs$out, "persons_crypt.csv")))

  # Column order: encrypted first, then non-encrypted in input order.
  expect_equal(names(out_df), c("id_crypt", "age"))
  expect_equal(nrow(out_df), 5L)
  # Trimmed duplicate "alice" hashes like "alice".
  expect_equal(out_df$id_crypt[1], out_df$id_crypt[4])
  # Empty string becomes NA.
  expect_true(is.na(out_df$id_crypt[5]))
})

test_that("csv streaming: no globalenv pollution (Option B)", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  src <- data.frame(id = c("a", "b", "c"), stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "p.csv")
  write_csv_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "p.csv",
                encrypted_file  = "p_crypt.csv",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row_csv_streaming(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE, chunk_size = 1L)

  expect_false(exists("id_crypt",   envir = globalenv(), inherits = FALSE))
  expect_false(exists("p_crypt",    envir = globalenv(), inherits = FALSE))
  expect_false(exists("tc_p_crypt", envir = globalenv(), inherits = FALSE))
})

test_that("csv streaming: TC available via get_correspondence_tables()", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  cryptRopen:::.clear_correspondence_tables()

  src <- data.frame(id = c("a", "b", "a", "c"),
                    stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "p.csv")
  write_csv_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "p.csv",
                encrypted_file  = "p_crypt.csv",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row_csv_streaming(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE, chunk_size = 2L)

  tcs <- get_correspondence_tables()
  expect_true("tc_p_crypt" %in% names(tcs))
  tc <- tcs[["tc_p_crypt"]]
  expect_equal(sort(names(tc)), sort(c("id", "id_crypt")))
  expect_equal(sort(unique(tc$id)), c("a", "b", "c"))
  expect_equal(anyDuplicated(tc), 0L)

  tc_disk <- as.data.frame(arrow::read_parquet(
    file.path(dirs$int, "tc_p_crypt.parquet")))
  expect_equal(rowset(tc), rowset(tc_disk))
})

test_that("csv streaming: outputs are invariant under chunk_size", {
  make_run <- function(chunk_size) {
    dirs <- setup_dirs()
    src  <- data.frame(id  = sprintf("v%03d", 1:30),
                       grp = rep(c("A", "B", "C"), length.out = 30),
                       stringsAsFactors = FALSE)
    inp <- file.path(dirs$inp, "big.csv")
    write_csv_fixture(inp, src)
    mask_row <- make_mask_row(folder_path = dirs$inp, file = "big.csv",
                  encrypted_file  = "big_crypt.csv",
                  vars_to_encrypt = "id")
    cryptRopen:::.process_mask_row_csv_streaming(
      mask_row = mask_row, input_path = inp,
      output_path = dirs$out, intermediate_path = dirs$int,
      encryption_key = "k", algorithm = "md5",
      correspondence_table = TRUE, chunk_size = chunk_size)
    list(
      dirs = dirs,
      out  = as.data.frame(arrow::read_csv_arrow(
        file.path(dirs$out, "big_crypt.csv"))),
      tc   = as.data.frame(arrow::read_parquet(
        file.path(dirs$int, "tc_big_crypt.parquet")))
    )
  }

  pre   <- ls(envir = globalenv())
  r1    <- make_run(1L)
  r10   <- make_run(10L)
  rbig  <- make_run(1e6L)

  on.exit({
    unlink(r1$dirs$root,   recursive = TRUE, force = TRUE)
    unlink(r10$dirs$root,  recursive = TRUE, force = TRUE)
    unlink(rbig$dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  expect_equal(rowset(r1$out),   rowset(r10$out))
  expect_equal(rowset(r10$out),  rowset(rbig$out))
  expect_equal(rowset(r1$tc),    rowset(r10$tc))
  expect_equal(rowset(r10$tc),   rowset(rbig$tc))
})

test_that("csv streaming: inspect_*.xlsx layout matches in_memory", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  src <- data.frame(id = c("a", "b", "c", "d"),
                    v  = 1:4,
                    stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "t.csv")
  write_csv_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "t.csv",
                encrypted_file  = "t_crypt.csv",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row_csv_streaming(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE, chunk_size = 2L)

  xlsx_path <- file.path(dirs$out, "inspect_t_crypt.csv.xlsx")
  expect_true(file.exists(xlsx_path))

  skip_if_not_installed("readxl")
  sheet <- readxl::read_excel(xlsx_path, col_names = FALSE)
  expect_equal(trimws(as.character(sheet[[1]][2])), "Obs =")
  expect_equal(trimws(as.character(sheet[[1]][3])), "Nvars =")
  expect_equal(as.integer(sheet[[2]][2]), 4L)
})

test_that("csv streaming: vars_to_remove drops columns from the output", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  src <- data.frame(id   = c("a", "b"),
                    keep = c(1L, 2L),
                    drop = c("x", "y"),
                    stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "r.csv")
  write_csv_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "r.csv",
                encrypted_file  = "r_crypt.csv",
                vars_to_encrypt = "id",
                vars_to_remove  = "drop")

  cryptRopen:::.process_mask_row_csv_streaming(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE, chunk_size = 10L)

  out_df <- as.data.frame(arrow::read_csv_arrow(
    file.path(dirs$out, "r_crypt.csv")))
  expect_equal(names(out_df), c("id_crypt", "keep"))
})

test_that("csv streaming: correspondence_table = FALSE writes no TC anywhere", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  cryptRopen:::.clear_correspondence_tables()

  src <- data.frame(id = c("a", "b"), stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "n.csv")
  write_csv_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "n.csv",
                encrypted_file  = "n_crypt.csv",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row_csv_streaming(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = FALSE, chunk_size = 10L)

  expect_false(file.exists(file.path(dirs$int, "tc_n_crypt.parquet")))
  expect_false("tc_n_crypt" %in% names(get_correspondence_tables()))
  expect_true(file.exists(file.path(dirs$out, "n_crypt.csv")))
})

test_that("dispatcher: engine='streaming' parquet-in + csv-out falls back to in_memory", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  src <- data.frame(id = c("a", "b"), stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "m.parquet")
  write_parquet_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "m.parquet",
                encrypted_file  = "m_crypt.csv",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE,
    engine = "streaming", chunk_size = 1L)

  # Fell back to in_memory. Phase 1.D.6.b: no globalenv pollution.
  expect_true(file.exists(file.path(dirs$out, "m_crypt.csv")))
  expect_false(exists("m_crypt", envir = globalenv(), inherits = FALSE))
})

test_that("dispatcher: engine='streaming' csv-in + parquet-out falls back to in_memory", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  src <- data.frame(id = c("a", "b"), stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "mix.csv")
  write_csv_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "mix.csv",
                encrypted_file  = "mix_crypt.parquet",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE,
    engine = "streaming", chunk_size = 1L)

  expect_true(file.exists(file.path(dirs$out, "mix_crypt.parquet")))
  # Phase 1.D.6.b: the fallback no longer pollutes globalenv.
  expect_false(exists("mix_crypt", envir = globalenv(), inherits = FALSE))
})

# ---- Phase 1.D.4.d — auto routing rule ---------------------------------
# `engine = "auto"` now behaves as `engine = "streaming"` for streamable
# endpoints (parquet/parquet, csv/csv) and falls back to in_memory
# otherwise. These blocks verify both branches:
#   - auto + parquet/parquet → streaming (no globalenv pollution, TC in
#     .cryptRopen_env, no direct tc_* assignment).
#   - auto + csv/csv         → streaming (same signature).
#   - auto + rds             → in_memory (since 1.D.6.b, also clean).
#   - auto + mixed endpoints → in_memory (fallback, also clean).

test_that("auto routing (1.D.4.d): engine='auto' routes parquet/parquet to the streaming engine", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  src <- data.frame(id = c("alice", "bob", "carol"),
                    age = c(30L, 40L, 50L),
                    stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "a.parquet")
  write_parquet_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "a.parquet",
                encrypted_file  = "a_crypt.parquet",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE,
    engine = "auto", chunk_size = 1L)

  # Streaming signature — no globalenv pollution; TC in .cryptRopen_env.
  expect_false(exists("a_crypt",    envir = globalenv(), inherits = FALSE))
  expect_false(exists("tc_a_crypt", envir = globalenv(), inherits = FALSE))
  expect_false(exists("id_crypt",   envir = globalenv(), inherits = FALSE))
  expect_true(file.exists(file.path(dirs$out, "a_crypt.parquet")))
  expect_true(file.exists(file.path(dirs$int, "tc_a_crypt.parquet")))

  tcs <- cryptRopen::get_correspondence_tables()
  expect_true("tc_a_crypt" %in% names(tcs))

  # Cross-check: auto and explicit streaming yield the same row set.
  dirs2 <- setup_dirs()
  on.exit(unlink(dirs2$root, recursive = TRUE, force = TRUE), add = TRUE)
  inp2 <- file.path(dirs2$inp, "a.parquet")
  write_parquet_fixture(inp2, src)
  mask_row2 <- make_mask_row(folder_path = dirs2$inp, file = "a.parquet",
                 encrypted_file  = "a_crypt.parquet",
                 vars_to_encrypt = "id")
  cryptRopen:::.process_mask_row(
    mask_row = mask_row2, input_path = inp2,
    output_path = dirs2$out, intermediate_path = dirs2$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE,
    engine = "streaming", chunk_size = 1L)

  auto_out   <- as.data.frame(arrow::read_parquet(
    file.path(dirs$out,  "a_crypt.parquet")))
  stream_out <- as.data.frame(arrow::read_parquet(
    file.path(dirs2$out, "a_crypt.parquet")))
  expect_equal(rowset(auto_out), rowset(stream_out))
})

test_that("auto routing (1.D.4.d): engine='auto' routes csv/csv to the csv streaming engine", {
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  src <- data.frame(id = c("alice", "bob", "carol"),
                    age = c(30L, 40L, 50L),
                    stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "a.csv")
  write_csv_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "a.csv",
                encrypted_file  = "a_crypt.csv",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE,
    engine = "auto", chunk_size = 2L)

  # Streaming signature — no globalenv pollution.
  expect_false(exists("a_crypt",    envir = globalenv(), inherits = FALSE))
  expect_false(exists("tc_a_crypt", envir = globalenv(), inherits = FALSE))
  expect_false(exists("id_crypt",   envir = globalenv(), inherits = FALSE))
  expect_true(file.exists(file.path(dirs$out, "a_crypt.csv")))
  expect_true(file.exists(file.path(dirs$int, "tc_a_crypt.parquet")))

  tcs <- cryptRopen::get_correspondence_tables()
  expect_true("tc_a_crypt" %in% names(tcs))

  # Cross-check: auto and explicit streaming yield the same row set.
  dirs2 <- setup_dirs()
  on.exit(unlink(dirs2$root, recursive = TRUE, force = TRUE), add = TRUE)
  inp2 <- file.path(dirs2$inp, "a.csv")
  write_csv_fixture(inp2, src)
  mask_row2 <- make_mask_row(folder_path = dirs2$inp, file = "a.csv",
                 encrypted_file  = "a_crypt.csv",
                 vars_to_encrypt = "id")
  cryptRopen:::.process_mask_row(
    mask_row = mask_row2, input_path = inp2,
    output_path = dirs2$out, intermediate_path = dirs2$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE,
    engine = "streaming", chunk_size = 2L)

  auto_out   <- as.data.frame(arrow::read_csv_arrow(
    file.path(dirs$out,  "a_crypt.csv")))
  stream_out <- as.data.frame(arrow::read_csv_arrow(
    file.path(dirs2$out, "a_crypt.csv")))
  expect_equal(rowset(auto_out), rowset(stream_out))
})

test_that("auto routing (1.D.4.d): engine='auto' with non-streamable input falls back to in_memory", {
  # rds is not streamable — auto must route to in_memory. Phase 1.D.6.b
  # retired the in_memory globalenv side effect, so the fallback is
  # also clean.
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  rds <- file.path(dirs$inp, "s.rds")
  saveRDS(data.frame(id = c("a", "b"), stringsAsFactors = FALSE), rds)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "s.rds",
                encrypted_file  = "s_crypt.rds",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row(
    mask_row = mask_row, input_path = rds,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE,
    engine = "auto", chunk_size = 4L)

  expect_true(file.exists(file.path(dirs$out, "s_crypt.rds")))
  # Phase 1.D.6.b: in_memory engine no longer pollutes globalenv.
  expect_false(exists("s_crypt", envir = globalenv(), inherits = FALSE))
})

test_that("auto routing (1.D.4.d): engine='auto' with mixed endpoints falls back to in_memory", {
  # parquet-in / csv-out — neither streaming engine applies.
  dirs <- setup_dirs()
  pre  <- ls(envir = globalenv())
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    clean_globals(pre)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  src <- data.frame(id = c("a", "b"), stringsAsFactors = FALSE)
  inp <- file.path(dirs$inp, "mix.parquet")
  write_parquet_fixture(inp, src)

  mask_row <- make_mask_row(folder_path = dirs$inp, file = "mix.parquet",
                encrypted_file  = "mix_crypt.csv",
                vars_to_encrypt = "id")

  cryptRopen:::.process_mask_row(
    mask_row = mask_row, input_path = inp,
    output_path = dirs$out, intermediate_path = dirs$int,
    encryption_key = "k", algorithm = "md5",
    correspondence_table = TRUE,
    engine = "auto", chunk_size = 1L)

  expect_true(file.exists(file.path(dirs$out, "mix_crypt.csv")))
  # Phase 1.D.6.b: in_memory engine no longer pollutes globalenv.
  expect_false(exists("mix_crypt", envir = globalenv(), inherits = FALSE))
})
