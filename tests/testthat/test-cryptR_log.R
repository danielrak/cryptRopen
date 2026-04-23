# tests/testthat/test-cryptR_log.R
#
# Phase 1.D.6.c — unit tests for the log-writing + TC re-injection
# helpers introduced in R/cryptR_log.R. These run SYNCHRONOUSLY without
# real mirai daemons so they are cheap and deterministic, complementing
# the end-to-end coverage in test-crypt_r_async.R.
#
# Targets:
#   .extract_row_result() — payload homogenisation (4 branches)
#   .reinject_correspondence_tables() — side effect on .cryptRopen_env
#   .build_crypt_r_log_df() — column order, empty-case typing
#   .log_already_written() — mtime comparison semantics
#   .write_crypt_r_log_file() — filename + tolerated write failures
#   .finalize_job_side_effects() — integration


# ---- Fixtures ----------------------------------------------------------

# Build a list shaped exactly like `.make_row_result()` emits.
make_ok_result <- function(encrypted_file = "persons_crypt.csv",
                           tc_name = "tc_persons_crypt",
                           tc_df = data.frame(
                             id = "a", id_crypt = "h",
                             stringsAsFactors = FALSE
                           ),
                           n_rows = 3L,
                           out_path = NULL) {
  size_b <- if (is.null(out_path) || !file.exists(out_path)) {
    NA_real_
  } else {
    as.numeric(file.info(out_path)$size)
  }
  sha <- NA_character_
  list(
    success = TRUE,
    error_message = NA_character_,
    tc_name = tc_name,
    tc_df = tc_df,
    metrics = list(
      start_time             = Sys.time() - 1,
      end_time               = Sys.time(),
      duration_sec           = 1.0,
      n_rows_processed       = n_rows,
      output_file_size_bytes = size_b,
      output_file_sha256     = sha
    )
  )
}

# Minimal cryptR_job stub. output_path defaults to a fresh tempdir.
make_job_stub <- function(tasks = list(), mask_rows = NULL,
                          output_path = NULL,
                          started_at = Sys.time()) {
  if (is.null(output_path)) {
    output_path <- tempfile("crypt_r_log_")
    dir.create(output_path, recursive = TRUE)
  }
  if (is.null(mask_rows)) {
    mask_rows <- data.frame(
      encrypted_file = names(tasks) %||% character(0),
      stringsAsFactors = FALSE
    )
  }
  cryptRopen:::.new_cryptR_job(
    tasks             = tasks,
    mask_rows         = mask_rows,
    output_path       = output_path,
    intermediate_path = tempfile("int_"),
    started_at        = started_at
  )
}

# Small infix for default-on-NULL — keeps the fixtures terse.
`%||%` <- function(a, b) if (is.null(a)) b else a


# ---- .extract_row_result() --------------------------------------------

test_that(".extract_row_result(): resolved, expected shape is returned as-is", {
  r <- make_ok_result()
  task <- structure(list(data = r), class = "mirai")
  out <- cryptRopen:::.extract_row_result(task, encrypted_file = "x")
  expect_identical(out, r)
})

test_that(".extract_row_result(): resolved, unexpected shape becomes a trivial success", {
  # Bare integer — what an ad-hoc test mirai often returns.
  task <- structure(list(data = 7L), class = "mirai")
  out <- cryptRopen:::.extract_row_result(task, encrypted_file = "x")
  expect_true(out$success)
  expect_true(is.na(out$error_message))
  expect_true(is.na(out$tc_name))
  expect_null(out$tc_df)
})

test_that(".extract_row_result(): non-mirai input (hand-built stand-in) yields a trivial success", {
  out <- cryptRopen:::.extract_row_result(
    task = NULL,
    encrypted_file = "x"
  )
  expect_true(out$success)
  expect_true(is.na(out$error_message))
})


# ---- .reinject_correspondence_tables() --------------------------------

test_that(".reinject_correspondence_tables(): success + TC populates .cryptRopen_env", {
  cryptRopen:::.clear_correspondence_tables()
  r <- make_ok_result(
    tc_name = "tc_demo",
    tc_df = data.frame(
      id = "a", id_crypt = "h",
      stringsAsFactors = FALSE
    )
  )
  cryptRopen:::.reinject_correspondence_tables(list(r))
  tcs <- cryptRopen::get_correspondence_tables()
  expect_true("tc_demo" %in% names(tcs))
  expect_equal(tcs$tc_demo$id, "a")

  # Isolate the test from whatever follows.
  cryptRopen:::.clear_correspondence_tables()
})

test_that(".reinject_correspondence_tables(): failure or missing TC is skipped silently", {
  cryptRopen:::.clear_correspondence_tables()
  # Failure row — no TC to inject.
  r_fail <- list(
    success = FALSE, error_message = "boom",
    tc_name = NA_character_, tc_df = NULL,
    metrics = list()
  )
  # Success row without a TC (e.g. correspondence_table = FALSE path).
  r_notc <- list(
    success = TRUE, error_message = NA_character_,
    tc_name = NA_character_, tc_df = NULL,
    metrics = list()
  )
  cryptRopen:::.reinject_correspondence_tables(list(r_fail, r_notc))
  expect_length(cryptRopen::get_correspondence_tables(), 0L)
})


# ---- .build_crypt_r_log_df() ------------------------------------------

test_that(".build_crypt_r_log_df(): empty mask returns a typed zero-row data.frame", {
  job <- make_job_stub(
    tasks = list(),
    mask_rows = data.frame(
      encrypted_file = character(0),
      stringsAsFactors = FALSE
    )
  )
  df <- cryptRopen:::.build_crypt_r_log_df(job, list())
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 0L)
  # Column types match the populated case.
  expect_type(df$success, "logical")
  expect_type(df$error_message, "character")
  expect_s3_class(df$start_time, "POSIXct")
  expect_type(df$duration_sec, "double")
  expect_type(df$n_rows_processed, "integer")
  expect_type(df$output_file_sha256, "character")
})

test_that(".build_crypt_r_log_df(): populated case carries mask columns first, metrics after", {
  mask <- data.frame(
    folder_path = "/in",
    file = "x.csv",
    encrypted_file = "x_crypt.csv",
    to_encrypt = "X",
    stringsAsFactors = FALSE
  )
  job <- make_job_stub(
    tasks = list(x_crypt.csv = NA),
    mask_rows = mask
  )
  results <- list(make_ok_result(n_rows = 42L))

  df <- cryptRopen:::.build_crypt_r_log_df(job, results)
  expect_equal(nrow(df), 1L)
  # Mask columns come first, in original order.
  expect_equal(
    names(df)[1:4],
    c("folder_path", "file", "encrypted_file", "to_encrypt")
  )
  # Metrics columns follow.
  expect_true(all(c(
    "success", "error_message", "start_time",
    "end_time", "duration_sec", "n_rows_processed",
    "output_file_size_bytes", "output_file_sha256"
  )
  %in% names(df)))
  expect_true(df$success)
  expect_equal(df$n_rows_processed, 42L)
})


# ---- .log_already_written() -------------------------------------------

test_that(".log_already_written(): FALSE when output_path is missing or empty", {
  # Non-existent directory.
  job <- make_job_stub(tasks = list())
  unlink(job$output_path, recursive = TRUE, force = TRUE)
  expect_false(cryptRopen:::.log_already_written(job))

  # Existing but empty directory.
  dir.create(job$output_path, recursive = TRUE)
  expect_false(cryptRopen:::.log_already_written(job))
})

test_that(".log_already_written(): TRUE only when a matching file has mtime >= started_at", {
  job <- make_job_stub(
    tasks = list(),
    started_at = Sys.time()
  )
  # Stale log from a previous run — predates started_at.
  stale <- file.path(job$output_path, "log_crypt_r_19991231_235959.xlsx")
  file.create(stale)
  Sys.setFileTime(stale, Sys.time() - 3600)
  expect_false(cryptRopen:::.log_already_written(job))

  # Fresh log — post started_at.
  fresh <- file.path(job$output_path, "log_crypt_r_20991231_235959.xlsx")
  file.create(fresh)
  Sys.setFileTime(fresh, Sys.time() + 5)
  expect_true(cryptRopen:::.log_already_written(job))
})


# ---- .write_crypt_r_log_file() ----------------------------------------

test_that(".write_crypt_r_log_file(): produces a file named log_crypt_r_<ts>.xlsx", {
  skip_if_not_installed("writexl")
  job <- make_job_stub(
    tasks = list(),
    mask_rows = data.frame(
      encrypted_file = character(0),
      stringsAsFactors = FALSE
    )
  )
  path <- cryptRopen:::.write_crypt_r_log_file(job, list())
  expect_true(!is.na(path))
  expect_true(file.exists(path))
  expect_match(basename(path), "^log_crypt_r_[0-9]{8}_[0-9]{6}\\.xlsx$")
})

test_that(".write_crypt_r_log_file(): returns NA_character_ when the write fails", {
  # Missing output_path directory → writexl cannot create the file.
  job <- make_job_stub(
    tasks = list(),
    mask_rows = data.frame(
      encrypted_file = character(0),
      stringsAsFactors = FALSE
    )
  )
  unlink(job$output_path, recursive = TRUE, force = TRUE)
  path <- cryptRopen:::.write_crypt_r_log_file(job, list())
  expect_true(is.na(path))
})


# ---- .finalize_job_side_effects() -------------------------------------

test_that(".finalize_job_side_effects(): writes the log AND re-injects TCs in one call", {
  skip_if_not_installed("writexl")
  cryptRopen:::.clear_correspondence_tables()
  # Hand-built stand-in (non-mirai task) whose $data is the typed result.
  r <- make_ok_result(
    tc_name = "tc_final",
    tc_df = data.frame(
      id = "a", id_crypt = "h",
      stringsAsFactors = FALSE
    )
  )
  task <- structure(list(data = r), class = "mirai")

  mask <- data.frame(
    folder_path = "/in",
    file = "x.csv",
    encrypted_file = "persons_crypt.csv",
    to_encrypt = "X",
    stringsAsFactors = FALSE
  )
  job <- make_job_stub(
    tasks = list(persons_crypt.csv = task),
    mask_rows = mask
  )

  cryptRopen:::.finalize_job_side_effects(job)

  # Log file landed.
  logs <- list.files(job$output_path, pattern = "^log_crypt_r_.*\\.xlsx$")
  expect_length(logs, 1L)

  # TC re-injected.
  tcs <- cryptRopen::get_correspondence_tables()
  expect_true("tc_final" %in% names(tcs))

  cryptRopen:::.clear_correspondence_tables()
})

test_that(".finalize_job_side_effects(): idempotent — second call does not emit a second log", {
  skip_if_not_installed("writexl")
  r <- make_ok_result(tc_name = NA_character_, tc_df = NULL)
  task <- structure(list(data = r), class = "mirai")
  mask <- data.frame(
    folder_path = "/in", file = "x.csv",
    encrypted_file = "x_crypt.csv", to_encrypt = "X",
    stringsAsFactors = FALSE
  )
  job <- make_job_stub(
    tasks = list(x_crypt.csv = task),
    mask_rows = mask
  )

  cryptRopen:::.finalize_job_side_effects(job)
  cryptRopen:::.finalize_job_side_effects(job)

  logs <- list.files(job$output_path, pattern = "^log_crypt_r_.*\\.xlsx$")
  expect_length(logs, 1L)
})
