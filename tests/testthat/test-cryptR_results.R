# tests/testthat/test-cryptR_results.R
#
# Phase 2.B — disk-oriented companion view.
#
# cryptR_results(job) returns one row per task with:
#   encrypted_file, output_file_path, exists, size_bytes, sha256,
#   success, error_message.
#
# Coverage:
#   - empty job: typed 0-row data.frame with the full 7-column schema;
#   - resolved "done" task with a .make_row_result()-shaped payload:
#     size/sha propagate, exists reflects live disk state;
#   - failed task (mirai-level error): success = FALSE, error_message set,
#     metrics NA;
#   - running task: success = FALSE, exists = FALSE, NA metrics;
#   - file deletion detection: exists flips from TRUE to FALSE when the
#     output file is removed between the run and the query.
#
# Daemon pool is shared with other *cryptR_job* unit tests: 2 daemons,
# file-level, torn down on teardown_env.

if (requireNamespace("mirai", quietly = TRUE)) {
  mirai::daemons(2)
  withr::defer(
    try(mirai::daemons(0), silent = TRUE),
    envir = testthat::teardown_env()
  )
}

# ---- Helpers (duplicated from test-cryptR_job.R to keep files independent)

m_done_results <- function(value = 42L) {
  mirai::mirai(x, x = value)
}

m_fail_results <- function(msg = "boom") {
  mirai::mirai(stop(m), m = msg)
}

m_running_results <- function(sec = 3) {
  mirai::mirai(Sys.sleep(s), s = sec)
}

make_job_results <- function(tasks, output_path = tempfile("out_")) {
  nms <- names(tasks)
  if (is.null(nms)) nms <- paste0("file_", seq_along(tasks))
  names(tasks) <- nms
  mask_rows <- data.frame(
    encrypted_file   = nms,
    stringsAsFactors = FALSE
  )
  cryptRopen:::.new_cryptR_job(
    tasks             = tasks,
    mask_rows         = mask_rows,
    output_path       = output_path,
    intermediate_path = tempfile("int_")
  )
}

# Shape the typed engine payload used in several blocks below.
fake_payload_results <- function(success = TRUE,
                                 size = 1234,
                                 sha  = "deadbeef",
                                 err  = NA_character_) {
  list(
    success        = success,
    error_message  = err,
    tc_name        = NA_character_,
    tc_df          = NULL,
    metrics = list(
      start_time             = as.POSIXct("2026-04-23 10:00:00", tz = "UTC"),
      end_time               = as.POSIXct("2026-04-23 10:00:05", tz = "UTC"),
      duration_sec           = 5,
      n_rows_processed       = 10L,
      output_file_size_bytes = size,
      output_file_sha256     = sha
    )
  )
}


# ---------------------------------------------------------------------------

test_that("cryptR_results() rejects non-cryptR_job input", {
  expect_error(cryptR_results(list()), "cryptR_job")
  expect_error(cryptR_results(NULL),   "cryptR_job")
})


test_that("cryptR_results() on an empty job returns a typed 0-row data.frame", {
  empty_job <- cryptRopen:::.new_cryptR_job(
    tasks             = list(),
    mask_rows         = data.frame(encrypted_file = character(0),
                                   stringsAsFactors = FALSE),
    output_path       = "/tmp/out",
    intermediate_path = "/tmp/int"
  )
  res <- cryptR_results(empty_job)
  expect_s3_class(res, "data.frame")
  expect_equal(nrow(res), 0L)
  expect_named(
    res,
    c("encrypted_file", "output_file_path", "exists",
      "size_bytes", "sha256", "success", "error_message")
  )
  expect_type(res$encrypted_file,   "character")
  expect_type(res$output_file_path, "character")
  expect_type(res$exists,           "logical")
  expect_type(res$size_bytes,       "double")
  expect_type(res$sha256,           "character")
  expect_type(res$success,          "logical")
  expect_type(res$error_message,    "character")
})


test_that("cryptR_results() propagates size/sha from the engine payload", {
  skip_if_not_installed("mirai")

  out_dir <- tempfile("results_ok_"); dir.create(out_dir)
  on.exit(unlink(out_dir, recursive = TRUE, force = TRUE), add = TRUE)

  # Actually create the file on disk so `exists` flips to TRUE.
  writeLines("fake content", file.path(out_dir, "persons_crypt.csv"))

  t_ok <- mirai::mirai(p, p = fake_payload_results(size = 4242,
                                                   sha  = "abc123"))
  mirai::call_mirai(t_ok)

  job <- make_job_results(list(persons_crypt.csv = t_ok),
                          output_path = out_dir)
  res <- cryptR_results(job)

  expect_equal(nrow(res), 1L)
  expect_equal(res$encrypted_file, "persons_crypt.csv")
  expect_equal(
    res$output_file_path,
    file.path(out_dir, "persons_crypt.csv")
  )
  expect_true(res$exists)
  expect_equal(res$size_bytes, 4242)
  expect_equal(res$sha256,     "abc123")
  expect_true(res$success)
  expect_true(is.na(res$error_message))
})


test_that("cryptR_results() flags a disappeared output via exists = FALSE", {
  skip_if_not_installed("mirai")

  out_dir <- tempfile("results_gone_"); dir.create(out_dir)
  on.exit(unlink(out_dir, recursive = TRUE, force = TRUE), add = TRUE)

  target <- file.path(out_dir, "persons_crypt.csv")
  writeLines("fake content", target)

  t_ok <- mirai::mirai(p, p = fake_payload_results(size = 100, sha = "x"))
  mirai::call_mirai(t_ok)
  job <- make_job_results(list(persons_crypt.csv = t_ok),
                          output_path = out_dir)

  expect_true(cryptR_results(job)$exists)

  # User rm'd the file after the run — size/sha keep the worker-side
  # values, but `exists` reflects the live disk state.
  file.remove(target)
  res2 <- cryptR_results(job)
  expect_false(res2$exists)
  expect_equal(res2$size_bytes, 100)
  expect_equal(res2$sha256,     "x")
  expect_true(res2$success)
})


test_that("cryptR_results() surfaces mirai-level failures as success = FALSE + message", {
  skip_if_not_installed("mirai")

  out_dir <- tempfile("results_fail_"); dir.create(out_dir)
  on.exit(unlink(out_dir, recursive = TRUE, force = TRUE), add = TRUE)

  t_bad <- m_fail_results("explicit failure")
  mirai::call_mirai(t_bad)

  job <- make_job_results(list(bad_crypt.csv = t_bad),
                          output_path = out_dir)
  res <- cryptR_results(job)

  expect_false(res$success)
  expect_match(res$error_message, "explicit failure")
  # No payload → size/sha NA.
  expect_true(is.na(res$size_bytes))
  expect_true(is.na(res$sha256))
  # File was never produced.
  expect_false(res$exists)
  expect_equal(
    res$output_file_path,
    file.path(out_dir, "bad_crypt.csv")
  )
})


test_that("cryptR_results() marks a still-running task as not-success, exists FALSE, NA metrics", {
  skip_if_not_installed("mirai")

  out_dir <- tempfile("results_run_"); dir.create(out_dir)
  on.exit(unlink(out_dir, recursive = TRUE, force = TRUE), add = TRUE)

  t_run <- m_running_results(3)
  job   <- make_job_results(list(slow_crypt.csv = t_run),
                            output_path = out_dir)
  res   <- cryptR_results(job)

  expect_false(res$success)
  expect_false(res$exists)
  expect_true(is.na(res$size_bytes))
  expect_true(is.na(res$sha256))
  # `.extract_row_result()` sets a descriptive error_message for
  # unresolved tasks — accept any non-NA text there.
  expect_false(is.na(res$error_message))
})


test_that("cryptR_results() is join-compatible with cryptR_status() on encrypted_file", {
  skip_if_not_installed("mirai")

  out_dir <- tempfile("results_join_"); dir.create(out_dir)
  on.exit(unlink(out_dir, recursive = TRUE, force = TRUE), add = TRUE)

  writeLines("x", file.path(out_dir, "a_crypt.csv"))

  t1 <- mirai::mirai(p, p = fake_payload_results(size = 10, sha = "h1"))
  t2 <- m_fail_results("nope")
  mirai::call_mirai(t1)
  mirai::call_mirai(t2)

  job <- make_job_results(list(a_crypt.csv = t1, b_crypt.csv = t2),
                          output_path = out_dir)

  st  <- cryptR_status(job)
  res <- cryptR_results(job)

  # Same row set, same identifier semantics.
  expect_setequal(st$encrypted_file, res$encrypted_file)
  expect_equal(nrow(st), nrow(res))

  # Orthogonality: status is process-oriented, results is disk-oriented.
  expect_false("output_file_path" %in% names(st))
  expect_false("size_bytes"       %in% names(st))
  expect_false("state"            %in% names(res))
})
