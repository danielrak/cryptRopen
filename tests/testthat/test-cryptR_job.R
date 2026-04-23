# tests/testthat/test-cryptR_job.R
#
# Phase 1.D.6.a unit tests — exercise the `cryptR_job` S3 class and companions
# in isolation, with ad-hoc `mirai::mirai()` tasks. crypt_r() is NOT touched
# in 1.D.6.a, so these tests don't go through the orchestrator.
#
# Daemon strategy: 2 daemons for the whole file, set once up front and torn
# down by testthat's file-level deferral. Using daemons (rather than the
# auto-dispatcher) gives predictable low-latency task dispatch.

if (requireNamespace("mirai", quietly = TRUE)) {
  mirai::daemons(2)
  withr::defer(
    try(mirai::daemons(0), silent = TRUE),
    envir = testthat::teardown_env()
  )
}

# Small helper: dispatch a mirai that resolves quickly to a known value.
m_done <- function(value = 42L) {
  mirai::mirai(x, x = value)
}

# Dispatch a mirai that errors as soon as it runs.
m_fail <- function(msg = "boom") {
  mirai::mirai(stop(m), m = msg)
}

# Dispatch a mirai that stays running for `sec` seconds — used to observe
# the "running" state deterministically. Teardown (`daemons(0)`) cancels it.
m_running <- function(sec = 3) {
  mirai::mirai(Sys.sleep(s), s = sec)
}

# Build a cryptR_job wrapping a named task list + matching mask_rows stub.
make_job <- function(tasks) {
  nms <- names(tasks)
  if (is.null(nms)) nms <- paste0("file_", seq_along(tasks))
  names(tasks) <- nms
  mask_rows <- data.frame(
    encrypted_file = nms,
    stringsAsFactors = FALSE
  )
  cryptRopen:::.new_cryptR_job(
    tasks             = tasks,
    mask_rows         = mask_rows,
    output_path       = tempfile("out_"),
    intermediate_path = tempfile("int_")
  )
}


# ---------------------------------------------------------------------------

test_that(".new_cryptR_job() accepts an empty task list", {
  job <- cryptRopen:::.new_cryptR_job(
    tasks             = list(),
    mask_rows         = data.frame(encrypted_file = character(0),
                                   stringsAsFactors = FALSE),
    output_path       = "/tmp/out",
    intermediate_path = "/tmp/int"
  )
  expect_s3_class(job, "cryptR_job")
  expect_length(job$tasks, 0L)
  expect_false(job$log_written)
  expect_null(job$watcher)
  expect_s3_class(job$started_at, "POSIXct")

  status <- cryptR_status(job)
  expect_equal(nrow(status), 0L)
  expect_equal(levels(status$state), c("running", "done", "failed"))
})


test_that("cryptR_status() classifies running / done / failed", {
  skip_if_not_installed("mirai")

  t_done <- m_done(7L)
  t_fail <- m_fail("oops")
  # Wait for the short ones to resolve.
  mirai::call_mirai(t_done)
  mirai::call_mirai(t_fail)

  t_run <- m_running(3)

  job <- make_job(list(done_task = t_done,
                       fail_task = t_fail,
                       run_task  = t_run))

  st <- cryptR_status(job)
  expect_equal(as.character(st$state[st$encrypted_file == "done_task"]), "done")
  expect_equal(as.character(st$state[st$encrypted_file == "fail_task"]), "failed")
  expect_equal(as.character(st$state[st$encrypted_file == "run_task"]),  "running")

  expect_true(is.na(st$error_message[st$encrypted_file == "done_task"]))
  expect_true(is.na(st$error_message[st$encrypted_file == "run_task"]))
  expect_false(is.na(st$error_message[st$encrypted_file == "fail_task"]))
  # The message is captured verbatim by mirai.
  expect_match(st$error_message[st$encrypted_file == "fail_task"], "oops")
})


test_that("cryptR_status() is idempotent on resolved tasks", {
  skip_if_not_installed("mirai")

  t1 <- m_done(1L)
  t2 <- m_fail("nope")
  mirai::call_mirai(t1)
  mirai::call_mirai(t2)

  job <- make_job(list(a = t1, b = t2))
  s1 <- cryptR_status(job)
  s2 <- cryptR_status(job)
  expect_equal(s1, s2)
  # All resolved — no "running" left.
  expect_false(any(as.character(s1$state) == "running"))
})


test_that("cryptR_wait() blocks until all tasks resolve", {
  skip_if_not_installed("mirai")

  t1 <- mirai::mirai(Sys.sleep(s), s = 0.3)
  t2 <- mirai::mirai(Sys.sleep(s), s = 0.5)
  job <- make_job(list(one = t1, two = t2))

  t_before <- Sys.time()
  cryptR_wait(job)
  elapsed <- as.numeric(difftime(Sys.time(), t_before, units = "secs"))

  # Both tasks should be resolved.
  st <- cryptR_status(job)
  expect_true(all(as.character(st$state) %in% c("done", "failed")))
  # Elapsed ≥ max sleep (0.5 s), but allow generous scheduler slack.
  expect_gte(elapsed, 0.4)
})


test_that("cryptR_wait(timeout = ...) raises a cryptR_timeout", {
  skip_if_not_installed("mirai")

  t_long <- m_running(5)
  job <- make_job(list(slow = t_long))

  err <- expect_error(cryptR_wait(job, timeout = 0.2, poll_interval = 0.05))
  expect_s3_class(err, "cryptR_timeout")
  # Keep the test file fast — the task is cancelled at teardown via daemons(0).
})


test_that("cryptR_collect() waits then flips log_written", {
  skip_if_not_installed("mirai")

  t1 <- m_done(1L)
  mirai::call_mirai(t1)

  job <- make_job(list(one = t1))
  expect_false(job$log_written)

  out <- cryptR_collect(job)
  expect_s3_class(out, "cryptR_job")
  expect_true(out$log_written)
  # Input object is unchanged (returned a copy).
  expect_false(job$log_written)
})


test_that("print.cryptR_job() emits structural keywords", {
  skip_if_not_installed("mirai")

  t1 <- m_done(1L)
  t2 <- m_fail("x")
  mirai::call_mirai(t1)
  mirai::call_mirai(t2)

  job <- make_job(list(a = t1, b = t2))
  out <- capture.output(print(job))

  # Structural keywords — language-neutral, no regex on R base messages.
  expect_true(any(grepl("cryptR_job",  out)))
  expect_true(any(grepl("tasks",       out)))
  expect_true(any(grepl("done",        out)))
  expect_true(any(grepl("failed",      out)))
  expect_true(any(grepl("elapsed",     out)))
  expect_true(any(grepl("output_path", out)))
})


test_that("a failed task surfaces via status, never as an exception from wait", {
  skip_if_not_installed("mirai")

  t_ok   <- m_done(1L)
  t_bad  <- m_fail("explicit failure")
  job <- make_job(list(ok = t_ok, bad = t_bad))

  # Should NOT raise — wait only blocks on resolution, it does not rethrow.
  expect_no_error(cryptR_wait(job, timeout = 5))

  st <- cryptR_status(job)
  expect_equal(as.character(st$state[st$encrypted_file == "bad"]), "failed")
  expect_match(st$error_message[st$encrypted_file == "bad"], "explicit failure")
})
