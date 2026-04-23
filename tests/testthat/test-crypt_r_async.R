# tests/testthat/test-crypt_r_async.R
#
# Phase 1.D.6.b — end-to-end tests of the non-blocking `crypt_r()` orchestrator.
#
# Scope:
#   - `crypt_r()` returns a `cryptR_job` immediately (non-blocking).
#   - After `cryptR_wait()` / `cryptR_collect()`, file outputs match the
#     synchronous semantics (encrypted dataset + TC parquet + inspect xlsx).
#   - Per-row fault tolerance: one failing row does not poison the others.
#   - Daemons ownership toggling: job-owned vs externally-owned.
#   - `cryptR_collect()` is idempotent on ownership teardown.
#
# Daemon strategy: no file-scoped daemons. Tests that want a pre-existing
# daemon pool set it up locally and tear it down on exit, so the
# ownership-detection path in `crypt_r()` is exercised both ways.
#
# Byte-level file content is already covered by test-baseline.R; this file
# focuses on orchestration semantics.


# ---- Preflight ---------------------------------------------------------
# Under `devtools::test()`, `cryptRopen` is loaded via `pkgload` — not
# installed into a library visible to the mirai daemons' fresh R
# processes. Dispatching `cryptRopen:::.process_mask_row` would then
# surface `there is no package called 'cryptRopen'`. Run the probe once
# at file scope and gate every test with `skip_if_async_unavailable()`.
.cryptR_async_preflight <- function() {
  if (!requireNamespace("mirai", quietly = TRUE)) return(FALSE)

  had_daemons <- tryCatch({
    st <- mirai::status()
    d  <- st$daemons
    !is.null(d) && ((is.matrix(d) && nrow(d) > 0L) || length(d) > 0L)
  }, error = function(e) FALSE)

  if (!had_daemons) {
    mirai::daemons(1)
    # Scoped teardown: whatever happens below, we restore the "no
    # daemons" state we started from.
    on.exit(try(mirai::daemons(0), silent = TRUE), add = TRUE)
  }

  tryCatch({
    t <- mirai::mirai(requireNamespace("cryptRopen", quietly = TRUE))
    mirai::call_mirai(t)
    isTRUE(t$data)
  }, error = function(e) FALSE)
}
.cryptR_async_ok <- tryCatch(.cryptR_async_preflight(),
                             error = function(e) FALSE)

skip_if_async_unavailable <- function() {
  if (!isTRUE(.cryptR_async_ok)) {
    testthat::skip(
      "cryptRopen is not resolvable inside mirai daemons (likely devtools::test())")
  }
}


# ---- Fixtures ----------------------------------------------------------

setup_dirs_async <- function() {
  root <- tempfile("crypt_r_async_")
  dir.create(root)
  inp <- file.path(root, "inputs");       dir.create(inp)
  out <- file.path(root, "output");       dir.create(out)
  int <- file.path(root, "intermediate"); dir.create(int)
  msk <- file.path(root, "mask");         dir.create(msk)
  list(root = root, inp = inp, out = out, int = int, msk = msk)
}

# Build a minimal mask row (columns consumed by crypt_r + the filter
# column `to_encrypt`).
mask_row_async <- function(folder_path, file, encrypted_file,
                           vars_to_encrypt, to_encrypt = "X",
                           vars_to_remove = NA) {
  data.frame(
    folder_path     = folder_path,
    file            = file,
    to_encrypt      = to_encrypt,
    encrypted_file  = encrypted_file,
    vars_to_encrypt = vars_to_encrypt,
    vars_to_remove  = vars_to_remove,
    stringsAsFactors = FALSE
  )
}

# Write a mask data.frame to an xlsx file inside dirs$msk and return the
# (folder, file) pair expected by crypt_r().
write_mask_xlsx <- function(dirs, mask_df, filename = "mask.xlsx") {
  path <- file.path(dirs$msk, filename)
  writexl::write_xlsx(mask_df, path)
  list(folder = dirs$msk, file = filename)
}

# Seed a simple CSV input with a handful of rows.
write_simple_csv <- function(path, n = 3L) {
  df <- data.frame(
    id  = paste0("u", seq_len(n)),
    val = seq_len(n),
    stringsAsFactors = FALSE
  )
  utils::write.csv(df, path, row.names = FALSE)
  df
}


# ---- Non-blocking return ----------------------------------------------

test_that("crypt_r() returns a cryptR_job immediately (non-blocking)", {
  skip_if_async_unavailable()
  skip_if_not_installed("readxl")

  dirs <- setup_dirs_async()
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  write_simple_csv(file.path(dirs$inp, "persons.csv"))
  mk <- write_mask_xlsx(dirs, mask_row_async(
    folder_path = dirs$inp, file = "persons.csv",
    encrypted_file = "persons_crypt.csv", vars_to_encrypt = "id"))

  t0  <- Sys.time()
  job <- crypt_r(
    mask_folder_path  = mk$folder, mask_file = mk$file,
    output_path       = dirs$out,  intermediate_path = dirs$int,
    encryption_key    = "testkey", algorithm = "md5",
    correspondence_table = TRUE, engine = "in_memory",
    n_workers = 1L)
  t1  <- Sys.time()
  on.exit(try(cryptR_collect(job, timeout = 30), silent = TRUE), add = TRUE)

  expect_s3_class(job, "cryptR_job")
  expect_length(job$tasks, 1L)
  expect_named(job$tasks, "persons_crypt.csv")
  # The call must return well before the task has had time to finish its
  # rio::import + hashing pipeline on a real dataset. 1.5 s is a very
  # generous ceiling on any CI-class machine; it fails loudly only if
  # crypt_r() is accidentally blocking.
  expect_lt(as.numeric(difftime(t1, t0, units = "secs")), 1.5)
})


test_that("crypt_r() on an empty mask returns a trivial cryptR_job without spawning daemons", {
  skip_if_async_unavailable()
  skip_if_not_installed("readxl")

  dirs <- setup_dirs_async()
  on.exit(unlink(dirs$root, recursive = TRUE, force = TRUE), add = TRUE)

  # One skipped row → post-filter, mask is empty.
  empty_mask <- mask_row_async(
    folder_path = dirs$inp, file = "n.csv",
    encrypted_file = "n_crypt.csv", vars_to_encrypt = "id",
    to_encrypt = "N")
  mk <- write_mask_xlsx(dirs, empty_mask)

  job <- crypt_r(
    mask_folder_path  = mk$folder, mask_file = mk$file,
    output_path       = dirs$out,  intermediate_path = dirs$int,
    encryption_key    = "k", algorithm = "md5",
    correspondence_table = TRUE)

  expect_s3_class(job, "cryptR_job")
  expect_length(job$tasks, 0L)
  expect_false(job$daemons_owned_by_job)
  # Collecting a trivial job just flips log_written and returns.
  out <- cryptR_collect(job)
  expect_true(out$log_written)
})


# ---- Wait + outputs ----------------------------------------------------

test_that("after cryptR_wait(), file outputs are produced and match in_memory semantics", {
  skip_if_async_unavailable()
  skip_if_not_installed("readxl")

  dirs <- setup_dirs_async()
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  src <- write_simple_csv(file.path(dirs$inp, "persons.csv"), n = 4L)
  mk  <- write_mask_xlsx(dirs, mask_row_async(
    folder_path = dirs$inp, file = "persons.csv",
    encrypted_file = "persons_crypt.csv", vars_to_encrypt = "id"))

  job <- crypt_r(
    mask_folder_path  = mk$folder, mask_file = mk$file,
    output_path       = dirs$out,  intermediate_path = dirs$int,
    encryption_key    = "k", algorithm = "md5",
    correspondence_table = TRUE, engine = "in_memory",
    n_workers = 1L)
  on.exit(try(cryptR_collect(job, timeout = 30), silent = TRUE), add = TRUE)

  cryptR_wait(job, timeout = 30)
  st <- cryptR_status(job)
  expect_equal(as.character(st$state), "done")

  # Output files landed.
  expect_true(file.exists(file.path(dirs$out, "persons_crypt.csv")))
  expect_true(file.exists(file.path(dirs$int, "tc_persons_crypt.parquet")))
  expect_true(file.exists(
    file.path(dirs$out, "inspect_persons_crypt.csv.xlsx")))

  # Encrypted output has the expected schema.
  out_df <- utils::read.csv(file.path(dirs$out, "persons_crypt.csv"),
                            stringsAsFactors = FALSE)
  expect_true("id_crypt" %in% names(out_df))
  expect_false("id"      %in% names(out_df))
  expect_equal(nrow(out_df), nrow(src))
})


# ---- Fault tolerance ---------------------------------------------------

test_that("one failing row does not poison the others", {
  skip_if_async_unavailable()
  skip_if_not_installed("readxl")

  dirs <- setup_dirs_async()
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  write_simple_csv(file.path(dirs$inp, "ok.csv"))
  # Second row references a non-existent file — rio::import will error
  # inside the task, which must be captured by the in_memory engine's
  # try() and not propagate beyond the task.
  mask_df <- rbind(
    mask_row_async(folder_path = dirs$inp, file = "ok.csv",
                   encrypted_file = "ok_crypt.csv",
                   vars_to_encrypt = "id"),
    mask_row_async(folder_path = dirs$inp, file = "ghost.csv",
                   encrypted_file = "ghost_crypt.csv",
                   vars_to_encrypt = "id")
  )
  mk <- write_mask_xlsx(dirs, mask_df)

  job <- crypt_r(
    mask_folder_path  = mk$folder, mask_file = mk$file,
    output_path       = dirs$out,  intermediate_path = dirs$int,
    encryption_key    = "k", algorithm = "md5",
    correspondence_table = TRUE, engine = "in_memory",
    n_workers = 2L)
  on.exit(try(cryptR_collect(job, timeout = 30), silent = TRUE), add = TRUE)

  # cryptR_wait() never throws on task failure — it just waits for resolution.
  expect_no_error(cryptR_wait(job, timeout = 30))

  # The per-row engine catches its own errors with try(), so both tasks
  # resolve as "done" from mirai's perspective. The meaningful assertion
  # is that the successful row produced its output.
  expect_true(file.exists(file.path(dirs$out, "ok_crypt.csv")))
  expect_false(file.exists(file.path(dirs$out, "ghost_crypt.csv")))
})


# ---- Daemons ownership -------------------------------------------------

test_that("when no daemons are running, crypt_r() claims ownership and cryptR_collect() tears them down", {
  skip_if_async_unavailable()
  skip_if_not_installed("readxl")

  # Start from a clean slate: if something left daemons running, we
  # don't own them, but we also don't want to interfere.
  already_running <- tryCatch({
    st <- mirai::status(); d <- st$daemons
    !is.null(d) && ((is.matrix(d) && nrow(d) > 0L) || length(d) > 0L)
  }, error = function(e) FALSE)
  skip_if(already_running,
          "pre-existing daemons — the 'job-owned' path cannot be tested cleanly")

  dirs <- setup_dirs_async()
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    cryptRopen:::.clear_correspondence_tables()
    try(mirai::daemons(0), silent = TRUE)  # belt-and-braces
  }, add = TRUE)

  write_simple_csv(file.path(dirs$inp, "ok.csv"))
  mk <- write_mask_xlsx(dirs, mask_row_async(
    folder_path = dirs$inp, file = "ok.csv",
    encrypted_file = "ok_crypt.csv", vars_to_encrypt = "id"))

  job <- crypt_r(
    mask_folder_path  = mk$folder, mask_file = mk$file,
    output_path       = dirs$out,  intermediate_path = dirs$int,
    encryption_key    = "k", algorithm = "md5",
    correspondence_table = TRUE, engine = "in_memory",
    n_workers = 2L)

  expect_true(job$daemons_owned_by_job)
  expect_false(job$daemons_torn_down)

  out <- cryptR_collect(job, timeout = 30)
  expect_true(out$log_written)
  expect_true(out$daemons_torn_down)

  # Idempotent: a second collect does not error or re-teardown.
  out2 <- cryptR_collect(out, timeout = 5)
  expect_true(out2$daemons_torn_down)
})


test_that("when daemons are already running, crypt_r() reuses them and does NOT tear them down", {
  skip_if_async_unavailable()
  skip_if_not_installed("readxl")

  mirai::daemons(2)
  on.exit(try(mirai::daemons(0), silent = TRUE), add = TRUE)

  dirs <- setup_dirs_async()
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  write_simple_csv(file.path(dirs$inp, "ok.csv"))
  mk <- write_mask_xlsx(dirs, mask_row_async(
    folder_path = dirs$inp, file = "ok.csv",
    encrypted_file = "ok_crypt.csv", vars_to_encrypt = "id"))

  job <- crypt_r(
    mask_folder_path  = mk$folder, mask_file = mk$file,
    output_path       = dirs$out,  intermediate_path = dirs$int,
    encryption_key    = "k", algorithm = "md5",
    correspondence_table = TRUE, engine = "in_memory")

  expect_false(job$daemons_owned_by_job)

  out <- cryptR_collect(job, timeout = 30)
  expect_true(out$log_written)
  # User-owned daemons: collect must NOT have torn them down.
  expect_false(out$daemons_torn_down)

  # Daemons are still reachable — status() should not error.
  expect_no_error(mirai::status())
})


# ---- Correspondence tables via get_correspondence_tables() -------------

test_that("after collect, correspondence tables are available via get_correspondence_tables()", {
  skip_if_async_unavailable()
  skip_if_not_installed("readxl")

  dirs <- setup_dirs_async()
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  cryptRopen:::.clear_correspondence_tables()

  write_simple_csv(file.path(dirs$inp, "persons.csv"), n = 5L)
  mk <- write_mask_xlsx(dirs, mask_row_async(
    folder_path = dirs$inp, file = "persons.csv",
    encrypted_file = "persons_crypt.csv", vars_to_encrypt = "id"))

  job <- crypt_r(
    mask_folder_path  = mk$folder, mask_file = mk$file,
    output_path       = dirs$out,  intermediate_path = dirs$int,
    encryption_key    = "k", algorithm = "md5",
    correspondence_table = TRUE, engine = "in_memory",
    n_workers = 1L)
  out <- cryptR_collect(job, timeout = 30)

  # Phase 1.D.6.c: the worker now returns `tc_df` inside its typed
  # result (`.make_row_result()`), and `cryptR_collect()` re-injects it
  # into the parent's `.cryptRopen_env` via
  # `.reinject_correspondence_tables()`. So `get_correspondence_tables()`
  # in the parent should see the TC even though the daemon populated a
  # different env.
  tcs <- cryptRopen::get_correspondence_tables()
  expect_true("tc_persons_crypt" %in% names(tcs))
  expect_true(all(c("id", "id_crypt") %in% names(tcs$tc_persons_crypt)))

  # The disk parquet remains authoritative and must match.
  tc_path <- file.path(dirs$int, "tc_persons_crypt.parquet")
  expect_true(file.exists(tc_path))
  tc_disk <- as.data.frame(arrow::read_parquet(tc_path))
  expect_setequal(tc_disk$id, tcs$tc_persons_crypt$id)
})


# ---- Recap log xlsx ----------------------------------------------------

test_that("cryptR_collect() writes a log_crypt_r_<ts>.xlsx under output_path", {
  skip_if_async_unavailable()
  skip_if_not_installed("readxl")

  dirs <- setup_dirs_async()
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  write_simple_csv(file.path(dirs$inp, "persons.csv"), n = 3L)
  mk <- write_mask_xlsx(dirs, mask_row_async(
    folder_path = dirs$inp, file = "persons.csv",
    encrypted_file = "persons_crypt.csv", vars_to_encrypt = "id"))

  job <- crypt_r(
    mask_folder_path  = mk$folder, mask_file = mk$file,
    output_path       = dirs$out,  intermediate_path = dirs$int,
    encryption_key    = "k", algorithm = "md5",
    correspondence_table = TRUE, engine = "in_memory",
    n_workers = 1L)
  out <- cryptR_collect(job, timeout = 30)

  logs <- list.files(dirs$out, pattern = "^log_crypt_r_.*\\.xlsx$")
  expect_length(logs, 1L)

  # The log has the mask row + per-row metrics columns.
  df <- readxl::read_xlsx(file.path(dirs$out, logs[[1]]))
  expect_true("encrypted_file" %in% names(df))
  expect_true(all(c("success", "error_message", "start_time",
                    "end_time", "duration_sec", "n_rows_processed",
                    "output_file_size_bytes", "output_file_sha256")
                  %in% names(df)))
  expect_equal(nrow(df), 1L)
  expect_true(df$success[1])
})


test_that("a second cryptR_collect() on the same job does NOT write a second log", {
  skip_if_async_unavailable()
  skip_if_not_installed("readxl")

  dirs <- setup_dirs_async()
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  write_simple_csv(file.path(dirs$inp, "persons.csv"), n = 2L)
  mk <- write_mask_xlsx(dirs, mask_row_async(
    folder_path = dirs$inp, file = "persons.csv",
    encrypted_file = "persons_crypt.csv", vars_to_encrypt = "id"))

  job <- crypt_r(
    mask_folder_path  = mk$folder, mask_file = mk$file,
    output_path       = dirs$out,  intermediate_path = dirs$int,
    encryption_key    = "k", algorithm = "md5",
    correspondence_table = TRUE, engine = "in_memory",
    n_workers = 1L)
  out1 <- cryptR_collect(job, timeout = 30)
  # A second collect call passes the already-updated job (log_written = TRUE)
  # AND even a third call on the original raw job would hit the
  # `.log_already_written()` guard. Cover both paths.
  out2 <- cryptR_collect(out1, timeout = 5)
  out3 <- cryptR_collect(job,  timeout = 5)

  logs <- list.files(dirs$out, pattern = "^log_crypt_r_.*\\.xlsx$")
  expect_length(logs, 1L)
})


# ---- Auto watcher ------------------------------------------------------

test_that("auto watcher writes the log without a manual cryptR_collect() after later::run_now()", {
  skip_if_async_unavailable()
  skip_if_not_installed("later")
  skip_if_not_installed("readxl")

  dirs <- setup_dirs_async()
  on.exit({
    unlink(dirs$root, recursive = TRUE, force = TRUE)
    cryptRopen:::.clear_correspondence_tables()
  }, add = TRUE)

  write_simple_csv(file.path(dirs$inp, "persons.csv"), n = 2L)
  mk <- write_mask_xlsx(dirs, mask_row_async(
    folder_path = dirs$inp, file = "persons.csv",
    encrypted_file = "persons_crypt.csv", vars_to_encrypt = "id"))

  job <- crypt_r(
    mask_folder_path  = mk$folder, mask_file = mk$file,
    output_path       = dirs$out,  intermediate_path = dirs$int,
    encryption_key    = "k", algorithm = "md5",
    correspondence_table = TRUE, engine = "in_memory",
    n_workers = 1L)

  # The watcher slot is set by .start_watcher() when later is available.
  expect_true(isTRUE(job$watcher))

  # Wait for tasks — watcher callback has not fired yet (no run_now).
  cryptR_wait(job, timeout = 30)
  # Manually pump the later queue until the watcher finalises the job
  # (the watcher reschedules itself on every tick where tasks are still
  # running; here they are all done, so the first tick should finalize).
  deadline <- Sys.time() + 10
  while (Sys.time() < deadline) {
    later::run_now()
    logs <- list.files(dirs$out, pattern = "^log_crypt_r_.*\\.xlsx$")
    if (length(logs) == 1L) break
    Sys.sleep(0.2)
  }

  logs <- list.files(dirs$out, pattern = "^log_crypt_r_.*\\.xlsx$")
  expect_length(logs, 1L)

  # A manual collect afterwards must NOT add a second log file.
  out <- cryptR_collect(job, timeout = 5)
  logs2 <- list.files(dirs$out, pattern = "^log_crypt_r_.*\\.xlsx$")
  expect_length(logs2, 1L)

  # Ensure no daemons still lingering (watcher tears them down if owned).
  if (isTRUE(out$daemons_owned_by_job)) {
    try(mirai::daemons(0), silent = TRUE)
  }
})
