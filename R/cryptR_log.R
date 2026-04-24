# Recap log + correspondence-table re-injection helpers.
#
# The log-writing machinery lives here so the watcher (R/cryptR_watcher.R)
# and the synchronous manual cryptR_collect() path can share the same code:
# both end up calling `.finalize_job_side_effects()` which drives result
# extraction, TC re-injection and log writing. The file-existence
# idempotence check (`.log_already_written()`) is what keeps watcher +
# manual collect from producing two xlsx logs for the same run.
#
# Data flow:
#   .process_mask_row_*() (in worker / daemon)
#     -> returns a list{success, error_message, tc_name, tc_df, metrics}
#        (see .make_row_result in crypt_r.R)
#     -> shipped back to the parent as `task$data`
#
#   cryptR_collect() (in parent)
#     -> for each task, .extract_row_result(task) homogenises `task$data`
#     -> .reinject_correspondence_tables(results) repopulates
#        .cryptRopen_env (the daemon's env is process-isolated; this
#        is how get_correspondence_tables() sees TCs after an async run)
#     -> .build_crypt_r_log_df(job, results) joins the filtered mask rows
#        with the per-row metrics
#     -> .write_crypt_r_log_file(job, results) serialises the data.frame
#        to log_crypt_r_<timestamp>.xlsx in job$output_path
#
# Idempotence: the writer picks a timestamp from Sys.time() each call,
# so two successive collects would produce two logs. The caller
# (cryptR_collect) guards against that via `job$log_written` so a
# manual collect after an auto watcher does NOT duplicate the file.


#' Pull a homogeneous per-row result out of a `mirai` task (or a
#' hand-built stand-in).
#'
#' `task$data` after a successful `.process_mask_row_*()` run is the
#' list shape returned by `.make_row_result()`. Three other shapes are
#' tolerated so the log can still be assembled in degraded scenarios:
#'
#'   - a mirai error value (`mirai::is_error_value() == TRUE`) — the
#'     daemon threw; we populate `success = FALSE` and the error text;
#'   - a still-unresolved mirai (caller forgot to `cryptR_wait()`) —
#'     we return a `success = FALSE`, `error_message = "unresolved"`
#'     sentinel rather than block;
#'   - a non-list resolved value (e.g. a bare integer from an ad-hoc
#'     task in the `test-cryptR_job.R` unit tests) — we treat it as
#'     a trivial "done" with metadata set to NA.
#'
#' @param task A `mirai` handle, resolved or not. Callers should
#'   `cryptR_wait()` first for best results.
#' @param encrypted_file Character(1). Row identifier, used only to
#'   make error messages traceable.
#' @return A list with exactly the same shape as
#'   `.make_row_result()`, suitable for downstream aggregation.
#' @noRd
.extract_row_result <- function(task, encrypted_file) {
  # Defensive default — populated further down.
  default <- list(
    success = FALSE,
    error_message = NA_character_,
    tc_name = NA_character_,
    tc_df = NULL,
    metrics = list(
      start_time             = as.POSIXct(NA),
      end_time               = as.POSIXct(NA),
      duration_sec           = NA_real_,
      n_rows_processed       = NA_integer_,
      output_file_size_bytes = NA_real_,
      output_file_sha256     = NA_character_
    )
  )

  if (!inherits(task, "mirai")) {
    # Hand-built stand-in — e.g. test-cryptR_job.R assembles a
    # cryptR_job around raw mirai objects. We don't know the payload
    # shape; report a minimal success so the log writer has something
    # to show.
    default$success <- TRUE
    default$error_message <- NA_character_
    return(default)
  }

  # Still running — don't block here (the caller is responsible for
  # cryptR_wait). Just surface the state.
  if (isTRUE(mirai::unresolved(task))) {
    default$error_message <- sprintf(
      "task for %s still unresolved at collect time", encrypted_file
    )
    return(default)
  }

  # Resolved — distinguish mirai-level error vs. user-returned value.
  if (isTRUE(mirai::is_error_value(task$data))) {
    msg <- tryCatch(as.character(task$data),
      error = function(e) "unknown mirai error"
    )
    if (length(msg) != 1L || is.na(msg) || !nzchar(msg)) {
      msg <- "unknown mirai error"
    }
    default$error_message <- msg
    return(default)
  }

  value <- task$data

  # Expected shape: a list produced by .make_row_result().
  if (is.list(value) &&
    all(c(
      "success", "error_message", "tc_name",
      "tc_df", "metrics"
    ) %in% names(value))) {
    return(value)
  }

  # Resolved but unexpected shape (e.g. an integer from an ad-hoc
  # mirai in tests). Treat as a bare success, NAs everywhere.
  default$success <- TRUE
  default
}


#' Re-publish the correspondence tables captured inside the workers.
#'
#' Workers run in mirai daemons which are separate processes. Their
#' `.cryptRopen_env` is not the parent's. `.process_mask_row_*()`
#' populates the daemon-local env AND ships `tc_df` back in the result;
#' this helper pushes the client-side copy into the parent's
#' `.cryptRopen_env` via `.store_correspondence_table()`, so that
#' `get_correspondence_tables()` — which reads from the parent's env
#' only — returns the same content the synchronous code path would
#' have left there.
#'
#' Rows without a TC (`correspondence_table = FALSE`, or rows that
#' failed before TC assembly) are skipped silently.
#'
#' @param results A list of per-row result lists, as produced by
#'   `.extract_row_result()`.
#' @return Invisible `NULL`.
#' @noRd
.reinject_correspondence_tables <- function(results) {
  for (r in results) {
    if (isTRUE(r$success) &&
      is.character(r$tc_name) && length(r$tc_name) == 1L &&
      !is.na(r$tc_name) && nzchar(r$tc_name) &&
      !is.null(r$tc_df)) {
      .store_correspondence_table(name = r$tc_name, df = r$tc_df)
    }
  }
  invisible(NULL)
}


#' Join the filtered mask rows with the per-row metrics to produce the
#' recap data.frame.
#'
#' Column order (left to right):
#'   1. every column of `job$mask_rows` in its original order
#'      (folder_path, file, encrypted_file, vars_to_encrypt,
#'      vars_to_remove, to_encrypt, row_number, dupl_encrypted_file,
#'      ndupl_encrypted_file, …);
#'   2. success (logical), error_message (character);
#'   3. start_time, end_time (POSIXct), duration_sec (numeric);
#'   4. n_rows_processed (integer), output_file_size_bytes (numeric),
#'      output_file_sha256 (character).
#'
#' Row order is the same as `job$tasks` / `job$mask_rows`.
#'
#' @param job A `cryptR_job` object.
#' @param results A list of per-row result lists (see
#'   `.extract_row_result()`).
#' @return A data.frame. Character NAs are preserved so `writexl`
#'   writes empty cells rather than the literal string "NA".
#' @noRd
.build_crypt_r_log_df <- function(job, results) {
  n <- nrow(job$mask_rows)
  if (n == 0L || length(results) == 0L) {
    # Empty-mask job: still produce a typed zero-row data.frame so the
    # xlsx writer emits a valid (empty) sheet. Column types match the
    # populated case to keep downstream readers happy.
    return(cbind(
      job$mask_rows,
      data.frame(
        success                = logical(0),
        error_message          = character(0),
        start_time             = as.POSIXct(character(0)),
        end_time               = as.POSIXct(character(0)),
        duration_sec           = numeric(0),
        n_rows_processed       = integer(0),
        output_file_size_bytes = numeric(0),
        output_file_sha256     = character(0),
        stringsAsFactors       = FALSE
      )
    ))
  }

  stopifnot(length(results) == n)

  extras <- data.frame(
    success = vapply(results, \(r) isTRUE(r$success), logical(1)),
    error_message = vapply(
      results,
      \(r) if (is.null(r$error_message)) {
        NA_character_
      } else {
        as.character(r$error_message)
      },
      character(1)
    ),
    start_time = do.call(c, lapply(
      results,
      \(r) if (is.null(r$metrics$start_time)) {
        as.POSIXct(NA)
      } else {
        r$metrics$start_time
      }
    )),
    end_time = do.call(c, lapply(
      results,
      \(r) if (is.null(r$metrics$end_time)) {
        as.POSIXct(NA)
      } else {
        r$metrics$end_time
      }
    )),
    duration_sec = vapply(
      results,
      \(r) if (is.null(r$metrics$duration_sec)) {
        NA_real_
      } else {
        as.numeric(r$metrics$duration_sec)
      },
      numeric(1)
    ),
    n_rows_processed = vapply(
      results,
      \(r) if (is.null(r$metrics$n_rows_processed)) {
        NA_integer_
      } else {
        as.integer(r$metrics$n_rows_processed)
      },
      integer(1)
    ),
    output_file_size_bytes = vapply(
      results,
      \(r) if (is.null(r$metrics$output_file_size_bytes)) {
        NA_real_
      } else {
        as.numeric(r$metrics$output_file_size_bytes)
      },
      numeric(1)
    ),
    output_file_sha256 = vapply(
      results,
      \(r) if (is.null(r$metrics$output_file_sha256)) {
        NA_character_
      } else {
        as.character(r$metrics$output_file_sha256)
      },
      character(1)
    ),
    stringsAsFactors = FALSE
  )

  cbind(job$mask_rows, extras)
}


#' Write the recap log for a job to `log_crypt_r_<timestamp>.xlsx`.
#'
#' Returns the full path of the file written, or `NA_character_` if
#' the write failed. Failures are never fatal — the job's outputs are
#' already on disk at this point, and the log is an observational
#' artifact, not part of the contract.
#'
#' @param job A `cryptR_job` object.
#' @param results A list of per-row result lists.
#' @param now A `POSIXct(1)` timestamp; exposed so tests can pin the
#'   filename. Defaults to `Sys.time()`.
#' @return Character(1): the path written, or `NA_character_` on
#'   failure.
#' @noRd
.write_crypt_r_log_file <- function(job, results, now = Sys.time()) {
  # Fast-fail on a missing output directory: writexl's C layer would
  # still return an error, but it also prints a "[ERROR] workbook_close()"
  # line to stderr that `tryCatch` can't silence. Detect the condition
  # early and bail cleanly.
  if (!is.character(job$output_path) ||
    length(job$output_path) != 1L ||
    is.na(job$output_path) ||
    !dir.exists(job$output_path)) {
    return(NA_character_)
  }

  df <- .build_crypt_r_log_df(job, results)

  ts <- format(now, "%Y%m%d_%H%M%S")
  path <- file.path(job$output_path, paste0("log_crypt_r_", ts, ".xlsx"))

  tryCatch(
    {
      writexl::write_xlsx(df, path)
      path
    },
    error = function(e) NA_character_
  )
}


#' Has a log file for this run already been written?
#'
#' The watcher (R/cryptR_watcher.R) and manual `cryptR_collect()` can
#' both fire on the same job. We do NOT use a shared in-memory flag
#' because S3 copy-on-modify semantics would keep the user's `job` out
#' of sync with the watcher's captured copy. Instead, we use the output
#' directory as the source of truth: any `log_crypt_r_*.xlsx` whose
#' mtime is at or after `job$started_at` is attributable to this run.
#'
#' The timestamp comparison prevents a log from a *previous* crypt_r()
#' run (stale file sitting in `output_path`) from being misread as
#' "already written for this run".
#'
#' @param job A `cryptR_job` object.
#' @return `TRUE` if a matching log file exists, `FALSE` otherwise.
#' @noRd
.log_already_written <- function(job) {
  if (!is.character(job$output_path) ||
    length(job$output_path) != 1L ||
    is.na(job$output_path) ||
    !dir.exists(job$output_path)) {
    return(FALSE)
  }
  started <- job$started_at
  if (!inherits(started, "POSIXct") || length(started) != 1L) {
    return(FALSE)
  }
  existing <- list.files(job$output_path,
    pattern = "^log_crypt_r_.*\\.xlsx$",
    full.names = TRUE
  )
  if (length(existing) == 0L) {
    return(FALSE)
  }
  info <- file.info(existing)
  mtimes <- info$mtime
  # 1-second slack absorbs filesystem mtime quantisation (Windows NTFS
  # mtime can be truncated to integer seconds, while `started_at` has
  # sub-second precision). A stale log <1 s older than `started_at`
  # would still be misread as "this run"'s, but in practice jobs take
  # much longer than 1 s so the window is empty.
  any(!is.na(mtimes) & mtimes >= (started - 1))
}


#' Finalize the post-run side effects of a job.
#'
#' Shared entry point for both the manual `cryptR_collect()` path and
#' the auto watcher (R/cryptR_watcher.R). Runs three steps in order:
#'
#'   1. Extract a per-row typed result for every `job$tasks` entry via
#'      `.extract_row_result()` (tolerant of unresolved / mirai-error /
#'      unexpected payload shapes).
#'   2. Re-publish any returned correspondence tables into the parent
#'      process `.cryptRopen_env` — this is what makes
#'      `get_correspondence_tables()` see TCs produced by daemon-local
#'      workers. Always safe to repeat (same name/df → stable).
#'   3. Write the xlsx recap log — **guarded by `.log_already_written()`**
#'      so that if the watcher has already produced the log, a later
#'      manual collect does NOT emit a second file with a different
#'      timestamp. If the log write fails, we do not raise — see
#'      `.write_crypt_r_log_file()` for the rationale.
#'
#' @param job A `cryptR_job` object.
#' @return Invisible `NULL`.
#' @noRd
.finalize_job_side_effects <- function(job) {
  encrypted_files <- names(job$tasks)
  if (is.null(encrypted_files)) {
    encrypted_files <- rep(NA_character_, length(job$tasks))
  }
  results <- mapply(
    .extract_row_result,
    task           = job$tasks,
    encrypted_file = encrypted_files,
    SIMPLIFY       = FALSE,
    USE.NAMES      = FALSE
  )
  .reinject_correspondence_tables(results)
  if (!.log_already_written(job)) {
    .write_crypt_r_log_file(job, results)
  }
  invisible(NULL)
}
