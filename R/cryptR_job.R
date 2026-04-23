# Asynchronous job scaffolding for crypt_r().
#
# Phase 1.D.6.a goal: introduce the `cryptR_job` S3 class and the four
# companion functions (`cryptR_status()`, `cryptR_wait()`, `cryptR_collect()`,
# `print.cryptR_job()`) *in isolation* — without touching `crypt_r()` yet.
# The orchestrator itself is rewritten to return a `cryptR_job` in 1.D.6.b;
# the récapitulatif log (`log_crypt_r_<timestamp>.xlsx`) is wired in 1.D.6.c.
#
# A `cryptR_job` is a thin S3 list carrying:
#   tasks               : named list of `mirai` handles (one per filtered mask row;
#                         names are the dedup'd `encrypted_file` stems).
#   mask_rows           : data.frame of filtered mask rows, in 1:1 correspondence
#                         with `tasks` (used by the 1.D.6.c log writer).
#   output_path         : absolute path where outputs + the final log land.
#   intermediate_path   : absolute path where TCs (parquet) land.
#   started_at          : Sys.time() at dispatch.
#   log_written         : logical flag, toggled by cryptR_collect().
#   watcher             : reserved slot for the mirai watcher that will write
#                         the log automatically in 1.D.6.c; NULL in 1.D.6.a.


#' @noRd
.new_cryptR_job <- function(tasks,
                            mask_rows,
                            output_path,
                            intermediate_path,
                            started_at = Sys.time(),
                            watcher = NULL) {
  stopifnot(
    is.list(tasks),
    is.data.frame(mask_rows),
    length(tasks) == nrow(mask_rows),
    is.character(output_path), length(output_path) == 1L,
    is.character(intermediate_path), length(intermediate_path) == 1L,
    inherits(started_at, "POSIXct")
  )
  # Task names: prefer existing names(tasks); fall back to mask_rows$encrypted_file
  # (already dedup'd by crypt_r's mask-processing code in 1.D.6.b).
  if (is.null(names(tasks)) && "encrypted_file" %in% names(mask_rows)) {
    names(tasks) <- as.character(mask_rows$encrypted_file)
  }

  structure(
    list(
      tasks             = tasks,
      mask_rows         = mask_rows,
      output_path       = output_path,
      intermediate_path = intermediate_path,
      started_at        = started_at,
      log_written       = FALSE,
      watcher           = watcher
    ),
    class = "cryptR_job"
  )
}


#' Inspect the state of an asynchronous `crypt_r()` job
#'
#' Returns a one-row-per-task snapshot of the job's progress. The result is
#' taken *at the time of the call*: a task resolved between two successive
#' calls will flip from `running` to `done`/`failed`, but the function itself
#' has no side effects (no log writing, no waiting).
#'
#' @param job A `cryptR_job` object, as returned by [crypt_r()].
#' @return A data.frame with columns `encrypted_file` (character),
#'   `state` (factor with levels `running` / `done` / `failed`) and
#'   `error_message` (character, `NA` unless `state == "failed"`).
#' @seealso [cryptR_wait()], [cryptR_collect()].
#' @export
#' @examples
#' \dontrun{
#' job <- crypt_r(...)
#' cryptR_status(job)
#' }
cryptR_status <- function(job) {
  if (!inherits(job, "cryptR_job")) {
    stop("cryptR_status() expects a 'cryptR_job' object.")
  }
  n <- length(job$tasks)
  if (n == 0L) {
    return(data.frame(
      encrypted_file = character(0),
      state          = factor(character(0),
                              levels = c("running", "done", "failed")),
      error_message  = character(0),
      stringsAsFactors = FALSE
    ))
  }

  states  <- vapply(job$tasks, .mirai_task_state,  character(1))
  errors  <- vapply(job$tasks, .mirai_task_error,  character(1))
  names_t <- names(job$tasks)
  if (is.null(names_t)) names_t <- rep(NA_character_, n)

  data.frame(
    encrypted_file = names_t,
    state          = factor(states, levels = c("running", "done", "failed")),
    error_message  = errors,
    stringsAsFactors = FALSE
  )
}


#' Block until an asynchronous `crypt_r()` job finishes
#'
#' Polls every task in `job` and returns once they are all resolved
#' (either `done` or `failed`). A failed task does *not* raise an
#' exception here — its state is simply visible via [cryptR_status()].
#'
#' @param job A `cryptR_job` object.
#' @param timeout Numeric. Maximum wait in seconds. Defaults to `Inf`.
#'   On expiration, an error of class `cryptR_timeout` is raised.
#' @param poll_interval Numeric. Polling period in seconds. Defaults to 0.1.
#' @return `invisible(job)`.
#' @seealso [cryptR_status()], [cryptR_collect()].
#' @export
#' @examples
#' \dontrun{
#' job <- crypt_r(...)
#' cryptR_wait(job, timeout = 60)
#' }
cryptR_wait <- function(job, timeout = Inf, poll_interval = 0.1) {
  if (!inherits(job, "cryptR_job")) {
    stop("cryptR_wait() expects a 'cryptR_job' object.")
  }
  stopifnot(
    is.numeric(timeout), length(timeout) == 1L, !is.na(timeout), timeout > 0,
    is.numeric(poll_interval), length(poll_interval) == 1L,
    !is.na(poll_interval), poll_interval > 0
  )

  if (length(job$tasks) == 0L) {
    return(invisible(job))
  }

  deadline <- if (is.finite(timeout)) Sys.time() + timeout else NA
  repeat {
    still_running <- vapply(job$tasks, mirai::unresolved, logical(1))
    if (!any(still_running)) break

    if (!is.na(deadline) && Sys.time() >= deadline) {
      stop(structure(
        class = c("cryptR_timeout", "error", "condition"),
        list(
          message = sprintf(
            "cryptR_wait() timed out after %g seconds (%d task(s) still running).",
            timeout, sum(still_running)),
          call = sys.call(-1L)
        )
      ))
    }
    Sys.sleep(poll_interval)
  }
  invisible(job)
}


#' Finalize an asynchronous `crypt_r()` job
#'
#' Waits for all tasks to resolve (see [cryptR_wait()]) and triggers the
#' recap log writer. In Phase 1.D.6.a the log writer is a stub that only
#' flips `job$log_written`; Phase 1.D.6.c implements the full
#' `log_crypt_r_<timestamp>.xlsx` output.
#'
#' @inheritParams cryptR_wait
#' @return `invisible(job)` — with `log_written = TRUE`.
#' @seealso [cryptR_status()], [cryptR_wait()].
#' @export
#' @examples
#' \dontrun{
#' job <- crypt_r(...)
#' cryptR_collect(job)
#' }
cryptR_collect <- function(job, timeout = Inf, poll_interval = 0.1) {
  if (!inherits(job, "cryptR_job")) {
    stop("cryptR_collect() expects a 'cryptR_job' object.")
  }
  cryptR_wait(job, timeout = timeout, poll_interval = poll_interval)
  # Phase 1.D.6.a: stub log writer. The real implementation (1.D.6.c) will:
  #   1. Call cryptR_status(job) for final states.
  #   2. Join on job$mask_rows.
  #   3. Add start_time/end_time/duration_sec/n_rows_processed/file stats.
  #   4. writexl::write_xlsx(..., file.path(job$output_path,
  #        paste0("log_crypt_r_", format(Sys.time(), ...), ".xlsx")))
  job$log_written <- TRUE
  invisible(job)
}


#' @export
print.cryptR_job <- function(x, ...) {
  status <- cryptR_status(x)
  counts <- table(factor(status$state,
                         levels = c("running", "done", "failed")))

  cat("<cryptR_job>\n")
  cat("  tasks       : ", length(x$tasks), "\n", sep = "")
  cat("  running     : ", counts[["running"]], "\n", sep = "")
  cat("  done        : ", counts[["done"]],    "\n", sep = "")
  cat("  failed      : ", counts[["failed"]],  "\n", sep = "")

  elapsed <- difftime(Sys.time(), x$started_at, units = "secs")
  cat("  elapsed     : ",
      sprintf("%.2f s", as.numeric(elapsed)), "\n", sep = "")
  cat("  output_path : ", x$output_path, "\n", sep = "")
  cat("  log_written : ", isTRUE(x$log_written), "\n", sep = "")
  invisible(x)
}
