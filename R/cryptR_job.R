# Asynchronous job scaffolding for crypt_r().
#
# Phase 1.D.6.a introduced the `cryptR_job` S3 class and the four companion
# functions (`cryptR_status()`, `cryptR_wait()`, `cryptR_collect()`,
# `print.cryptR_job()`) *in isolation* — without touching `crypt_r()`. Phase
# 1.D.6.b wires the orchestrator: `crypt_r()` now dispatches per-row tasks
# via `mirai::mirai()` and returns a `cryptR_job`. Phase 1.D.6.c fleshes out
# `cryptR_collect()`: it now (a) extracts typed per-row results, (b) re-injects
# the correspondence tables into the parent's `.cryptRopen_env` so that
# `get_correspondence_tables()` works after an async run, and (c) writes
# `log_crypt_r_<timestamp>.xlsx` in `job$output_path`. The log-writing
# helpers live in `R/cryptR_log.R`.
#
# A `cryptR_job` is a thin S3 list carrying:
#   tasks                : named list of `mirai` handles (one per filtered
#                          mask row; names are the dedup'd `encrypted_file`
#                          stems).
#   mask_rows            : data.frame of filtered mask rows, in 1:1
#                          correspondence with `tasks` (used by the 1.D.6.c
#                          log writer).
#   output_path          : absolute path where outputs + the final log land.
#   intermediate_path    : absolute path where TCs (parquet) land.
#   started_at           : Sys.time() at dispatch.
#   log_written          : logical flag, toggled by cryptR_collect().
#   watcher              : reserved slot for the mirai watcher that will
#                          write the log automatically in 1.D.6.c; NULL
#                          in 1.D.6.a / 1.D.6.b.
#   daemons_owned_by_job : logical(1). TRUE when crypt_r() set up the mirai
#                          daemons itself (no pre-existing daemons detected)
#                          — cryptR_collect() then tears them down on exit.
#                          FALSE when the user is managing the daemons
#                          externally, in which case cryptR_collect() leaves
#                          them alone. Added in 1.D.6.b.
#   daemons_torn_down    : logical(1). Set to TRUE by cryptR_collect() after
#                          a successful `mirai::daemons(0)` teardown; keeps
#                          the teardown idempotent across repeated collect
#                          calls. Added in 1.D.6.b.


#' @noRd
.new_cryptR_job <- function(tasks,
                            mask_rows,
                            output_path,
                            intermediate_path,
                            started_at          = Sys.time(),
                            watcher             = NULL,
                            daemons_owned_by_job = FALSE) {
  stopifnot(
    is.list(tasks),
    is.data.frame(mask_rows),
    length(tasks) == nrow(mask_rows),
    is.character(output_path), length(output_path) == 1L,
    is.character(intermediate_path), length(intermediate_path) == 1L,
    inherits(started_at, "POSIXct"),
    is.logical(daemons_owned_by_job), length(daemons_owned_by_job) == 1L,
    !is.na(daemons_owned_by_job)
  )
  # Task names: prefer existing names(tasks); fall back to mask_rows$encrypted_file
  # (already dedup'd by crypt_r's mask-processing code in 1.D.6.b).
  if (is.null(names(tasks)) && "encrypted_file" %in% names(mask_rows)) {
    names(tasks) <- as.character(mask_rows$encrypted_file)
  }

  structure(
    list(
      tasks                = tasks,
      mask_rows            = mask_rows,
      output_path          = output_path,
      intermediate_path    = intermediate_path,
      started_at           = started_at,
      log_written          = FALSE,
      watcher              = watcher,
      daemons_owned_by_job = daemons_owned_by_job,
      daemons_torn_down    = FALSE
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
#' Since Phase 2.A the snapshot also carries per-row metrics
#' (`start_time`, `end_time`, `duration_sec`, `n_rows_processed`) for
#' resolved tasks — populated from the task payload shipped back by the
#' engines (`.make_row_result()`). Metrics are `NA` for tasks still
#' `running` or that errored before producing a payload; this lets you
#' monitor a run in-flight without reading the `log_crypt_r_*.xlsx`
#' file from disk.
#'
#' @param job A `cryptR_job` object, as returned by [crypt_r()].
#' @return A data.frame with columns
#'   `encrypted_file` (character),
#'   `state` (factor: `running` / `done` / `failed`),
#'   `error_message` (character, `NA` unless `state == "failed"`),
#'   `start_time` / `end_time` (POSIXct, `NA` until the task resolves),
#'   `duration_sec` (numeric seconds, `NA` until the task resolves),
#'   `n_rows_processed` (integer, `NA` until the task resolves and the
#'   engine reports a row count).
#' @seealso [cryptR_wait()], [cryptR_collect()], [summary.cryptR_job()].
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
      encrypted_file   = character(0),
      state            = factor(character(0),
                                levels = c("running", "done", "failed")),
      error_message    = character(0),
      start_time       = as.POSIXct(character(0)),
      end_time         = as.POSIXct(character(0)),
      duration_sec     = numeric(0),
      n_rows_processed = integer(0),
      stringsAsFactors = FALSE
    ))
  }

  states  <- vapply(job$tasks, .mirai_task_state,  character(1))
  errors  <- vapply(job$tasks, .mirai_task_error,  character(1))
  metrics <- lapply(job$tasks, .task_metrics)
  names_t <- names(job$tasks)
  if (is.null(names_t)) names_t <- rep(NA_character_, n)

  # c() on POSIXct preserves the class but harmonises time zones. Using
  # do.call rather than `unlist` because `unlist` drops the POSIXct class.
  start_time <- do.call(c, lapply(metrics, `[[`, "start_time"))
  end_time   <- do.call(c, lapply(metrics, `[[`, "end_time"))

  data.frame(
    encrypted_file   = names_t,
    state            = factor(states,
                              levels = c("running", "done", "failed")),
    error_message    = errors,
    start_time       = start_time,
    end_time         = end_time,
    duration_sec     = vapply(metrics, `[[`, numeric(1), "duration_sec"),
    n_rows_processed = vapply(metrics, `[[`, integer(1), "n_rows_processed"),
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
#' Waits for all tasks to resolve (see [cryptR_wait()]), extracts the
#' per-row results, re-publishes the correspondence tables into the
#' parent's `.cryptRopen_env` (so `get_correspondence_tables()` sees them
#' after an async run), writes the recap log
#' `log_crypt_r_<timestamp>.xlsx` under `job$output_path`, and — when
#' `crypt_r()` created the mirai daemons itself
#' (`daemons_owned_by_job = TRUE`) — tears them down.
#'
#' Idempotent on two independent axes:
#'   * `job$log_written` guards the log-writing + TC re-injection block,
#'     so a manual `cryptR_collect()` called *after* the auto watcher
#'     (1.D.6.c) has already run is a no-op — no duplicate xlsx, no TCs
#'     stored twice.
#'   * `job$daemons_torn_down` guards the `mirai::daemons(0)` call so
#'     a second collect does not attempt a double teardown.
#'
#' When daemons were set up externally before `crypt_r()` was called,
#' `daemons_owned_by_job` is `FALSE` and teardown is never attempted —
#' the user retains control of their own daemons.
#'
#' @inheritParams cryptR_wait
#' @return `invisible(job)` — with `log_written = TRUE` and, when
#'   applicable, `daemons_torn_down = TRUE`. The modifications are
#'   applied to the returned object only; S3 objects are not mutable
#'   in place in R, so callers who want the updated flags must capture
#'   the return value (`job <- cryptR_collect(job)`).
#' @seealso [cryptR_status()], [cryptR_wait()].
#' @export
#' @examples
#' \dontrun{
#' job <- crypt_r(...)
#' job <- cryptR_collect(job)
#' }
cryptR_collect <- function(job, timeout = Inf, poll_interval = 0.1) {
  if (!inherits(job, "cryptR_job")) {
    stop("cryptR_collect() expects a 'cryptR_job' object.")
  }
  cryptR_wait(job, timeout = timeout, poll_interval = poll_interval)

  # Idempotence guard: if this specific cryptR_job value has already
  # gone through collect, skip. The watcher may also have run — its
  # side-effects (log file, TCs) are checked/deduped *inside*
  # `.finalize_job_side_effects()` via `.log_already_written()`, so
  # repeating the call after a watcher-driven finalize is safe either
  # way. The local `job$log_written` flag mirrors the return contract
  # documented in Phase 1.D.6.a tests (input value stays unchanged;
  # the returned copy has the flag flipped).
  if (!isTRUE(job$log_written)) {
    .finalize_job_side_effects(job)
    job$log_written <- TRUE
  }

  # Phase 1.D.6.b: tear down the daemons we own. Silently tolerate any
  # failure (e.g. daemons already gone) — the flag below makes the
  # operation idempotent across repeated collect calls.
  if (isTRUE(job$daemons_owned_by_job) && !isTRUE(job$daemons_torn_down)) {
    try(mirai::daemons(0), silent = TRUE)
    job$daemons_torn_down <- TRUE
  }
  invisible(job)
}


#' @export
print.cryptR_job <- function(x, ...) {
  status <- cryptR_status(x)
  counts <- table(factor(status$state,
                         levels = c("running", "done", "failed")))
  n_workers <- .n_workers_active()

  cat("<cryptR_job>\n")
  cat("  tasks       : ", length(x$tasks), "\n", sep = "")
  cat("  running     : ", counts[["running"]], "\n", sep = "")
  cat("  done        : ", counts[["done"]],    "\n", sep = "")
  cat("  failed      : ", counts[["failed"]],  "\n", sep = "")
  cat("  workers     : ",
      if (is.na(n_workers)) "?" else n_workers, "\n", sep = "")

  elapsed <- difftime(Sys.time(), x$started_at, units = "secs")
  cat("  elapsed     : ",
      sprintf("%.2f s", as.numeric(elapsed)), "\n", sep = "")
  cat("  output_path : ", x$output_path, "\n", sep = "")
  cat("  log_written : ", isTRUE(x$log_written), "\n", sep = "")
  invisible(x)
}


#' Dashboard summary of an asynchronous `crypt_r()` job
#'
#' Companion to `print()`, introduced in Phase 2.A. Returns a structured
#' list (class `summary.cryptR_job`) carrying the aggregates a
#' monitoring script typically needs: total task count, per-state
#' counts, elapsed seconds, active workers, total rows processed (sum
#' over resolved tasks — excludes NA), output path, and the full
#' per-task status dataframe.
#'
#' The object has its own `print()` method that renders a compact
#' dashboard.
#'
#' @param object A `cryptR_job` object.
#' @param ... Ignored.
#' @return An object of class `summary.cryptR_job` — a named list
#'   with elements `n_tasks`, `counts`, `elapsed_sec`, `n_workers`,
#'   `total_rows`, `output_path`, `log_written`, `status` (the full
#'   [cryptR_status()] dataframe).
#' @seealso [cryptR_status()], [cryptR_collect()].
#' @export
#' @examples
#' \dontrun{
#' job <- crypt_r(...)
#' summary(job)
#' }
summary.cryptR_job <- function(object, ...) {
  status <- cryptR_status(object)
  states <- as.character(status$state)
  counts <- c(
    running = sum(states == "running"),
    done    = sum(states == "done"),
    failed  = sum(states == "failed")
  )
  elapsed <- as.numeric(
    difftime(Sys.time(), object$started_at, units = "secs"))

  total_rows <- if (any(!is.na(status$n_rows_processed)))
                  as.integer(sum(status$n_rows_processed, na.rm = TRUE))
                else NA_integer_

  structure(
    list(
      n_tasks     = length(object$tasks),
      counts      = counts,
      elapsed_sec = elapsed,
      n_workers   = .n_workers_active(),
      total_rows  = total_rows,
      output_path = object$output_path,
      log_written = isTRUE(object$log_written),
      status      = status
    ),
    class = "summary.cryptR_job"
  )
}


#' @export
print.summary.cryptR_job <- function(x, ...) {
  cat("<cryptR_job summary>\n")
  cat(sprintf("  tasks        : %d\n",   x$n_tasks))
  cat(sprintf("    running    : %d\n",   x$counts[["running"]]))
  cat(sprintf("    done       : %d\n",   x$counts[["done"]]))
  cat(sprintf("    failed     : %d\n",   x$counts[["failed"]]))
  cat(sprintf("  workers      : %s\n",
              if (is.na(x$n_workers)) "?" else as.character(x$n_workers)))
  cat(sprintf("  elapsed      : %.2f s\n", x$elapsed_sec))
  cat(sprintf("  rows total   : %s\n",
              if (is.na(x$total_rows)) "?" else
                formatC(x$total_rows, format = "d", big.mark = " ")))
  cat(sprintf("  output_path  : %s\n",   x$output_path))
  cat(sprintf("  log_written  : %s\n",   x$log_written))
  invisible(x)
}
