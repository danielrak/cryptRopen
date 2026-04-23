# Auto-watcher for cryptR_job: writes the recap log + re-injects TCs as
# soon as the last mirai task resolves, without the user having to call
# cryptR_collect() manually.
#
# Design notes (Phase 1.D.6.c)
# ----------------------------
# mirai (as of 2.x) does not ship a native on-completion callback. The
# official integration path is via `as.promise.mirai()`, which itself
# relies on the `later` event loop being pumped — either implicitly in
# long-lived frameworks like Shiny/Plumber, or explicitly via
# `later::run_now()` in batch R sessions.
#
# Rather than pulling in `promises` (which would be a second dep and
# still requires `later` to pump), we use `later::later()` directly:
#   * A `tick()` closure polls the job's tasks via `mirai::unresolved()`.
#   * If any task is still running, `tick()` reschedules itself with a
#     `poll_interval` delay.
#   * Once everything has resolved, `tick()` runs the same finalize
#     pipeline `cryptR_collect()` uses, then tears down daemons the
#     job owns.
#
# Why `later::later()` fires at all in a batch session: `later` ticks
# whenever R returns to the REPL idle state (hook C), AND whenever
# `later::run_now()` is called explicitly. In an interactive RStudio
# session the REPL idle is enough; tests must call `later::run_now()`
# to pump the queue deterministically.
#
# Idempotence wrt manual collect:
#   * Watcher writes the log only if `.log_already_written(job)` is
#     FALSE → TRUE by the fs marker after the first finalize.
#   * Watcher calls `mirai::daemons(0)` via `try()` → a duplicate
#     teardown triggered by a subsequent manual cryptR_collect() is
#     silently tolerated.
#   * TC re-injection is itself idempotent (same name/df → same entry
#     in `.cryptRopen_env`).
#
# Graceful fallback: if `later` is unavailable for any reason (e.g. not
# yet installed in the user's library), `.start_watcher()` returns the
# job unchanged and the user is expected to call `cryptR_collect()`
# manually. The package still works; only the auto-log feature is off.


#' Schedule a non-blocking watcher that finalises a `cryptR_job` when
#' its last task resolves.
#'
#' Called by `crypt_r()` right after `.new_cryptR_job()` so that the
#' recap log is written automatically without the user having to call
#' `cryptR_collect()`.
#'
#' The watcher captures the job by value; mirai task handles inside
#' that copy are reference-like, so `mirai::unresolved()` sees the live
#' state even after the caller's `job` has drifted.
#'
#' @param job A `cryptR_job` object.
#' @param poll_interval Numeric. Polling period in seconds.
#'   Defaults to 0.5.
#' @return `job`, with the `watcher` slot set to `TRUE` when a watcher
#'   was actually scheduled. The caller should capture this return
#'   value.
#' @noRd
.start_watcher <- function(job, poll_interval = 0.5) {
  stopifnot(inherits(job, "cryptR_job"))
  stopifnot(is.numeric(poll_interval), length(poll_interval) == 1L,
            !is.na(poll_interval), poll_interval > 0)

  # `later` is declared in DESCRIPTION Imports but we still guard
  # against a missing install (e.g. stripped environments) so the
  # package remains usable — just without the auto-log feature.
  if (!requireNamespace("later", quietly = TRUE)) {
    return(job)
  }

  # Empty-mask jobs: there is nothing to poll. Finalize right now so
  # the user gets an empty but valid log.
  if (length(job$tasks) == 0L) {
    tryCatch(.finalize_job_side_effects(job), error = function(e) NULL)
    if (isTRUE(job$daemons_owned_by_job)) {
      try(mirai::daemons(0), silent = TRUE)
    }
    job$watcher <- TRUE
    return(job)
  }

  # Capture the job value. The `$tasks` list inside it holds the same
  # mirai handles the caller has, so polling resolution here is
  # equivalent to polling from the caller's side.
  captured <- job

  tick <- function() {
    # Defensive: a failure inside `mirai::unresolved()` (e.g. daemons
    # forcibly torn down mid-run) should not crash the watcher. Treat
    # those as "resolved" so we proceed to the finalize path.
    still_running <- tryCatch(
      vapply(captured$tasks, mirai::unresolved, logical(1)),
      error = function(e) rep(FALSE, length(captured$tasks))
    )
    if (any(still_running)) {
      later::later(tick, delay = poll_interval)
      return(invisible(NULL))
    }

    # All tasks resolved — run the same finalize pipeline as manual
    # cryptR_collect(). Every step is internally idempotent (see
    # `.finalize_job_side_effects()` and `.log_already_written()`),
    # so a subsequent manual collect will not double-write the log.
    tryCatch(.finalize_job_side_effects(captured),
             error = function(e) NULL)

    # Teardown daemons we own, best-effort. A manual collect after this
    # will re-attempt the teardown via `mirai::daemons(0)` wrapped in
    # `try()` — harmless.
    if (isTRUE(captured$daemons_owned_by_job)) {
      try(mirai::daemons(0), silent = TRUE)
    }
    invisible(NULL)
  }

  later::later(tick, delay = poll_interval)

  # Flag on the returned job so `print.cryptR_job()` / tests can see
  # that a watcher was registered. The user cannot cancel the watcher
  # from this flag (the closure is held by the `later` queue), but
  # that is fine: the watcher is a pure observer — it only writes the
  # log if it is not already there and tears down daemons the job
  # declared ownership over.
  job$watcher <- TRUE
  job
}
