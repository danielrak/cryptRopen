# Internal helpers isolating mirai version-dependent details.
#
# The rest of the cryptR_job code talks to mirai *only* through these helpers
# so that (a) unit tests can fake mirai objects if needed, and (b) any API
# drift in the mirai package is contained in one place.
#
# Contract (stable, relied on by R/cryptR_job.R):
#   .mirai_task_state(task) -> character(1), one of
#     "running" | "done" | "failed"
#   .mirai_task_error(task) -> character(1) or NA_character_
#
# Rationale for the 3-state model (not 4): pending vs running requires visibility
# into the dispatcher queue (number of daemons vs number of tasks). That is an
# orchestration-level concern that doesn't belong on an individual `mirai`
# handle. Phase 1.D.6.b may surface "pending" later via the job-level orchestrator;
# until then, the state vocabulary is kept small and unambiguous.


#' @noRd
.mirai_task_state <- function(task) {
  if (!inherits(task, "mirai")) {
    stop(".mirai_task_state() expects a 'mirai' object.")
  }
  if (mirai::unresolved(task)) {
    return("running")
  }
  # Resolved: distinguish success from failure via mirai's own predicate.
  # `is_error_value()` covers both classical errors and timeouts / interrupts.
  if (mirai::is_error_value(task$data)) {
    return("failed")
  }
  "done"
}


#' @noRd
.mirai_task_error <- function(task) {
  state <- .mirai_task_state(task)
  if (state != "failed") {
    return(NA_character_)
  }
  # mirai stores the error as a classed character scalar in `task$data`.
  # `as.character()` strips the class and yields the message.
  msg <- tryCatch(as.character(task$data),
                  error = function(e) NA_character_)
  if (length(msg) != 1L || is.na(msg) || !nzchar(msg)) {
    return("unknown mirai error")
  }
  msg
}


#' Extract per-row metrics from a resolved mirai task payload.
#'
#' Companion to `.mirai_task_state()` / `.mirai_task_error()`. Introduced
#' in Phase 2.A so `cryptR_status()` can surface timing + row counts for
#' resolved tasks without requiring the caller to call `cryptR_collect()`
#' first. Returns four NA scalars when the task is still unresolved, is
#' a mirai error value, is not a `mirai` handle at all, or carries a
#' payload that does not match the `.make_row_result()` shape.
#'
#' The scalars mirror — in type — the corresponding fields of the
#' recap xlsx log, so a status snapshot and the final log agree on
#' column types for resolved rows.
#'
#' @param task A `mirai` handle or any object.
#' @return A named list of length 4: `start_time` (POSIXct),
#'   `end_time` (POSIXct), `duration_sec` (numeric),
#'   `n_rows_processed` (integer). Each field is `NA` when the value is
#'   unavailable.
#' @noRd
.task_metrics <- function(task) {
  empty <- list(
    start_time       = as.POSIXct(NA),
    end_time         = as.POSIXct(NA),
    duration_sec     = NA_real_,
    n_rows_processed = NA_integer_
  )
  if (!inherits(task, "mirai")) return(empty)
  if (isTRUE(mirai::unresolved(task))) return(empty)
  if (isTRUE(mirai::is_error_value(task$data))) return(empty)

  value <- task$data
  if (!is.list(value) || is.null(value$metrics)) return(empty)
  m <- value$metrics

  list(
    start_time       = if (inherits(m$start_time, "POSIXct")) m$start_time
                       else as.POSIXct(NA),
    end_time         = if (inherits(m$end_time, "POSIXct")) m$end_time
                       else as.POSIXct(NA),
    duration_sec     = if (is.numeric(m$duration_sec))
                         as.numeric(m$duration_sec)
                       else NA_real_,
    n_rows_processed = if (is.numeric(m$n_rows_processed) ||
                           is.integer(m$n_rows_processed))
                         as.integer(m$n_rows_processed)
                       else NA_integer_
  )
}


#' Best-effort count of the active mirai daemons on the default profile.
#'
#' Returns an integer scalar: the number of daemon slots mirai reports
#' on its default profile. Shared helper used by `crypt_r()` (to decide
#' whether to spawn daemons itself or reuse existing ones), and by
#' `print.cryptR_job()` / `summary.cryptR_job()` (to display worker
#' count). Introduced in Phase 2.A to de-duplicate the `mirai::status()`
#' call site that was open-coded in three places.
#'
#' Return semantics:
#'   - `0L` when no daemons are active.
#'   - Positive integer when the default profile has daemons registered.
#'   - `NA_integer_` when `mirai::status()` errors outright (unexpected
#'     mirai failure, not merely "no daemons").
#'
#' The return value is informational — it reflects the **configured**
#' daemons, which may slightly over-count if a daemon has died.
#'
#' @return Integer(1), possibly `NA`.
#' @noRd
.n_workers_active <- function() {
  tryCatch({
    st <- mirai::status()
    d  <- st$daemons
    if (is.null(d)) 0L
    else if (is.matrix(d)) as.integer(nrow(d))
    else as.integer(length(d))
  }, error = function(e) NA_integer_)
}
