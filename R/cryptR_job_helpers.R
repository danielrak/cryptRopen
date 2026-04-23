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
