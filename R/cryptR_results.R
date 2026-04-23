# Output-oriented companion view of a cryptR_job.
#
# Phase 2.B introduces `cryptR_results()` to complement the process-oriented
# `cryptR_status()` / `summary.cryptR_job()` pair. Whereas status answers
# "where is each task in its lifecycle?", results answers "what file did
# each task produce, and is it still on disk?". The two views are
# deliberately orthogonal — joining them on `encrypted_file` gives the
# full picture without one function mixing concerns.
#
# Source of truth:
#   * `output_file_path` is derived from `job$output_path` + `encrypted_file`
#     (cheap, always available).
#   * `exists` is computed parent-side via `file.exists()` — reflects the
#     current disk state at call time, so detecting a deleted output works.
#   * `size_bytes` and `sha256` are read from the per-task
#     `.make_row_result()` payload when available — those values were
#     computed by the worker at the end of its run. A discrepancy between
#     the payload size/sha and the live file indicates the file has been
#     touched since; surfacing that is the user's job via `file.info()` /
#     `digest::digest()` if they care.
#   * `success` / `error_message` come straight from the payload.


#' Per-output disk-oriented view of an async `crypt_r()` job
#'
#' Returns one row per filtered mask row with (a) the expected output
#' file path, (b) whether it currently exists on disk, and (c) the
#' size/hash the worker captured right after writing it. Introduced in
#' Phase 2.B as a companion to [cryptR_status()] — the two views are
#' orthogonal and join cleanly on `encrypted_file`.
#'
#' Running tasks contribute a row with `exists = FALSE` and NA for
#' `size_bytes` / `sha256`. Tasks that failed before reaching the final
#' export contribute a row with `success = FALSE`, `exists` reflecting
#' the live file state (which may still be `FALSE` because the engine
#' short-circuited), and `error_message` set.
#'
#' The `size_bytes` / `sha256` values are the ones the engine recorded
#' inside the worker. If the output file has been modified or removed
#' between then and the call to `cryptR_results()`, those columns do
#' **not** reflect that — they describe what was produced, not what is
#' currently on disk. Use `file.info()` / `digest::digest()` for live
#' measurements when needed.
#'
#' @param job A `cryptR_job` object, as returned by [crypt_r()].
#' @return A data.frame with columns:
#'   * `encrypted_file` (character): row identifier (dedup'd file name).
#'   * `output_file_path` (character): full path where the output is /
#'     would be written.
#'   * `exists` (logical): result of `file.exists(output_file_path)` at
#'     call time.
#'   * `size_bytes` (numeric): size recorded by the worker post-write,
#'     `NA` when the task has not produced a payload yet.
#'   * `sha256` (character): sha256 of the output at worker-completion
#'     time, `NA` otherwise.
#'   * `success` (logical): whether the engine reported a clean end.
#'   * `error_message` (character): concatenated engine errors, `NA` on
#'     success.
#' @seealso [cryptR_status()], [summary.cryptR_job()].
#' @export
#' @examples
#' \dontrun{
#' job <- crypt_r(...)
#' cryptR_wait(job)
#' cryptR_results(job)
#' }
cryptR_results <- function(job) {
  if (!inherits(job, "cryptR_job")) {
    stop("cryptR_results() expects a 'cryptR_job' object.")
  }

  n <- length(job$tasks)
  encrypted_files <- names(job$tasks)
  if (is.null(encrypted_files) && "encrypted_file" %in% names(job$mask_rows)) {
    encrypted_files <- as.character(job$mask_rows$encrypted_file)
  }
  if (is.null(encrypted_files)) encrypted_files <- rep(NA_character_, n)

  if (n == 0L) {
    return(data.frame(
      encrypted_file   = character(0),
      output_file_path = character(0),
      exists           = logical(0),
      size_bytes       = numeric(0),
      sha256           = character(0),
      success          = logical(0),
      error_message    = character(0),
      stringsAsFactors = FALSE
    ))
  }

  # Full payload extraction reuses the tolerant helper that the log
  # writer and the watcher already rely on — consistent behaviour
  # across the three code paths.
  results <- mapply(
    .extract_row_result,
    task           = job$tasks,
    encrypted_file = encrypted_files,
    SIMPLIFY       = FALSE,
    USE.NAMES      = FALSE
  )

  output_file_path <- file.path(job$output_path, encrypted_files)

  data.frame(
    encrypted_file = encrypted_files,
    output_file_path = output_file_path,
    exists = file.exists(output_file_path),
    size_bytes = vapply(
      results,
      \(r) if (is.null(r$metrics$output_file_size_bytes)) {
        NA_real_
      } else {
        as.numeric(r$metrics$output_file_size_bytes)
      },
      numeric(1)
    ),
    sha256 = vapply(
      results,
      \(r) if (is.null(r$metrics$output_file_sha256)) {
        NA_character_
      } else {
        as.character(r$metrics$output_file_sha256)
      },
      character(1)
    ),
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
    stringsAsFactors = FALSE
  )
}
