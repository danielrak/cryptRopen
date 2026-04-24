#' Build the typed per-row result list returned by the engines.
#'
#' Disk side effects (`.cryptRopen_env` stored TC + output file)
#' would be enough when every worker runs in the same R session, but
#' when a worker runs inside a `mirai` daemon its private env is
#' unreachable from the client; the recap log also needs per-row
#' metrics that are only cheap to compute inside the worker.
#'
#' The three engines (`.process_mask_row_in_memory()`,
#' `.process_mask_row_streaming()`, `.process_mask_row_csv_streaming()`)
#' assemble a structured list via this helper; the mirai dispatcher
#' ships it back to the parent as `task$data`, which `cryptR_collect()`
#' consumes to (a) re-populate `.cryptRopen_env` with the
#' correspondence tables captured inside the workers and (b) write
#' the `log_crypt_r_<timestamp>.xlsx` recap.
#'
#' Disk outputs and `.cryptRopen_env` side effects are *unchanged* —
#' this helper only packages additional metadata for the parent. Tests
#' that exercise the engines directly still pass because they read
#' from disk or `get_correspondence_tables()`, not from the return
#' value.
#'
#' @param success Logical(1).
#' @param error_message Character(1) or `NA_character_` when `success`
#'   is `TRUE`. If multiple blocks failed, messages are concatenated
#'   with `" | "`.
#' @param tc_name Character(1) — e.g. `"tc_persons_crypt"` — or
#'   `NA_character_` when `correspondence_table = FALSE` or when
#'   nothing was produced.
#' @param tc_df A tibble or `NULL`. Byte-identical to what was written
#'   to `intermediate_path/tc_<stem>.parquet`.
#' @param start_time,end_time POSIXct(1). `start_time` is captured
#'   before the first I/O; `end_time` after the last I/O attempt
#'   (including inspect xlsx).
#' @param n_rows_processed Integer(1) or `NA_integer_`. Rows in the
#'   final encrypted dataset (not counting the header).
#' @param output_file_path Character(1). Full path to the encrypted
#'   dataset on disk. Used to compute size / sha256 if the file
#'   exists.
#' @return A list with fields `success`, `error_message`, `tc_name`,
#'   `tc_df`, `metrics`. `metrics` is itself a list with `start_time`,
#'   `end_time`, `duration_sec`, `n_rows_processed`,
#'   `output_file_size_bytes`, `output_file_sha256`.
#' @noRd
.make_row_result <- function(success,
                             error_message,
                             tc_name,
                             tc_df,
                             start_time,
                             end_time,
                             n_rows_processed,
                             output_file_path) {
  size_bytes <- NA_real_
  sha256 <- NA_character_
  if (is.character(output_file_path) &&
    length(output_file_path) == 1L &&
    !is.na(output_file_path) &&
    file.exists(output_file_path)) {
    size_bytes <- tryCatch(
      as.numeric(file.info(output_file_path)$size),
      error = function(e) NA_real_
    )
    sha256 <- tryCatch(
      digest::digest(file = output_file_path, algo = "sha256"),
      error = function(e) NA_character_
    )
  }

  duration_sec <- tryCatch(
    as.numeric(difftime(end_time, start_time, units = "secs")),
    error = function(e) NA_real_
  )

  list(
    success = isTRUE(success),
    error_message = if (is.null(error_message)) {
      NA_character_
    } else {
      as.character(error_message)
    },
    tc_name = if (is.null(tc_name)) {
      NA_character_
    } else {
      as.character(tc_name)
    },
    tc_df = tc_df,
    metrics = list(
      start_time = start_time,
      end_time = end_time,
      duration_sec = duration_sec,
      n_rows_processed = if (is.null(n_rows_processed)) {
        NA_integer_
      } else {
        as.integer(n_rows_processed)
      },
      output_file_size_bytes = size_bytes,
      output_file_sha256 = sha256
    )
  )
}
