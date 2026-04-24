#' Transform a single stream chunk: cleanup + crypt + column assembly.
#'
#' Shared by `.process_mask_row_streaming()` (parquet) and
#' `.process_mask_row_csv_streaming()` (csv). Pure function — no I/O,
#' no globalenv / .cryptRopen_env side effect.
#'
#' Transformation steps (byte-for-byte identical to the historical
#' in_memory engine on matching inputs):
#'   1. Character cleanup: `str_trim()` then empty strings → `NA`.
#'   2. Per-variable encryption via `crypt_vector()`.
#'   3. Column assembly: `<var>_crypt` columns first in `vars_to_encrypt`
#'      order, then non-encrypted input columns in input order.
#'   4. Optional `vars_to_remove` drop (intersect-based — missing
#'      columns are silently ignored, matching in_memory behaviour).
#'
#' The returned `tc_chunk` is already `distinct()`. The caller merges
#' chunks across iterations with an outer `bind_rows %>% distinct`.
#'
#' @param chunk A tibble / data.frame — one scanner batch.
#' @param vars_to_encrypt,vars_to_remove Character vectors (as split
#'   from the mask row and trimmed).
#' @param encryption_key,algorithm Forwarded to `crypt_vector()`.
#' @param correspondence_table Logical(1). Controls whether `tc_chunk`
#'   is computed (`NULL` when `FALSE`).
#' @return A named list: `out_chunk` (tibble ready to write) and
#'   `tc_chunk` (tibble or `NULL`).
#' @noRd
.transform_stream_chunk <- function(chunk, vars_to_encrypt, vars_to_remove,
                                    encryption_key, algorithm,
                                    correspondence_table) {
  # 1. Character cleanup — trim + empty-string-to-NA.
  chunk <- dplyr::mutate(
    chunk,
    dplyr::across(dplyr::where(is.character), \(col) {
      col[nchar(stringr::str_trim(col)) == 0] <- NA
      stringr::str_trim(col)
    })
  )

  # 2. Encrypt each requested variable.
  crypted <- purrr::map(vars_to_encrypt, \(v) {
    crypt_vector(
      vector = chunk[[v]], key = encryption_key,
      algo = algorithm
    )
  })
  names(crypted) <- paste0(vars_to_encrypt, "_crypt")
  crypted_df <- dplyr::as_tibble(crypted)

  # 3. Per-chunk correspondence rows (if requested).
  tc_chunk <- NULL
  if (correspondence_table) {
    tc_chunk <- dplyr::distinct(
      dplyr::bind_cols(
        dplyr::select(chunk, dplyr::all_of(vars_to_encrypt)),
        crypted_df
      )
    )
  }

  # 4. Assemble output: <var>_crypt first, non-encrypted in input order,
  #    then vars_to_remove dropped.
  non_enc_cols <- setdiff(names(chunk), vars_to_encrypt)
  out_chunk <- dplyr::bind_cols(
    crypted_df,
    dplyr::select(chunk, dplyr::all_of(non_enc_cols))
  )

  if (!all(is.na(vars_to_remove))) {
    to_drop <- intersect(vars_to_remove, names(out_chunk))
    if (length(to_drop) > 0L) {
      out_chunk <- dplyr::select(out_chunk, -dplyr::all_of(to_drop))
    }
  }

  list(out_chunk = out_chunk, tc_chunk = tc_chunk)
}

#' Finalise the accumulated correspondence table: store in the package
#' private env + export to parquet on disk.
#'
#' Shared by the two streaming engines whose post-loop finalisation
#' is identical. Idempotent — a no-op when `correspondence_table` is
#' `FALSE` or when no rows were accumulated.
#'
#' @param tc_accum The accumulated correspondence tibble, or `NULL`.
#' @param intermediate_path Directory for the parquet output.
#' @param encrypted_stem The encrypted file stem (extension stripped).
#' @param correspondence_table Logical(1).
#' @return Invisible `NULL`.
#' @noRd
.finalize_stream_tc <- function(tc_accum, intermediate_path, encrypted_stem,
                                correspondence_table) {
  if (!correspondence_table || is.null(tc_accum)) {
    return(invisible(NULL))
  }

  tc_name <- paste0("tc_", encrypted_stem)
  .store_correspondence_table(name = tc_name, df = tc_accum)
  arrow::write_parquet(
    tc_accum,
    file.path(
      intermediate_path,
      paste0(tc_name, ".parquet")
    )
  )
  invisible(NULL)
}

#' Re-read a freshly written streamed output and emit the companion
#' `inspect_*.xlsx` report.
#'
#' Both streaming engines rely on a relecture-based inspect (chunks
#' are not kept in RAM across the scan).
#'
#' Relecture uses `rio::import()` — not arrow — to preserve strict
#' semantic non-regression with the in_memory engine. Rationale: the
#' in_memory engine calls `inspect()` on the pre-export tibble whose
#' types come from `rio::import()` (e.g. `data.table::fread` for CSV
#' returns `class = c("IDate", "Date")` on date columns, which makes
#' `inspect()` emit two rows per column via its `class`-vectorisation).
#' A prior implementation using `arrow::read_parquet` /
#' `arrow::read_csv_arrow` produced different `class()` tuples and
#' broke the baseline `compare_inspect()` comparison on 11 cases.
#' Reloading via `rio::import()` on the freshly written output
#' reproduces the historical type vector on both readers.
#'
#' Layout (identical to the in_memory engine):
#'   row 1 — writexl header artefact (column names),
#'   row 2 — `"Obs = "` + total row count,
#'   row 3 — `"Nvars = "` + inspect() row count,
#'   rows 4+ — inspect() output indexed by row number.
#'
#' @param output_file_path Full path to the encrypted output file.
#' @param output_path Directory where the xlsx will be written.
#' @param encrypted_file Base name of the encrypted file (with
#'   extension) — used to build the `inspect_*.xlsx` filename.
#' @return Invisible `NULL`. No-op if the output file is absent.
#' @noRd
.write_stream_inspect <- function(output_file_path,
                                  output_path, encrypted_file) {
  if (!file.exists(output_file_path)) {
    return(invisible(NULL))
  }

  out_df <- as.data.frame(rio::import(output_file_path, trust = TRUE))
  i <- inspect(out_df)
  # NB. The `1:nrow(i)` spelling (not `seq_len(nrow(i))`) is load-bearing:
  # `cbind()` names the new column from the *expression* passed to it, so
  # the historical in_memory engine emits a data.frame whose first column
  # is named "1:nrow(i)". writexl writes this name as the header row, which
  # readxl::read_xlsx(col_names = FALSE) then surfaces as the value of
  # row 1, column 1 in the compared xlsx. Using any other expression
  # (seq_len(), seq.int()…) breaks baseline on this single cell.
  layout <- rbind(
    c("Obs = ", nrow(out_df), rep("", ncol(i) - 1)),
    c("Nvars = ", nrow(i), rep("", ncol(i) - 1)),
    cbind(1:nrow(i), i)
  )
  writexl::write_xlsx(
    layout,
    file.path(
      output_path,
      paste0("inspect_", encrypted_file, ".xlsx")
    )
  )
  invisible(NULL)
}
