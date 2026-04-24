#' Does the given path have a `.parquet` extension?
#'
#' Used by the dispatcher to decide whether the streaming engine is
#' applicable. Comparison is case-insensitive. No I/O — purely a name
#' check; actual openability is handled by arrow at read time.
#'
#' @param path Character(1).
#' @return Logical(1).
#' @noRd
.is_parquet <- function(path) {
  if (length(path) != 1L || is.na(path)) {
    return(FALSE)
  }
  grepl("\\.parquet$", path, ignore.case = TRUE)
}

#' Does the given path have a `.csv` extension?
#'
#' Symmetric helper to `.is_parquet()` — lets the dispatcher enable
#' the CSV streaming engine.
#'
#' @param path Character(1).
#' @return Logical(1).
#' @noRd
.is_csv <- function(path) {
  if (length(path) != 1L || is.na(path)) {
    return(FALSE)
  }
  grepl("\\.csv$", path, ignore.case = TRUE)
}

#' Process one row of the encryption mask (dispatcher).
#'
#' Thin dispatcher that selects an engine based on `engine` + file
#' extensions.
#'
#' Routing rule:
#'   - `engine == "in_memory"` → always routes to
#'     `.process_mask_row_in_memory()` (historical code path).
#'   - `engine %in% c("auto", "streaming")` → tries streaming:
#'       * parquet-in + parquet-out → `.process_mask_row_streaming()`;
#'       * csv-in     + csv-out     → `.process_mask_row_csv_streaming()`;
#'       * mixed or non-streamable endpoints (rds, xlsx, parquet→csv,
#'         csv→parquet, …) → silent fallback to
#'         `.process_mask_row_in_memory()` to preserve non-regression.
#'
#' `"auto"` and `"streaming"` are functionally equivalent. The
#' distinction is kept for clarity: `"auto"` is the default promising
#' smart routing; `"streaming"` is an explicit opt-in. We deliberately
#' do **not** make `"streaming"` strict (i.e. erroring on non-
#' streamable inputs) — that would be an API change not authorised by
#' CLAUDE.md.
#'
#' Keeping the dispatcher separate from the engines guarantees that
#' the historical code path stays untouched while the streaming
#' engines evolve, protecting non-regression on existing baselines.
#'
#' @inheritParams .process_mask_row_in_memory
#' @param engine One of `"auto"`, `"in_memory"`, `"streaming"`. See
#'   routing rules above.
#' @param chunk_size Integer. Forwarded to the streaming engines only;
#'   ignored by in_memory.
#' @return Invisible list from the selected engine — see
#'   `.make_row_result()`. Callers who discard the return value (most
#'   unit tests, the AST-patched baseline `cases.R`) continue to work
#'   unchanged.
#' @noRd
.process_mask_row <- function(mask_row, input_path, output_path, intermediate_path,
                              encryption_key, algorithm, correspondence_table,
                              engine = "in_memory", chunk_size = 1e6L) {
  try_stream <- engine %in% c("auto", "streaming")
  out_file <- mask_row[["encrypted_file"]]

  use_parquet_stream <- try_stream &&
    .is_parquet(input_path) &&
    .is_parquet(out_file)
  use_csv_stream <- try_stream &&
    .is_csv(input_path) &&
    .is_csv(out_file)

  if (use_parquet_stream) {
    .process_mask_row_streaming(
      mask_row             = mask_row,
      input_path           = input_path,
      output_path          = output_path,
      intermediate_path    = intermediate_path,
      encryption_key       = encryption_key,
      algorithm            = algorithm,
      correspondence_table = correspondence_table,
      chunk_size           = chunk_size
    )
  } else if (use_csv_stream) {
    .process_mask_row_csv_streaming(
      mask_row             = mask_row,
      input_path           = input_path,
      output_path          = output_path,
      intermediate_path    = intermediate_path,
      encryption_key       = encryption_key,
      algorithm            = algorithm,
      correspondence_table = correspondence_table,
      chunk_size           = chunk_size
    )
  } else {
    # Fallback: `engine = "in_memory"`, or streaming requested on a
    # non-streamable input (rds, xlsx, etc.) or mixed endpoints
    # (parquet→csv, csv→parquet). Historical engine preserves
    # non-regression behaviour in all these cases.
    .process_mask_row_in_memory(
      mask_row             = mask_row,
      input_path           = input_path,
      output_path          = output_path,
      intermediate_path    = intermediate_path,
      encryption_key       = encryption_key,
      algorithm            = algorithm,
      correspondence_table = correspondence_table
    )
  }
}
