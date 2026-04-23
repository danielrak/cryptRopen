#' Process one row of the encryption mask — streaming engine (parquet).
#'
#' Introduced in Phase 1.D.4.b, refactored in 1.D.4.d to delegate
#' chunk transformation, TC finalisation and inspect reporting to
#' shared helpers (`.transform_stream_chunk()`, `.finalize_stream_tc()`,
#' `.write_stream_inspect()`).
#'
#' Streams the input parquet file through an arrow Scanner by chunks
#' of `chunk_size` rows, encrypts each chunk in memory, writes the
#' result incrementally to the output parquet file via
#' `arrow::ParquetFileWriter`, and builds the correspondence table by
#' accumulating per-chunk `distinct()` rows.
#'
#' Handles **parquet-in / parquet-out only** (dispatcher precondition).
#'
#' Behavioural guarantees (vs. the in-memory engine on matching input):
#'   - Output rows identical (same values, same types, same order).
#'   - Output column order identical: `<var>_crypt` first in
#'     `vars_to_encrypt` order, then remaining input columns in input
#'     order (`vars_to_remove` applied last).
#'   - Character cleanup identical: `str_trim()` + empty → NA.
#'   - Correspondence table contents identical (modulo row order, also
#'     non-deterministic in the in-memory engine).
#'   - `inspect_*.xlsx` layout identical.
#'
#' Deliberate differences (Option B, validated 2026-04-22):
#'   - **No pollution of `globalenv()`**. TC lives in `.cryptRopen_env`
#'     (retrievable via `get_correspondence_tables()`) and on disk.
#'   - `assign_to_global()` + `eval(parse(text = ...))` are not used.
#'
#' @param mask_row,input_path,output_path,intermediate_path,encryption_key,algorithm,correspondence_table
#'   Forwarded from the dispatcher — see `.process_mask_row_in_memory()`.
#' @param chunk_size Integer(1). Arrow Scanner `batch_size`. Only
#'   affects memory usage and performance; results are invariant.
#' @return Invisible list (see `.make_row_result()`). Disk side
#'   effects + `.cryptRopen_env` population unchanged; return value
#'   added in Phase 1.D.6.c for the mirai collect path.
#' @noRd
.process_mask_row_streaming <- function(mask_row, input_path, output_path, intermediate_path,
                                        encryption_key, algorithm, correspondence_table,
                                        chunk_size = 1e6L) {
  encrypted_file <- mask_row[["encrypted_file"]]
  encrypted_stem <- stringr::str_remove(encrypted_file, "\\..*$")
  output_file_path <- file.path(output_path, encrypted_file)

  vars_to_encrypt <- mask_row[["vars_to_encrypt"]] %>%
    stringr::str_split(",") %>%
    unlist() %>%
    stringr::str_trim()

  vars_to_remove <- mask_row[["vars_to_remove"]] %>%
    stringr::str_split(",") %>%
    unlist() %>%
    stringr::str_trim()

  tc_accum <- NULL
  writer <- NULL
  sink <- NULL
  n_rows_accum <- 0L
  start_time <- Sys.time()
  errs <- character(0)
  stream_ok <- FALSE

  # --- Read / transform / write by chunks -------------------------------
  tryCatch(
    {
      arrow_dataset <- arrow::open_dataset(input_path, format = "parquet")
      scanner <- arrow::Scanner$create(arrow_dataset,
        batch_size = as.integer(chunk_size)
      )
      reader <- scanner$ToRecordBatchReader()

      repeat {
        batch <- reader$read_next_batch()
        if (is.null(batch)) break

        chunk <- dplyr::as_tibble(as.data.frame(batch))
        transformed <- .transform_stream_chunk(
          chunk                = chunk,
          vars_to_encrypt      = vars_to_encrypt,
          vars_to_remove       = vars_to_remove,
          encryption_key       = encryption_key,
          algorithm            = algorithm,
          correspondence_table = correspondence_table
        )

        if (correspondence_table && !is.null(transformed$tc_chunk)) {
          tc_accum <- if (is.null(tc_accum)) {
            transformed$tc_chunk
          } else {
            dplyr::distinct(dplyr::bind_rows(tc_accum, transformed$tc_chunk))
          }
        }

        arrow_tbl <- arrow::as_arrow_table(transformed$out_chunk)
        n_rows_accum <- n_rows_accum + nrow(transformed$out_chunk)

        # First chunk fixes the output schema; subsequent chunks must
        # conform. Character cleanup + crypt_vector() are type-stable,
        # so the schema is stable across chunks by construction.
        # Arrow-R quirks (see 1.D.4.b commit 38d8151d):
        #   - `sink` must be an OutputStream (a character path is not
        #     accepted — hence the FileOutputStream wrapper);
        #   - the default `properties` built by
        #     `ParquetWriterProperties$create()` fails unless
        #     `column_names` is provided explicitly;
        #   - `WriteTable()` also requires a non-default `chunk_size`.
        if (is.null(writer)) {
          sink <- arrow::FileOutputStream$create(output_file_path)
          writer <- arrow::ParquetFileWriter$create(
            schema = arrow_tbl$schema,
            sink = sink,
            properties = arrow::ParquetWriterProperties$create(
              column_names = names(arrow_tbl)
            )
          )
        }
        writer$WriteTable(arrow_tbl, chunk_size = arrow_tbl$num_rows)
      }

      if (!is.null(writer)) writer$Close()
      if (!is.null(sink)) sink$close()
      # `<-` (not `<<-`) because the tryCatch body evaluates in this
      # function's frame — see the long comment in the in_memory engine.
      stream_ok <- TRUE
    },
    error = function(e) {
      errs <<- c(errs, paste0("stream: ", conditionMessage(e)))
      # Ensure writer/sink are closed on partial streams so the file is
      # flushable; otherwise the partial output stays locked on Windows.
      try(if (!is.null(writer)) writer$Close(), silent = TRUE)
      try(if (!is.null(sink)) sink$close(), silent = TRUE)
    }
  )

  # --- Correspondence table (shared finaliser) --------------------------
  tryCatch(
    {
      .finalize_stream_tc(
        tc_accum, intermediate_path, encrypted_stem,
        correspondence_table
      )
    },
    error = function(e) {
      errs <<- c(errs, paste0("tc_finalize: ", conditionMessage(e)))
    }
  )

  # --- Inspect (shared; rio::import for type fidelity) ------------------
  tryCatch(
    {
      .write_stream_inspect(output_file_path, output_path, encrypted_file)
    },
    error = function(e) {
      errs <<- c(errs, paste0("inspect: ", conditionMessage(e)))
    }
  )

  end_time <- Sys.time()
  tc_name <- if (correspondence_table && !is.null(tc_accum)) {
    paste0("tc_", encrypted_stem)
  } else {
    NA_character_
  }

  invisible(.make_row_result(
    success = stream_ok && file.exists(output_file_path),
    error_message = if (length(errs) == 0L) {
      NA_character_
    } else {
      paste(errs, collapse = " | ")
    },
    tc_name = tc_name,
    tc_df = tc_accum,
    start_time = start_time,
    end_time = end_time,
    n_rows_processed = if (n_rows_accum > 0L) n_rows_accum else NA_integer_,
    output_file_path = output_file_path
  ))
}
