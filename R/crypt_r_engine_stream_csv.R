#' Process one row of the encryption mask — streaming engine (CSV).
#'
#' Introduced in Phase 1.D.4.c, refactored in 1.D.4.d to share chunk
#' transformation / TC finalisation / inspect reporting with the
#' parquet streaming engine.
#'
#' Streams the input CSV via `arrow::open_csv_dataset()` + Scanner,
#' encrypts each chunk in memory, appends each chunk to the output CSV
#' via `utils::write.table(append = TRUE)`. `arrow::CsvWriter` is
#' intentionally not used: it is not exported in every installed
#' `arrow` version. `utils::write.table` preserves the streaming
#' property on the read/transform side with no extra dependency.
#'
#' Handles **csv-in / csv-out only** (dispatcher precondition). The
#' correspondence table is still written as parquet (symmetric with
#' in_memory + parquet streaming): only the main encrypted dataset is
#' CSV.
#'
#' Semantic guarantees vs. the in-memory engine: identical row set,
#' identical output column order, identical character cleanup rules.
#' **Byte-level equality is not guaranteed** — base R's writer quoting
#' rules differ from `rio::export()` / `data.table::fwrite`. Tests
#' compare via re-read content (`rowset()`), not by file hash.
#'
#' Like the parquet streaming engine: no globalenv pollution, TC in
#' `.cryptRopen_env` + on disk.
#'
#' @inheritParams .process_mask_row_streaming
#' @return Invisible list (see `.make_row_result()`). Disk side
#'   effects unchanged; typed return added in Phase 1.D.6.c.
#' @noRd
.process_mask_row_csv_streaming <- function(mask_row, input_path, output_path, intermediate_path,
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
  first_chunk <- TRUE
  n_rows_accum <- 0L
  start_time <- Sys.time()
  errs <- character(0)
  stream_ok <- FALSE

  # --- Read / transform / write by chunks -------------------------------
  tryCatch(
    {
      arrow_dataset <- arrow::open_csv_dataset(input_path)
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

        n_rows_accum <- n_rows_accum + nrow(transformed$out_chunk)

        # Append chunk to CSV output. First chunk writes the header and
        # creates the file; subsequent chunks append without header.
        utils::write.table(
          as.data.frame(transformed$out_chunk),
          file      = output_file_path,
          sep       = ",",
          dec       = ".",
          qmethod   = "double",
          row.names = FALSE,
          col.names = first_chunk,
          append    = !first_chunk
        )
        first_chunk <- FALSE
      }
      # `<-` (not `<<-`) because the tryCatch body evaluates in this
      # function's frame — see the long comment in the in_memory engine.
      stream_ok <- TRUE
    },
    error = function(e) {
      errs <<- c(errs, paste0("stream: ", conditionMessage(e)))
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
