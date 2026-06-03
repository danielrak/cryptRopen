#' Pseudonymize Variables Across Multiple Files via an Excel Mask
#'
#' The high-level entry point for the batch / industrialized workflow.
#' Reads a single Excel mask that describes, for each input file, which
#' columns to hash and which to drop; dispatches one `mirai` task per
#' filtered mask row (parallel, non-blocking); and returns immediately
#' a `cryptR_job` handle. The job is inspected with [cryptR_status()] /
#' [summary()] / [cryptR_results()], blocked on with [cryptR_wait()],
#' and finalized with [cryptR_collect()] (which writes a recap xlsx log
#' and tears down daemons when applicable).
#'
#' A per-row failure does not interrupt the other rows — the failure is
#' captured on the corresponding task and surfaces via [cryptR_status()].
#'
#' **Empty `vars_to_encrypt` cell.** A mask row whose `vars_to_encrypt`
#' cell is blank (empty, `NA`, or whitespace-only) is legitimate and
#' means "process this file, applying `vars_to_remove` if any, hashing
#' nothing." The output file is written under `output_path` re-encoded
#' to the format implied by `encrypted_file`; no `_crypt` columns are
#' emitted, and **no `tc_*.parquet`** is produced. The recap log
#' records `success = TRUE` and `tc_name = NA` for such rows.
#' [crypt_data()] does **not** support this case — see its
#' documentation.
#'
#' @param mask_folder_path Character scalar. Directory containing the
#'   Excel mask file.
#' @param mask_file Character scalar. Name of the Excel mask file
#'   (with extension).
#' @param output_path Character scalar. Existing directory where the
#'   pseudonymized output files will be written. Validated up-front.
#' @param intermediate_path Character scalar. Existing directory where
#'   the correspondence-table parquets (`tc_*.parquet`) and the recap
#'   xlsx log will be written. Validated up-front.
#' @param encryption_key Character scalar. The salt prepended to each
#'   value before hashing. See [crypt_vector()] for the underlying
#'   transformation.
#' @param algorithm Character scalar. Any algorithm accepted by the
#'   `algo` argument of [digest::digest()]. Defaults to `"md5"`.
#' @param correspondence_table Logical scalar. If `TRUE` (default), a
#'   per-input correspondence table `tc_<stem>.parquet` is written under
#'   `intermediate_path` and also stored in the package-private
#'   environment (retrievable via [get_correspondence_tables()] after
#'   [cryptR_collect()] re-injects them post-run).
#' @param engine One of `"auto"`, `"in_memory"`, `"streaming"`.
#'   Selects the per-row processing engine. `"auto"` (default) and
#'   `"streaming"` both route parquet-in/parquet-out and csv-in/csv-out
#'   to the streaming engines (an `'arrow'` Scanner by chunks, with
#'   progressive write); mixed or non-streamable endpoints (rds, xlsx,
#'   parquet→csv, csv→parquet) silently fall back to `"in_memory"`.
#'   `"in_memory"` always reads the full input into RAM.
#' @param chunk_size Integer scalar. Number of rows per chunk when the
#'   effective engine is streaming. Ignored by `"in_memory"`. Defaults
#'   to `1e6`.
#' @param n_workers Integer scalar or `NULL`. Number of `mirai` daemons
#'   to spawn for the dispatch. When `NULL` (default), `crypt_r()`
#'   picks `min(parallel::detectCores() - 1, n_rows, 8L)` (floored
#'   at 1). If daemons are already running on the default profile when
#'   `crypt_r()` is called, `n_workers` is ignored and the existing
#'   daemons are reused — the user retains control and
#'   [cryptR_collect()] will not tear them down.
#' @return A `cryptR_job` object carrying one `mirai` task per filtered
#'   mask row. Inspect with [cryptR_status()] / [summary()] /
#'   [cryptR_results()]; block with [cryptR_wait()]; finalize with
#'   [cryptR_collect()].
#' @family async_job
#' @seealso [cryptR_status()], [cryptR_wait()], [cryptR_collect()],
#'   [cryptR_results()], [get_correspondence_tables()].
#' @export
#'
#' @examples
#' \donttest{
#' # Minimal end-to-end run using the persons.csv fixture shipped
#' # with the package. Everything is written to tempdir() and cleaned
#' # up at the end.
#' work_dir <- file.path(tempdir(), "crypt_r_example")
#' mask_dir <- file.path(work_dir, "mask")
#' out_dir  <- file.path(work_dir, "output")
#' int_dir  <- file.path(work_dir, "intermediate")
#' dir.create(mask_dir, recursive = TRUE, showWarnings = FALSE)
#' dir.create(out_dir, showWarnings = FALSE)
#' dir.create(int_dir, showWarnings = FALSE)
#'
#' input_file <- system.file("extdata", "persons.csv", package = "cryptRopen")
#' mask <- data.frame(
#'   folder_path     = dirname(input_file),
#'   file            = basename(input_file),
#'   encrypted_file  = "persons_crypt.csv",
#'   vars_to_encrypt = "email",
#'   vars_to_remove  = NA,
#'   to_encrypt      = "X",
#'   stringsAsFactors = FALSE
#' )
#' writexl::write_xlsx(mask, file.path(mask_dir, "mask.xlsx"))
#'
#' # n_workers = 1L keeps the example friendly to CRAN check policies
#' # on parallelism in examples.
#' job <- crypt_r(
#'   mask_folder_path  = mask_dir,
#'   mask_file         = "mask.xlsx",
#'   output_path       = out_dir,
#'   intermediate_path = int_dir,
#'   encryption_key    = "demo-key",
#'   n_workers         = 1L
#' )
#' job <- cryptR_collect(job)
#'
#' list.files(out_dir)
#' unlink(work_dir, recursive = TRUE)
#' }
crypt_r <- function(mask_folder_path, mask_file,
                    output_path, intermediate_path,
                    encryption_key, algorithm = "md5",
                    correspondence_table = TRUE,
                    engine = c("auto", "in_memory", "streaming"),
                    chunk_size = 1e6L,
                    n_workers = NULL) {
  engine <- match.arg(engine)
  stopifnot(
    is.numeric(chunk_size),
    length(chunk_size) == 1L,
    !is.na(chunk_size),
    chunk_size > 0
  )
  chunk_size <- as.integer(chunk_size)

  if (!is.null(n_workers)) {
    stopifnot(
      is.numeric(n_workers), length(n_workers) == 1L,
      !is.na(n_workers), n_workers >= 1
    )
    n_workers <- as.integer(n_workers)
  }

  # --- Fast-fail on invalid output directories ------------------------
  # Both paths are *shared* by every worker. If either is missing, all
  # dispatched tasks would fail silently (bubbling up as per-task errors
  # only via cryptR_status()) and the user would only learn about it
  # after the whole job has resolved. Validating up-front turns this
  # into an actionable stop() at call time.
  #
  # Per-input paths (resolved from mask$folder_path + mask$file) are NOT
  # validated here — per CLAUDE.md, a failing row must not interrupt the
  # others, and the per-row tryCatch inside the engines already captures
  # "file not found" errors.
  .assert_writable_dir <- function(path, label) {
    if (!is.character(path) || length(path) != 1L || is.na(path) ||
      !nzchar(path)) {
      stop(
        sprintf(
          "`%s` must be a non-empty character(1) path. Got: %s",
          label,
          if (is.character(path)) {
            paste(utils::capture.output(dput(path)),
              collapse = " "
            )
          } else {
            class(path)[1]
          }
        ),
        call. = FALSE
      )
    }
    if (!dir.exists(path)) {
      stop(
        sprintf(
          "`%s` directory does not exist: %s\nCreate it before calling crypt_r().",
          label, path
        ),
        call. = FALSE
      )
    }
    invisible(NULL)
  }
  .assert_writable_dir(output_path, "output_path")
  .assert_writable_dir(intermediate_path, "intermediate_path")

  # The Excel mask:
  mask <-
    rio::import(file.path(mask_folder_path, mask_file)) %>%
    # Drop fully blank rows (a convenience for hand-edited masks where
    # users sometimes leave empty lines for visual grouping).
    dplyr::filter(dplyr::if_any(dplyr::everything(), ~ !is.na(.))) %>%
    # Insert a row number for downstream technical references.
    dplyr::mutate(row_number = dplyr::row_number()) %>%
    # Keep only rows the user actually wants processed.
    dplyr::filter(to_encrypt == "X")

  # Check eventual duplicated file names, if there is, add indication
  # to keep file names unique:
  mask <-
    dplyr::mutate(
      mask,
      dupl_encrypted_file = duplicated(encrypted_file),
      ndupl_encrypted_file = cumsum(dupl_encrypted_file) %>%
        (\(n) paste0("DUPL", n)),
      encrypted_file =
        ifelse(dupl_encrypted_file,
          paste0(
            ndupl_encrypted_file, "_",
            encrypted_file
          ),
          encrypted_file
        )
    )

  n_rows <- nrow(mask)

  # Empty mask: nothing to do. Return a trivial cryptR_job so the caller
  # can still introspect / collect uniformly. The watcher short-circuits
  # to an immediate finalize on empty-mask jobs (an empty xlsx log is
  # still produced for uniformity).
  if (n_rows == 0L) {
    job <- .new_cryptR_job(
      tasks                = list(),
      mask_rows            = mask,
      output_path          = output_path,
      intermediate_path    = intermediate_path,
      daemons_owned_by_job = FALSE
    )
    return(.start_watcher(job))
  }

  # --- n_workers resolution --------------------------------------------
  # Heuristic: leave one core to the OS, cap at 8 to avoid hammering
  # shared machines, never exceed the number of tasks.
  if (is.null(n_workers)) {
    cores <- tryCatch(parallel::detectCores(logical = TRUE),
      error = function(e) 2L
    )
    if (is.null(cores) || is.na(cores) || cores < 2L) cores <- 2L
    n_workers <- max(1L, min(as.integer(cores) - 1L, n_rows, 8L))
  } else {
    n_workers <- max(1L, min(n_workers, n_rows))
  }

  # --- Daemons ownership ------------------------------------------------
  # If daemons are already running on the default profile, we reuse them
  # and leave teardown to the user. If none are running, we set up our
  # own and flag the job so cryptR_collect() tears them down at the end.
  # `.n_workers_active()` returns NA if mirai::status() errors outright;
  # in that case we fall back to "no daemons active" (0L) and spawn our
  # own, matching the previous open-coded tryCatch behavior.
  n_existing <- .n_workers_active()
  if (is.na(n_existing)) n_existing <- 0L

  daemons_owned_by_job <- FALSE
  if (n_existing == 0L) {
    mirai::daemons(n_workers)
    daemons_owned_by_job <- TRUE
  }

  # Load the package in each daemon so `cryptRopen:::.process_mask_row`
  # resolves. Silently tolerate failures: if cryptRopen is not installed
  # in the daemon's library paths (dev session), task dispatch below will
  # still surface the lookup error as a failed task via cryptR_status().
  try(mirai::everywhere({
    suppressPackageStartupMessages(requireNamespace("cryptRopen",
      quietly = TRUE
    ))
  }), silent = TRUE)

  # --- Dispatch one mirai task per filtered mask row -------------------
  input_paths <- paste0(mask$folder_path, "/", mask$file)

  tasks <- purrr::map(seq_len(n_rows), \(i) {
    mask_row_i <- mask[i, , drop = FALSE]
    input_path_i <- input_paths[[i]]

    # Resolve the internal dispatcher inside the daemon via
    # `getFromNamespace()` rather than `cryptRopen:::.process_mask_row`
    # at the call site. Same runtime behavior, but `:::` in a published
    # function body triggers an R CMD check NOTE
    # ("::: calls to the package's namespace in its code"); the
    # `getFromNamespace()` form is the documented escape hatch. The
    # daemon has already been asked to load cryptRopen via the
    # `mirai::everywhere()` block above, so the namespace lookup
    # succeeds in the worker.
    mirai::mirai(
      {
        .pmr <- utils::getFromNamespace(".process_mask_row", "cryptRopen")
        .pmr(
          mask_row             = mask_row_i,
          input_path           = input_path_i,
          output_path          = output_path,
          intermediate_path    = intermediate_path,
          encryption_key       = encryption_key,
          algorithm            = algorithm,
          correspondence_table = correspondence_table,
          engine               = engine,
          chunk_size           = chunk_size
        )
      },
      mask_row_i = mask_row_i,
      input_path_i = input_path_i,
      output_path = output_path,
      intermediate_path = intermediate_path,
      encryption_key = encryption_key,
      algorithm = algorithm,
      correspondence_table = correspondence_table,
      engine = engine,
      chunk_size = chunk_size
    )
  })

  names(tasks) <- as.character(mask$encrypted_file)

  job <- .new_cryptR_job(
    tasks                = tasks,
    mask_rows            = mask,
    output_path          = output_path,
    intermediate_path    = intermediate_path,
    daemons_owned_by_job = daemons_owned_by_job
  )

  # Register the auto-watcher so the recap xlsx log is written as soon
  # as the last mirai task resolves, without the user having to call
  # cryptR_collect() manually. Idempotent wrt a manual collect — see
  # R/cryptR_watcher.R + `.log_already_written()`.
  .start_watcher(job)
}
